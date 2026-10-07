"""Job 1 (raw -> transformed, transactions): validation rules, reconciliation, idempotency, incremental merge."""
import json
import pathlib
from decimal import Decimal

import pytest

import lake_io
from conftest import load_job, parquet_mtimes, posix, write_csv

pytestmark = pytest.mark.spark

HEADER = ["txn_id", "account_id", "txn_date", "amount", "txn_type", "category", "merchant"]


@pytest.fixture(scope="module")
def job1():
    return load_job("01_raw_to_transformed.py", "job1_under_test")


@pytest.fixture
def lake(job1, tmp_path, monkeypatch):
    """Point the job at an empty temporary lake that already has an accounts table."""
    base = posix(tmp_path)
    monkeypatch.setattr(job1, "RAW", f"{base}/raw")
    monkeypatch.setattr(job1, "TRANSFORMED", f"{base}/transformed")
    monkeypatch.setattr(job1, "REJECTED", f"{base}/rejected")
    monkeypatch.setattr(job1, "STATE_FILE", f"{base}/state/transactions_processed.json")
    monkeypatch.setattr(job1, "LOCAL", False)                     # so main() never stops the shared Spark session
    write_csv(tmp_path / "raw" / "accounts" / "accounts.csv", ["account_id", "customer_id"],
              [["A1", "C1"], ["A2", "C2"]])
    return tmp_path


def put_batch(lake, ingest_date, rows):
    write_csv(lake / "raw" / "transactions" / f"ingest_date={ingest_date}" / "part1.csv", HEADER, rows)


def account_ids(job1, spark):
    return job1.read_account_ids(spark, f"{job1.RAW}/accounts")


BATCH1 = [
    ["V1", "A1", "2025-01-05", "10.50", "DEBIT", "Groceries", "M1"],
    ["V2", "A1", "2025-01-15", "20.00", "CREDIT", "Salary", "M2"],
    ["V3", "A2", "2025-01-20", "30.25", "DEBIT", " dining ", "M3"],       # messy category: repaired, not rejected
    ["V4", "A2", "2025-01-28", "40.00", "DEBIT", "FUEL", "M1"],
    ["V5", "A1", "2025-02-02", "50.00", "debit", "Shopping", "M2"],
    ["V6", "A1", "2025-02-10", "60.00", "DEBIT", "Travel", "M3"],
    ["V7", "A2", "2025-02-18", "70.00", "CREDIT", "Rent", "M1"],
    ["V8", "A2", "2025-02-25", "80.00", "DEBIT", "Utilities", "M2"],
    ["V1", "A1", "2025-01-05", "10.50", "DEBIT", "Groceries", "M1"],      # exact duplicate of V1
    ["N1", "A1", "2025-01-09", "", "DEBIT", "Groceries", "M1"],           # blank amount
    ["O1", "A9999", "2025-01-11", "5.00", "DEBIT", "Groceries", "M1"],    # account does not exist
    ["B1", "A1", "2025-02-30", "5.00", "DEBIT", "Groceries", "M1"],       # impossible date
    ["G1", "A1", "2025-01-12", "-3.00", "DEBIT", "Groceries", "M1"],      # negative amount
]


# ---------- the individual rules ----------
def test_each_bad_row_gets_the_right_reject_reason(spark, job1):
    rows = [
        ("OK", "A1", "2025-03-01", "10.50", "DEBIT", "Groceries", "M1"),
        ("NULLAMT", "A1", "2025-03-01", None, "DEBIT", "Groceries", "M1"),
        ("TEXTAMT", "A1", "2025-03-01", "abc", "DEBIT", "Groceries", "M1"),
        ("EMPTYAMT", "A1", "2025-03-01", "", "DEBIT", "Groceries", "M1"),
        ("NEG", "A1", "2025-03-01", "-5", "DEBIT", "Groceries", "M1"),
        ("ZERO", "A1", "2025-03-01", "0", "DEBIT", "Groceries", "M1"),
        ("FEB30", "A1", "2025-02-30", "5", "DEBIT", "Groceries", "M1"),
        ("NOTADATE", "A1", "not_a_date", "5", "DEBIT", "Groceries", "M1"),
        ("BADFMT", "A1", "13-45-2025", "5", "DEBIT", "Groceries", "M1"),
        ("BOTH", "A1", "not_a_date", None, "DEBIT", "Groceries", "M1"),     # two problems: the first rule wins
        (None, "A1", "2025-03-01", "5", "DEBIT", "Groceries", "M1"),
    ]
    df = spark.createDataFrame(rows, job1.TXN_SCHEMA)
    got = {(r["txn_id"] or "NULLID"): r["reject_reason"] for r in job1.cast_and_flag(df).collect()}
    assert got == {
        "OK": None,
        "NULLAMT": "null_or_invalid_amount", "TEXTAMT": "null_or_invalid_amount", "EMPTYAMT": "null_or_invalid_amount",
        "NEG": "non_positive_amount", "ZERO": "non_positive_amount",
        "FEB30": "invalid_date", "NOTADATE": "invalid_date", "BADFMT": "invalid_date",
        "BOTH": "null_or_invalid_amount",
        "NULLID": "null_txn_id",
    }


def test_orphans_are_flagged_but_never_override_an_earlier_reason(spark, job1, lake):
    schema = "txn_id string, account_id string, reject_reason string"
    df = spark.createDataFrame([("T1", "A1", None), ("T2", "A9999", None), ("T3", "A9999", "invalid_date")], schema)
    out = {r["txn_id"]: r["reject_reason"] for r in job1.flag_orphans(df, account_ids(job1, spark)).collect()}
    assert out == {"T1": None, "T2": "orphan_account", "T3": "invalid_date"}


def test_dedupe_keeps_exactly_one_row_per_key(spark, job1):
    df = spark.createDataFrame([("T1", "x"), ("T1", "x"), ("T2", "y")], "txn_id string, v string")
    assert sorted(r["txn_id"] for r in job1.dedupe(df, "txn_id").collect()) == ["T1", "T2"]


def test_good_rows_are_standardized_and_typed(spark, job1):
    rows = [("T1", "A1", "2025-11-26", "73.1", " debit ", "  groceries ", " M1 "),
            ("T2", "A1", "2025-12-02", "5", "CREDIT", "FUEL", "M2")]
    flagged = job1.cast_and_flag(spark.createDataFrame(rows, job1.TXN_SCHEMA))
    clean = job1.to_clean(flagged, "2026-10-07")
    by_id = {r["txn_id"]: r for r in clean.collect()}
    assert by_id["T1"]["category"] == "Groceries" and by_id["T2"]["category"] == "Fuel"
    assert by_id["T1"]["txn_type"] == "DEBIT" and by_id["T1"]["merchant"] == "M1"
    assert by_id["T1"]["amount"] == Decimal("73.10")                       # exact money, two decimals
    assert dict(clean.dtypes)["amount"] == "decimal(12,2)"
    assert (by_id["T1"]["year"], by_id["T1"]["month"]) == (2025, 11)
    assert (by_id["T2"]["year"], by_id["T2"]["month"]) == (2025, 12)
    assert by_id["T1"]["ingest_date"] == "2026-10-07"


# ---------- one batch, end to end ----------
def test_a_batch_splits_into_valid_rejected_and_duplicates_and_reconciles(spark, job1, lake, capsys):
    put_batch(lake, "2026-10-07", BATCH1)
    assert job1.process_batch(spark, "2026-10-07", account_ids(job1, spark)) is True

    out = capsys.readouterr().out
    assert "read=13 dups=1 valid=8 rejected=4 -> PASS" in out
    assert "new rows added to transformed: 8" in out

    valid = spark.read.parquet(f"{job1.TRANSFORMED}/transactions")
    assert valid.count() == 8
    assert {r["category"] for r in valid.select("category").distinct().collect()} == \
        {"Groceries", "Salary", "Dining", "Fuel", "Shopping", "Travel", "Rent", "Utilities"}
    assert (lake / "transformed" / "transactions" / "year=2025" / "month=1").is_dir()
    assert (lake / "transformed" / "transactions" / "year=2025" / "month=2").is_dir()

    rejected = spark.read.parquet(f"{job1.REJECTED}/transactions/ingest_date=2026-10-07")
    reasons = {r["txn_id"]: r["reject_reason"] for r in rejected.collect()}
    assert reasons == {"N1": "null_or_invalid_amount", "O1": "orphan_account",
                       "B1": "invalid_date", "G1": "non_positive_amount"}
    assert "source_file" in rejected.columns                                # the rejected rows say where they came from


def test_rerunning_the_same_batch_adds_nothing_and_rewrites_nothing(spark, job1, lake, capsys):
    put_batch(lake, "2026-10-07", BATCH1)
    acc = account_ids(job1, spark)
    job1.process_batch(spark, "2026-10-07", acc)
    before = parquet_mtimes(lake / "transformed")
    capsys.readouterr()

    job1.process_batch(spark, "2026-10-07", acc)
    out = capsys.readouterr().out
    assert "new rows added to transformed: 0 (already present: 8)" in out
    assert parquet_mtimes(lake / "transformed") == before                  # not a single file rewritten
    assert spark.read.parquet(f"{job1.TRANSFORMED}/transactions").count() == 8


def test_a_new_month_is_added_without_touching_existing_months(spark, job1, lake):
    put_batch(lake, "2026-10-07", BATCH1)
    acc = account_ids(job1, spark)
    job1.process_batch(spark, "2026-10-07", acc)
    before = parquet_mtimes(lake / "transformed")

    put_batch(lake, "2026-10-08", [
        ["W1", "A1", "2025-03-03", "11.00", "DEBIT", "Fuel", "M1"],
        ["W2", "A2", "2025-03-09", "12.00", "CREDIT", "Salary", "M2"],
        ["W3", "A2", "2025-03-20", "13.00", "DEBIT", "Rent", "M3"]])
    job1.process_batch(spark, "2026-10-08", acc)

    after = parquet_mtimes(lake / "transformed")
    assert (lake / "transformed" / "transactions" / "year=2025" / "month=3").is_dir()
    for path, mtime in before.items():                                     # January and February are untouched
        assert after[path] == mtime
    assert spark.read.parquet(f"{job1.TRANSFORMED}/transactions").count() == 11


def test_late_data_for_an_existing_month_is_merged_not_overwritten(spark, job1, lake):
    put_batch(lake, "2026-10-07", BATCH1)
    acc = account_ids(job1, spark)
    job1.process_batch(spark, "2026-10-07", acc)
    feb_before = {k: v for k, v in parquet_mtimes(lake / "transformed").items() if "month=2" in k}

    put_batch(lake, "2026-10-09", [
        ["V9", "A1", "2025-01-15", "15.00", "DEBIT", "Fuel", "M1"],       # genuinely new, old month
        ["V2", "A1", "2025-01-15", "20.00", "CREDIT", "Salary", "M2"]])   # already stored: must not be duplicated
    job1.process_batch(spark, "2026-10-09", acc)

    df = spark.read.parquet(f"{job1.TRANSFORMED}/transactions")
    january = df.filter("year = 2025 AND month = 1")
    assert january.count() == 5                                            # V1..V4 kept, V9 added
    assert january.filter("txn_id = 'V2'").count() == 1
    assert df.count() == 9
    assert {k: v for k, v in parquet_mtimes(lake / "transformed").items() if "month=2" in k} == feb_before


# ---------- the state-driven main() ----------
def test_main_processes_only_new_ingest_dates(spark, job1, lake, monkeypatch, capsys):
    monkeypatch.setattr(lake_io, "get_spark", lambda app, local: spark)
    state_file = pathlib.Path(job1.STATE_FILE)

    put_batch(lake, "2026-10-07", BATCH1)
    job1.main()
    out = capsys.readouterr().out
    assert "pending: ['2026-10-07']" in out and "PASS" in out
    assert json.loads(state_file.read_text())["processed_ingest_dates"] == ["2026-10-07"]

    job1.main()                                                            # nothing new arrived
    assert "Nothing new to process." in capsys.readouterr().out

    put_batch(lake, "2026-10-08", [["W1", "A1", "2025-03-03", "11.00", "DEBIT", "Fuel", "M1"]])
    job1.main()
    out = capsys.readouterr().out
    assert "already processed: ['2026-10-07'] | pending: ['2026-10-08']" in out
    assert "total rows in transformed/transactions: 9" in out
    assert json.loads(state_file.read_text())["processed_ingest_dates"] == ["2026-10-07", "2026-10-08"]
