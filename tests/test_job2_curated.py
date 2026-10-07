"""Job 2 (transformed -> curated): joins, aggregates with hand-calculated values, incremental runs by month."""
import datetime as dt
import json
import pathlib
from decimal import Decimal

import pytest

import lake_io
from conftest import load_job, parquet_mtimes, posix

pytestmark = pytest.mark.spark

TXN_SCHEMA = ("txn_id string, account_id string, txn_date date, amount decimal(12,2), txn_type string, "
              "category string, merchant string, ingest_date string, year int, month int")


def d(s):
    return dt.date.fromisoformat(s)


def txn_row(txn_id, account, date, amount, kind, category, ingest="2026-10-07"):
    return (txn_id, account, d(date), Decimal(amount), kind, category, "M1", ingest, d(date).year, d(date).month)


BATCH1 = [
    txn_row("T1", "A1", "2025-01-10", "100.00", "DEBIT", "Groceries"),
    txn_row("T2", "A1", "2025-01-20", "50.00", "DEBIT", "Dining"),
    txn_row("T3", "A1", "2025-01-25", "200.00", "CREDIT", "Salary"),
    txn_row("T4", "A2", "2025-02-05", "30.00", "DEBIT", "Groceries"),
    txn_row("T5", "A2", "2025-02-06", "70.00", "DEBIT", "Groceries"),
]
BATCH2 = [txn_row("T6", "A1", "2025-03-01", "40.00", "DEBIT", "Dining", ingest="2026-10-08")]


@pytest.fixture(scope="module")
def job2():
    return load_job("02_transformed_to_curated.py", "job2_under_test")


@pytest.fixture
def lake(spark, job2, tmp_path, monkeypatch):
    """A tiny transformed layer: 2 customers, 2 accounts, 5 transactions and 3 investments."""
    base = posix(tmp_path)
    monkeypatch.setattr(job2, "TRANSFORMED", f"{base}/transformed")
    monkeypatch.setattr(job2, "CURATED", f"{base}/curated")
    monkeypatch.setattr(job2, "STATE_FILE", f"{base}/state/curated_processed.json")
    monkeypatch.setattr(job2, "UNTIL", None)
    monkeypatch.setattr(job2, "LOCAL", False)
    monkeypatch.setattr(lake_io, "get_spark", lambda app, local: spark)

    spark.createDataFrame([("C1", "Retail", "Mumbai"), ("C2", "Premium", "Delhi")],
                          "customer_id string, segment string, city string") \
        .write.parquet(f"{base}/transformed/customers")
    spark.createDataFrame([("A1", "C1", "Savings", "ACTIVE", Decimal("1000.00")),
                           ("A2", "C2", "Current", "ACTIVE", Decimal("500.00"))],
                          "account_id string, customer_id string, account_type string, status string, balance decimal(14,2)") \
        .write.parquet(f"{base}/transformed/accounts")
    spark.createDataFrame([("I1", "C1", "Stock", 10, Decimal("5.50"), d("2025-01-02"), "2026-10-07"),
                           ("I2", "C1", "Stock", 5, Decimal("4.00"), d("2025-01-03"), "2026-10-07"),
                           ("I3", "C2", "Bond", 2, Decimal("100.00"), d("2025-01-04"), "2026-10-07")],
                          "investment_id string, customer_id string, product string, units int, price decimal(12,2), "
                          "trade_date date, ingest_date string") \
        .write.parquet(f"{base}/transformed/investments")
    add_transactions(spark, base, BATCH1)
    return tmp_path


def add_transactions(spark, base, rows):
    (spark.createDataFrame(rows, TXN_SCHEMA).write.mode("append")
     .partitionBy("year", "month").parquet(f"{base}/transformed/transactions"))


def txn_df(spark, job2):
    return spark.read.parquet(f"{job2.TRANSFORMED}/transactions")


# ---------- the joins and aggregates, checked against numbers worked out by hand ----------
def test_fact_table_neither_drops_nor_multiplies_rows(spark, job2, lake):
    txn = txn_df(spark, job2)
    fact = job2.build_fact(spark, txn)
    assert fact.count() == txn.count() == 5
    row = {r["txn_id"]: r for r in fact.collect()}["T4"]
    assert (row["customer_id"], row["segment"], row["city"], row["account_type"]) == ("C2", "Premium", "Delhi", "Current")


def test_monthly_spend_by_customer_matches_hand_calculation(spark, job2, lake):
    fact = job2.build_fact(spark, txn_df(spark, job2))
    got = {(r["year"], r["month"], r["customer_id"]): r for r in job2.monthly_spend_by_customer(fact).collect()}
    jan, feb = got[(2025, 1, "C1")], got[(2025, 2, "C2")]
    assert (jan["txn_count"], jan["total_debit"], jan["total_credit"], jan["net_flow"]) == \
        (3, Decimal("150.00"), Decimal("200.00"), Decimal("50.00"))
    assert (feb["txn_count"], feb["total_debit"], feb["total_credit"], feb["net_flow"]) == \
        (2, Decimal("100.00"), Decimal("0.00"), Decimal("-100.00"))
    assert len(got) == 2


def test_category_summary_matches_hand_calculation(spark, job2, lake):
    fact = job2.build_fact(spark, txn_df(spark, job2))
    got = {(r["year"], r["month"], r["category"]): r for r in job2.category_summary_monthly(fact).collect()}
    feb = got[(2025, 2, "Groceries")]
    assert (feb["txn_count"], feb["total_amount"], feb["avg_amount"], feb["max_amount"]) == \
        (2, Decimal("100.00"), Decimal("50.00"), Decimal("70.00"))
    assert got[(2025, 1, "Groceries")]["total_amount"] == Decimal("100.00")


def test_account_and_portfolio_summaries_match_hand_calculation(spark, job2, lake):
    fact = job2.build_fact(spark, txn_df(spark, job2))
    a1 = {r["account_id"]: r for r in job2.account_summary(fact).collect()}["A1"]
    assert (a1["txn_count"], a1["total_debit"], a1["total_credit"]) == (3, Decimal("150.00"), Decimal("200.00"))
    assert (a1["first_txn_date"], a1["last_txn_date"]) == (d("2025-01-10"), d("2025-01-25"))

    pf = {(r["customer_id"], r["product"]): r for r in job2.portfolio_summary(spark).collect()}
    stock = pf[("C1", "Stock")]
    assert (stock["num_trades"], stock["total_units"], stock["total_invested"], stock["avg_price"]) == \
        (2, 15, Decimal("75.00"), Decimal("4.75"))                    # 10 x 5.50 + 5 x 4.00
    assert pf[("C2", "Bond")]["total_invested"] == Decimal("200.00")


# ---------- the incremental behaviour of main() ----------
def test_main_builds_then_skips_then_rebuilds_only_the_new_month(spark, job2, lake, capsys):
    base = posix(lake)
    state_file = pathlib.Path(job2.STATE_FILE)

    job2.main()
    out = capsys.readouterr().out
    assert "pending: ['2026-10-07']" in out
    assert "affected months: [(2025, 1), (2025, 2)]" in out
    assert "join check: transformed=5 fact=5 -> PASS" in out
    assert spark.read.parquet(f"{base}/curated/fact_transactions").count() == 5
    assert json.loads(state_file.read_text())["processed_ingest_dates"] == ["2026-10-07"]

    job2.main()                                                        # nothing new
    assert "Nothing new to build." in capsys.readouterr().out
    before = parquet_mtimes(lake / "curated")

    add_transactions(spark, base, BATCH2)                              # a new batch lands in March
    job2.main()
    out = capsys.readouterr().out
    assert "pending: ['2026-10-08']" in out and "affected months: [(2025, 3)]" in out
    assert "join check: transformed=1 fact=1 -> PASS" in out

    after = parquet_mtimes(lake / "curated")
    partitioned = ("fact_transactions", "monthly_spend_by_customer", "category_summary_monthly")
    for path, mtime in before.items():                                 # January and February partitions untouched
        if path.startswith(partitioned) and ("month=1" in path or "month=2" in path):
            assert after[path] == mtime, path
    assert any("fact_transactions" in p and "month=3" in p for p in after)

    assert spark.read.parquet(f"{base}/curated/fact_transactions").count() == 6
    a1 = {r["account_id"]: r for r in spark.read.parquet(f"{base}/curated/account_summary").collect()}["A1"]
    assert a1["txn_count"] == 4                                        # lifetime table picked up the new batch
    assert json.loads(state_file.read_text())["processed_ingest_dates"] == ["2026-10-07", "2026-10-08"]


def test_until_limits_which_batches_are_picked_up(spark, job2, lake, capsys, monkeypatch):
    add_transactions(spark, posix(lake), BATCH2)
    monkeypatch.setattr(job2, "UNTIL", "2026-10-07")
    job2.main()
    out = capsys.readouterr().out
    assert "pending: ['2026-10-07']" in out and "2026-10-08" not in out.split("pending:")[1].split("\n")[0]
    assert spark.read.parquet(f"{posix(lake)}/curated/fact_transactions").count() == 5
