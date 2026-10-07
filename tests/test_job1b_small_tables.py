"""Job 1b (customers, accounts, investments): validation, standardization and reconciliation."""
from decimal import Decimal

import pytest

from conftest import load_job, posix, write_csv, write_json_lines

pytestmark = pytest.mark.spark


@pytest.fixture(scope="module")
def job1b():
    return load_job("01b_small_tables_to_transformed.py", "job1b_under_test")


@pytest.fixture
def lake(job1b, tmp_path, monkeypatch):
    base = posix(tmp_path)
    monkeypatch.setattr(job1b, "RAW", f"{base}/raw")
    monkeypatch.setattr(job1b, "TRANSFORMED", f"{base}/transformed")
    monkeypatch.setattr(job1b, "REJECTED", f"{base}/rejected")

    write_json_lines(tmp_path / "raw" / "customers" / "customers.json", [
        {"customer_id": "C1", "name": " Asha Rao ", "email": " ASHA@Example.COM ", "city": "Pune",
         "segment": " premium", "signup_date": "2020-01-15"},
        {"customer_id": "C2", "name": "Ravi", "email": "ravi@example.com", "city": "Delhi",
         "segment": "Retail", "signup_date": "2021-05-20"},
        {"customer_id": "C2", "name": "Ravi", "email": "ravi@example.com", "city": "Delhi",      # exact duplicate
         "segment": "Retail", "signup_date": "2021-05-20"},
        {"customer_id": "C3", "name": "Bad Date", "email": "b@example.com", "city": "Goa",
         "segment": "Retail", "signup_date": "2024-13-45"},
    ])
    write_csv(tmp_path / "raw" / "accounts" / "accounts.csv",
              ["account_id", "customer_id", "account_type", "open_date", "balance", "status"], [
                  ["A1", "C1", "savings", "2021-03-01", "1000.50", "active"],
                  ["A2", "C2", "Current", "2022-04-02", "2000", "ACTIVE"],
                  ["A3", "C99", "Savings", "2022-04-02", "10", "ACTIVE"],        # customer does not exist
                  ["A4", "C1", "Savings", "2020-02-30", "10", "ACTIVE"],         # impossible date
                  ["A5", "C1", "Savings", "2020-02-01", "abc", "ACTIVE"]])       # balance is not a number
    write_csv(tmp_path / "raw" / "investments" / "investments.csv",
              ["investment_id", "customer_id", "product", "units", "price", "trade_date"], [
                  ["I1", "C1", "mutual fund", "10", "5.5", "2025-03-01"],
                  ["I2", "C1", "Stock", "0", "5", "2025-03-01"],                 # units must be positive
                  ["I3", "C1", "Stock", "5", "-1", "2025-03-01"],                # price must be positive
                  ["I4", "C99", "Stock", "5", "5", "2025-03-01"],                # customer does not exist
                  ["I5", "C1", "Stock", "5", "5", "2025-13-01"]])                # impossible date
    return tmp_path


def run_all(spark, job1b):
    job1b.write_and_report(spark, "customers", job1b.process_customers(spark))
    job1b.write_and_report(spark, "accounts", job1b.process_accounts(spark))
    job1b.write_and_report(spark, "investments", job1b.process_investments(spark))


def reasons(spark, job1b, name, id_col):
    return {r[id_col]: r["reject_reason"] for r in spark.read.parquet(f"{job1b.REJECTED}/{name}").collect()}


def test_every_table_reconciles(spark, job1b, lake, capsys):
    run_all(spark, job1b)
    out = capsys.readouterr().out
    assert "[customers] read=4 dups=1 valid=2 rejected=1 -> PASS" in out
    assert "[accounts] read=5 dups=0 valid=2 rejected=3 -> PASS" in out
    assert "[investments] read=5 dups=0 valid=1 rejected=4 -> PASS" in out


def test_each_rejected_record_carries_the_right_reason(spark, job1b, lake):
    run_all(spark, job1b)
    assert reasons(spark, job1b, "customers", "customer_id") == {"C3": "invalid_signup_date"}
    assert reasons(spark, job1b, "accounts", "account_id") == {
        "A3": "orphan_customer", "A4": "invalid_open_date", "A5": "invalid_balance"}
    assert reasons(spark, job1b, "investments", "investment_id") == {
        "I2": "invalid_units", "I3": "invalid_price", "I4": "orphan_customer", "I5": "invalid_trade_date"}


def test_valid_records_are_cleaned_and_typed(spark, job1b, lake):
    run_all(spark, job1b)
    cust = {r["customer_id"]: r for r in spark.read.parquet(f"{job1b.TRANSFORMED}/customers").collect()}
    assert set(cust) == {"C1", "C2"}
    assert cust["C1"]["name"] == "Asha Rao"
    assert cust["C1"]["email"] == "asha@example.com"                     # trimmed and lower-cased
    assert cust["C1"]["segment"] == "Premium"                            # trimmed and capitalised

    accts = spark.read.parquet(f"{job1b.TRANSFORMED}/accounts")
    a1 = {r["account_id"]: r for r in accts.collect()}["A1"]
    assert (a1["account_type"], a1["status"], a1["balance"]) == ("Savings", "ACTIVE", Decimal("1000.50"))
    assert dict(accts.dtypes)["balance"] == "decimal(14,2)"

    inv = spark.read.parquet(f"{job1b.TRANSFORMED}/investments").collect()
    assert len(inv) == 1
    assert (inv[0]["product"], inv[0]["units"], inv[0]["price"]) == ("Mutual Fund", 10, Decimal("5.50"))
