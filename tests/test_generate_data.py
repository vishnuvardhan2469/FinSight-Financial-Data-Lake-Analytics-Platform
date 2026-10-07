"""The data generator: reproducible, and every injected fault is counted exactly."""
import json
import re
import sys
from collections import Counter

import numpy as np
import pandas as pd
import pytest

import generate_data as gd

ISO_DATE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
N = 20_000


@pytest.fixture(scope="module")
def accounts():
    rng = np.random.default_rng(1)
    customer_ids = np.array([f"C{i:06d}" for i in range(1, 201)])
    return gd.gen_accounts(rng, customer_ids), customer_ids


@pytest.fixture(scope="module")
def clean_transactions(accounts):
    acc, _ = accounts
    rng = np.random.default_rng(2)
    ids = acc["account_id"].to_numpy()
    w = rng.lognormal(0, 1, len(ids))
    return gd.gen_transactions(rng, N, 1, ids, w / w.sum(), "2025-01-01", "2026-06-30"), ids


@pytest.fixture(scope="module")
def faulty(clean_transactions):
    df, ids = clean_transactions
    counts = Counter()
    out = gd.inject_faults(df.copy(), np.random.default_rng(3), counts)
    return out, counts, ids


def test_dates_stay_inside_the_range_and_use_iso_format():
    dates = gd.random_date_strings(np.random.default_rng(1), 2000, "2025-01-01", "2025-03-31")
    assert all(ISO_DATE.match(d) for d in dates)
    assert min(dates) >= "2025-01-01" and max(dates) <= "2025-03-31"


def test_same_seed_gives_identical_dates():
    a = gd.random_date_strings(np.random.default_rng(7), 500, "2025-01-01", "2026-06-30")
    b = gd.random_date_strings(np.random.default_rng(7), 500, "2025-01-01", "2026-06-30")
    assert list(a) == list(b)


def test_customers_have_unique_ids_and_known_segments():
    from faker import Faker
    Faker.seed(1)
    customers = gd.gen_customers(np.random.default_rng(1), Faker("en_IN"), 50)
    assert len({c["customer_id"] for c in customers}) == 50
    assert {c["segment"] for c in customers} <= {"Retail", "Premium", "Corporate"}
    assert all(ISO_DATE.match(c["signup_date"]) for c in customers)


def test_every_customer_has_one_to_four_accounts(accounts):
    acc, customer_ids = accounts
    per_customer = acc.groupby("customer_id").size()
    assert per_customer.between(1, 4).all()
    assert set(acc["customer_id"]) == set(customer_ids)       # every account points at a real customer
    assert acc["account_id"].is_unique


def test_clean_transactions_are_well_formed(clean_transactions):
    df, ids = clean_transactions
    assert len(df) == N
    assert df["txn_id"].is_unique
    assert df["txn_id"].iloc[0] == "T000000001"
    assert (df["amount"] >= 1).all()
    assert np.allclose(df["amount"], df["amount"].round(2))
    assert set(df["txn_type"]) <= {"DEBIT", "CREDIT"}
    assert set(df["category"]) <= set(gd.CATEGORIES)
    assert set(df["account_id"]) <= set(ids)


def test_fault_counts_equal_the_answer_key_exactly(faulty):
    out, counts, _ = faulty
    for name, rate in gd.FAULT_RATES.items():
        assert counts[name] == int(N * rate), name
    assert len(out) == N + counts["duplicate_txn"]


def test_the_data_really_contains_the_recorded_faults(faulty):
    out, counts, ids = faulty
    assert out["amount"].isna().sum() == counts["null_amount"]
    assert out["txn_date"].isin(gd.BAD_DATES).sum() == counts["bad_date"]
    assert (out["amount"] < 0).sum() == counts["negative_amount"]
    assert (~out["account_id"].isin(ids)).sum() == counts["orphan_account"]
    assert (~out["category"].isin(gd.CATEGORIES)).sum() == counts["messy_category"]
    assert out["txn_id"].duplicated().sum() == counts["duplicate_txn"]


def test_no_row_receives_two_faults(faulty):
    out, _, ids = faulty
    flags = pd.DataFrame({
        "null_amount": out["amount"].isna(),
        "bad_date": out["txn_date"].isin(gd.BAD_DATES),
        "negative": out["amount"] < 0,
        "orphan": ~out["account_id"].isin(ids),
        "messy": ~out["category"].isin(gd.CATEGORIES),
    })
    assert flags.sum(axis=1).max() == 1


def test_duplicates_are_exact_copies(faulty):
    out, _, _ = faulty
    dup_rows = out[out["txn_id"].duplicated(keep=False)]
    distinct_versions = dup_rows.fillna("<NA>").groupby("txn_id").nunique().max().max()
    assert distinct_versions == 1


def test_orphan_account_ids_can_never_be_real(faulty):
    out, _, ids = faulty
    orphans = set(out["account_id"]) - set(ids)
    assert orphans and all(o.startswith("A9") for o in orphans)


def test_generation_is_reproducible(accounts):
    acc, _ = accounts
    ids = acc["account_id"].to_numpy()
    w = np.ones(len(ids)) / len(ids)

    def build():
        return gd.gen_transactions(np.random.default_rng(7), 1000, 1, ids, w, "2025-01-01", "2026-06-30")

    pd.testing.assert_frame_equal(build(), build())


def test_small_run_end_to_end_matches_its_own_summary(tmp_path, monkeypatch):
    monkeypatch.setattr(gd, "OUT_DIR", str(tmp_path))
    monkeypatch.setattr(sys, "argv", ["generate_data.py", "--small"])
    gd.main()

    summary = json.loads((tmp_path / "fault_summary.json").read_text())
    cfg = gd.SIZES["small"]
    chunks = cfg["txn"] // cfg["chunk"]

    main_files = sorted((tmp_path / "transactions").glob("transactions_part*.csv"))
    assert len(main_files) == chunks
    main_rows = sum(len(pd.read_csv(f, dtype=str)) for f in main_files)
    assert main_rows == summary["rows"]["transactions_main_written"]
    assert summary["rows"]["transactions_main_unique"] == cfg["txn"]
    assert summary["faults_main"]["duplicate_txn"] == chunks * int(cfg["chunk"] * gd.FAULT_RATES["duplicate_txn"])
    assert main_rows == cfg["txn"] + summary["faults_main"]["duplicate_txn"]

    batch2 = pd.read_csv(tmp_path / "transactions_batch2" / "transactions_batch2.csv", dtype=str)
    assert len(batch2) == summary["rows"]["transactions_batch2_written"]
    good_dates = batch2.loc[~batch2["txn_date"].isin(gd.BAD_DATES), "txn_date"]       # the rest must all be July 2026
    assert len(good_dates) > 0 and good_dates.str.startswith("2026-07").all()

    assert len(pd.read_csv(tmp_path / "accounts" / "accounts.csv")) == summary["rows"]["accounts"]
    assert len(pd.read_csv(tmp_path / "investments" / "investments.csv")) == cfg["invest"]
    lines = (tmp_path / "customers" / "customers.json").read_text().strip().splitlines()
    assert len(lines) == cfg["customers"]
