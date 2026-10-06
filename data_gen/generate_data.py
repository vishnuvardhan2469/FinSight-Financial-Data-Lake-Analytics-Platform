"""FinSight synthetic data generator.

Writes to data/raw/:
  customers/customers.json                 (JSON Lines)
  accounts/accounts.csv
  transactions/transactions_partN.csv      (main batch, with injected faults)
  transactions_batch2/transactions_batch2.csv  (later month, for incremental load)
  investments/investments.csv
  fault_summary.json                       (what was injected, for validation proof)

Usage:
  python data_gen/generate_data.py --small     # quick test (20k txns)
  python data_gen/generate_data.py             # full run (1.2M+ txns)
"""
import argparse
import json
import os
from collections import Counter

import numpy as np
import pandas as pd
from faker import Faker

SEED = 42
OUT_DIR = os.path.join("data", "raw")

MAIN_START, MAIN_END = "2025-01-01", "2026-06-30"    # 18 months
BATCH2_START, BATCH2_END = "2026-07-01", "2026-07-31"  # held-out month

SIZES = {
    "full": dict(customers=20_000, txn=1_200_000, chunk=200_000, batch2=100_000, invest=30_000),
    "small": dict(customers=1_000, txn=20_000, chunk=10_000, batch2=5_000, invest=1_500),
}

CATEGORIES = ["Groceries", "Dining", "Travel", "Rent", "Utilities", "Shopping",
              "Entertainment", "Healthcare", "Fuel", "Salary", "Transfer"]
CATEGORY_P = [0.18, 0.14, 0.07, 0.06, 0.09, 0.15, 0.06, 0.05, 0.08, 0.05, 0.07]

MERCHANTS = [f"Merchant_{i:02d}" for i in range(1, 31)]
_w = 1 / np.arange(1, len(MERCHANTS) + 1)          # a few merchants dominate
MERCHANT_P = _w / _w.sum()

BAD_DATES = ["31/13/2025", "2025-02-30", "not_a_date", "13-45-2025", "00/00/0000"]

# Share of rows that get each fault (disjoint sets; total about 4.8%)
FAULT_RATES = {
    "duplicate_txn": 0.010,
    "null_amount": 0.008,
    "bad_date": 0.008,
    "negative_amount": 0.006,
    "orphan_account": 0.006,
    "messy_category": 0.010,
}


def random_date_strings(rng, n, start, end):
    start, end = pd.Timestamp(start), pd.Timestamp(end)
    offsets = rng.integers(0, (end - start).days + 1, n)
    return np.array((start + pd.to_timedelta(offsets, unit="D")).strftime("%Y-%m-%d"))


def gen_customers(rng, fake, n):
    ids = [f"C{i:06d}" for i in range(1, n + 1)]
    signup = random_date_strings(rng, n, "2015-01-01", "2024-12-31")
    segments = rng.choice(["Retail", "Premium", "Corporate"], n, p=[0.7, 0.2, 0.1])
    return [
        {"customer_id": ids[i], "name": fake.name(), "email": fake.email(),
         "city": fake.city(), "segment": str(segments[i]), "signup_date": str(signup[i])}
        for i in range(n)
    ]


def gen_accounts(rng, customer_ids):
    per_customer = rng.integers(1, 5, len(customer_ids))      # 1-4 accounts each
    cust = np.repeat(customer_ids, per_customer)
    n = len(cust)
    return pd.DataFrame({
        "account_id": [f"A{i:07d}" for i in range(1, n + 1)],
        "customer_id": cust,
        "account_type": rng.choice(["Savings", "Current", "Credit", "Fixed Deposit"], n,
                                   p=[0.5, 0.25, 0.15, 0.1]),
        "open_date": random_date_strings(rng, n, "2015-01-01", "2024-12-31"),
        "balance": np.round(rng.lognormal(9, 1.2, n), 2),
        "status": rng.choice(["ACTIVE", "DORMANT", "CLOSED"], n, p=[0.88, 0.08, 0.04]),
    })


def gen_transactions(rng, n, first_id, account_ids, account_p, start, end):
    return pd.DataFrame({
        "txn_id": [f"T{i:09d}" for i in range(first_id, first_id + n)],
        "account_id": rng.choice(account_ids, n, p=account_p),
        "txn_date": random_date_strings(rng, n, start, end),
        "amount": np.round(np.clip(rng.lognormal(4.5, 1.0, n), 1, None), 2),
        "txn_type": rng.choice(["DEBIT", "CREDIT"], n, p=[0.7, 0.3]),
        "category": rng.choice(CATEGORIES, n, p=CATEGORY_P),
        "merchant": rng.choice(MERCHANTS, n, p=MERCHANT_P),
    })


def inject_faults(df, rng, counts):
    """Corrupt disjoint random subsets of rows; record how many of each."""
    n = len(df)
    order = rng.permutation(n)
    pos = 0

    def take(name):
        nonlocal pos
        k = int(n * FAULT_RATES[name])
        sel = order[pos:pos + k]
        pos += k
        counts[name] += len(sel)
        return sel

    sel = take("null_amount")
    df.loc[sel, "amount"] = np.nan

    sel = take("bad_date")
    df.loc[sel, "txn_date"] = rng.choice(BAD_DATES, len(sel))

    sel = take("negative_amount")
    df.loc[sel, "amount"] = -df.loc[sel, "amount"]

    sel = take("orphan_account")
    df.loc[sel, "account_id"] = [f"A9{x:06d}" for x in rng.integers(0, 999_999, len(sel))]

    sel = take("messy_category")
    messy = []
    for c in df.loc[sel, "category"]:
        v = rng.integers(0, 3)
        messy.append(c.upper() if v == 0 else (f"  {c.lower()} " if v == 1 else c.lower()))
    df.loc[sel, "category"] = messy

    sel = take("duplicate_txn")                      # extra copies of clean rows
    df = pd.concat([df, df.iloc[sel]], ignore_index=True)
    return df.iloc[rng.permutation(len(df))].reset_index(drop=True)


def gen_investments(rng, customer_ids, n, start, end):
    return pd.DataFrame({
        "investment_id": [f"I{i:07d}" for i in range(1, n + 1)],
        "customer_id": rng.choice(customer_ids, n),
        "product": rng.choice(["Mutual Fund", "Stock", "Bond", "ETF"], n, p=[0.4, 0.3, 0.15, 0.15]),
        "units": rng.integers(1, 500, n),
        "price": np.round(rng.lognormal(4, 0.8, n), 2),
        "trade_date": random_date_strings(rng, n, start, end),
    })


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--small", action="store_true", help="tiny dataset for testing")
    cfg = SIZES["small" if parser.parse_args().small else "full"]

    rng = np.random.default_rng(SEED)
    Faker.seed(SEED)
    fake = Faker("en_IN")

    for sub in ["customers", "accounts", "transactions", "transactions_batch2", "investments"]:
        os.makedirs(os.path.join(OUT_DIR, sub), exist_ok=True)

    # customers (JSON Lines: one JSON object per line, Spark reads it natively)
    customers = gen_customers(rng, fake, cfg["customers"])
    with open(os.path.join(OUT_DIR, "customers", "customers.json"), "w", encoding="utf-8") as f:
        for row in customers:
            f.write(json.dumps(row) + "\n")
    customer_ids = np.array([c["customer_id"] for c in customers])

    # accounts
    accounts = gen_accounts(rng, customer_ids)
    accounts.to_csv(os.path.join(OUT_DIR, "accounts", "accounts.csv"), index=False)
    account_ids = accounts["account_id"].to_numpy()
    w = rng.lognormal(0, 1, len(account_ids))        # some accounts are far busier
    account_p = w / w.sum()

    # transactions: main batch in chunks
    faults_main, faults_b2 = Counter(), Counter()
    next_id, written = 1, 0
    for k in range(1, cfg["txn"] // cfg["chunk"] + 1):
        df = gen_transactions(rng, cfg["chunk"], next_id, account_ids, account_p, MAIN_START, MAIN_END)
        next_id += cfg["chunk"]
        df = inject_faults(df, rng, faults_main)
        df.to_csv(os.path.join(OUT_DIR, "transactions", f"transactions_part{k}.csv"), index=False)
        written += len(df)
        print(f"  transactions_part{k}.csv: {len(df):,} rows")

    # transactions: held-out later month (for the incremental-load step)
    df = gen_transactions(rng, cfg["batch2"], next_id, account_ids, account_p, BATCH2_START, BATCH2_END)
    df = inject_faults(df, rng, faults_b2)
    df.to_csv(os.path.join(OUT_DIR, "transactions_batch2", "transactions_batch2.csv"), index=False)
    batch2_written = len(df)

    # investments
    inv = gen_investments(rng, customer_ids, cfg["invest"], "2024-01-01", MAIN_END)
    inv.to_csv(os.path.join(OUT_DIR, "investments", "investments.csv"), index=False)

    summary = {
        "rows": {
            "customers": len(customers),
            "accounts": len(accounts),
            "transactions_main_written": written,
            "transactions_main_unique": cfg["txn"],
            "transactions_batch2_written": batch2_written,
            "investments": len(inv),
        },
        "faults_main": dict(faults_main),
        "faults_batch2": dict(faults_b2),
    }
    with open(os.path.join(OUT_DIR, "fault_summary.json"), "w") as f:
        json.dump(summary, f, indent=2)
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()