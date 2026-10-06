"""Job 2: transformed -> curated, incremental by month. Runs locally or on Glue.

Local: python jobs/02_transformed_to_curated.py [--until YYYY-MM-DD]
Glue : --base s3://finsight-de-proj-data
(--until only limits which batches are picked up, for testing.)
"""
import time

from pyspark.sql import functions as F

import lake_io

args = lake_io.parse_args()
BASE = args.base.rstrip("/")
LOCAL = not lake_io.is_s3(BASE)
TRANSFORMED = f"{BASE}/transformed"
CURATED = f"{BASE}/curated"
STATE_FILE = f"{BASE}/state/curated_processed.json"
UNTIL = args.until


def load_state():
    return lake_io.read_json(STATE_FILE, {"processed_ingest_dates": []})["processed_ingest_dates"]


def save_state(dates):
    lake_io.write_json(STATE_FILE, {"processed_ingest_dates": sorted(dates)})


def build_fact(spark, txn):
    acct = (spark.read.parquet(f"{TRANSFORMED}/accounts")
            .select("account_id", "customer_id", "account_type",
                    F.col("status").alias("account_status"),
                    F.col("balance").alias("account_balance")))
    cust = spark.read.parquet(f"{TRANSFORMED}/customers").select("customer_id", "segment", "city")
    return (txn
            .join(F.broadcast(acct), "account_id", "inner")
            .join(F.broadcast(cust), "customer_id", "inner")
            .select("txn_id", "customer_id", "account_id", "txn_date", "amount", "txn_type",
                    "category", "merchant", "account_type", "account_status",
                    "segment", "city", "year", "month"))


def monthly_spend_by_customer(fact):
    is_debit = F.col("txn_type") == "DEBIT"
    return (fact.groupBy("year", "month", "customer_id", "segment")
            .agg(F.count("*").alias("txn_count"),
                 F.sum(F.when(is_debit, F.col("amount")).otherwise(0)).alias("total_debit"),
                 F.sum(F.when(~is_debit, F.col("amount")).otherwise(0)).alias("total_credit"))
            .withColumn("net_flow", F.col("total_credit") - F.col("total_debit")))


def category_summary_monthly(fact):
    return (fact.groupBy("year", "month", "category")
            .agg(F.count("*").alias("txn_count"),
                 F.sum("amount").alias("total_amount"),
                 F.round(F.avg("amount"), 2).alias("avg_amount"),
                 F.max("amount").alias("max_amount")))


def account_summary(fact):
    is_debit = F.col("txn_type") == "DEBIT"
    return (fact.groupBy("account_id", "customer_id", "account_type", "account_status")
            .agg(F.count("*").alias("txn_count"),
                 F.sum(F.when(is_debit, F.col("amount")).otherwise(0)).alias("total_debit"),
                 F.sum(F.when(~is_debit, F.col("amount")).otherwise(0)).alias("total_credit"),
                 F.min("txn_date").alias("first_txn_date"),
                 F.max("txn_date").alias("last_txn_date")))


def portfolio_summary(spark):
    inv = spark.read.parquet(f"{TRANSFORMED}/investments")
    cust = spark.read.parquet(f"{TRANSFORMED}/customers").select("customer_id", "segment")
    return (inv.join(F.broadcast(cust), "customer_id", "inner")
            .groupBy("customer_id", "segment", "product")
            .agg(F.count("*").alias("num_trades"),
                 F.sum("units").alias("total_units"),
                 F.sum(F.col("units") * F.col("price")).alias("total_invested"),
                 F.round(F.avg("price"), 2).alias("avg_price")))


def write_partitioned(df, name):
    (df.repartition("year", "month")
       .write.mode("overwrite").partitionBy("year", "month")
       .parquet(f"{CURATED}/{name}"))


def write_full(df, name):
    df.coalesce(1).write.mode("overwrite").parquet(f"{CURATED}/{name}")


def main():
    t0 = time.time()
    spark = lake_io.get_spark("finsight_transformed_to_curated", LOCAL)

    txn_all = spark.read.parquet(f"{TRANSFORMED}/transactions")
    done = load_state()
    dates = sorted(r[0] for r in txn_all.select("ingest_date").distinct().collect())
    pending = [d for d in dates if d not in done and (UNTIL is None or d <= UNTIL)]
    print(f"available: {dates} | already built: {done} | pending: {pending}")
    if not pending:
        print("Nothing new to build.")
        if LOCAL:
            spark.stop()
        return

    months = [(r["year"], r["month"]) for r in
              txn_all.filter(F.col("ingest_date").isin(pending))
                     .select("year", "month").distinct().collect()]
    print(f"affected months: {sorted(months)}")
    cond = None
    for y, m in months:
        c = (F.col("year") == y) & (F.col("month") == m)
        cond = c if cond is None else (cond | c)

    txn_m = txn_all.filter(cond)
    fact_m = build_fact(spark, txn_m).cache()
    n_fact, n_txn = fact_m.count(), txn_m.count()
    print(f"join check: transformed={n_txn:,} fact={n_fact:,} -> {'PASS' if n_fact == n_txn else 'FAIL'}")

    for name, df in [("fact_transactions", fact_m),
                     ("monthly_spend_by_customer", monthly_spend_by_customer(fact_m)),
                     ("category_summary_monthly", category_summary_monthly(fact_m))]:
        t = time.time()
        write_partitioned(df, name)
        print(f"[{name}] partitions rebuilt for {len(months)} month(s), {time.time() - t:.1f}s")

    fact_all = spark.read.parquet(f"{CURATED}/fact_transactions")
    write_full(account_summary(fact_all), "account_summary")
    write_full(portfolio_summary(spark), "portfolio_summary")

    save_state(done + pending)
    print(f"total rows in curated/fact_transactions: {fact_all.count():,}")
    print(f"elapsed seconds: {time.time() - t0:.1f}")
    if LOCAL:
        spark.stop()


if __name__ == "__main__":
    main()