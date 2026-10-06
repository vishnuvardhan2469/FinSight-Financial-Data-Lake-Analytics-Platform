"""Job 1: raw -> transformed (transactions), incremental. Runs locally or on Glue.

Local: python jobs/01_raw_to_transformed.py
Glue : --base s3://finsight-de-proj-data
"""
import time

from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType

import lake_io

args = lake_io.parse_args()
BASE = args.base.rstrip("/")
LOCAL = not lake_io.is_s3(BASE)
RAW = f"{BASE}/raw"
TRANSFORMED = f"{BASE}/transformed"
REJECTED = f"{BASE}/rejected"
STATE_FILE = f"{BASE}/state/transactions_processed.json"

TXN_COLS = ["txn_id", "account_id", "txn_date", "amount", "txn_type", "category", "merchant"]
TXN_SCHEMA = StructType([StructField(c, StringType(), True) for c in TXN_COLS])


def load_state():
    return lake_io.read_json(STATE_FILE, {"processed_ingest_dates": []})["processed_ingest_dates"]


def save_state(dates):
    lake_io.write_json(STATE_FILE, {"processed_ingest_dates": sorted(dates)})


def read_transactions(spark, path):
    return (spark.read.option("header", "true").schema(TXN_SCHEMA).csv(path)
            .withColumn("source_file", F.col("_metadata.file_path")))


def read_account_ids(spark, path):
    return (spark.read.option("header", "true").csv(path)
            .select("account_id").dropDuplicates()
            .withColumn("_acct_ok", F.lit(True)))


def dedupe(df, key):
    return df.dropDuplicates([key])


def cast_and_flag(df):
    df = (df
          .withColumn("amount_num", F.expr("try_cast(amount AS DOUBLE)"))
          .withColumn("txn_date_parsed",
                      F.expr("try_to_timestamp(txn_date, 'yyyy-MM-dd')").cast("date")))
    return df.withColumn(
        "reject_reason",
        F.when(F.col("txn_id").isNull(), "null_txn_id")
         .when(F.col("amount_num").isNull(), "null_or_invalid_amount")
         .when(F.col("amount_num") <= 0, "non_positive_amount")
         .when(F.col("txn_date_parsed").isNull(), "invalid_date"))


def flag_orphans(df, account_ids):
    j = df.join(F.broadcast(account_ids), "account_id", "left")
    return j.withColumn(
        "reject_reason",
        F.coalesce(F.col("reject_reason"),
                   F.when(F.col("_acct_ok").isNull(), F.lit("orphan_account"))))


def to_clean(valid, ingest_date):
    return valid.select(
        "txn_id", "account_id",
        F.col("txn_date_parsed").alias("txn_date"),
        F.col("amount_num").cast("decimal(12,2)").alias("amount"),
        F.upper(F.trim("txn_type")).alias("txn_type"),
        F.initcap(F.trim("category")).alias("category"),
        F.trim("merchant").alias("merchant"),
        F.year("txn_date_parsed").alias("year"),
        F.month("txn_date_parsed").alias("month"),
        F.lit(ingest_date).alias("ingest_date"))


def write_transformed(df):
    (df.repartition("year", "month")
       .write.mode("overwrite").partitionBy("year", "month")
       .parquet(f"{TRANSFORMED}/transactions"))


def merge_into_transformed(spark, clean):
    """Add only rows not already present; rewrite only the affected year/month partitions.
    Assumes a txn_id always belongs to the same month. Returns number of new rows."""
    target = f"{TRANSFORMED}/transactions"
    months = [(r["year"], r["month"]) for r in clean.select("year", "month").distinct().collect()]
    if not months:
        return 0

    if not lake_io.exists(target):
        n_new = clean.count()
        write_transformed(clean)
        return n_new

    cond = None
    for y, m in months:
        c = (F.col("year") == y) & (F.col("month") == m)
        cond = c if cond is None else (cond | c)
    existing = spark.read.parquet(target).filter(cond)
    new_only = clean.join(existing.select("txn_id"), "txn_id", "left_anti")
    n_new = new_only.count()
    if n_new == 0:
        return 0

    combined = existing.unionByName(new_only).localCheckpoint()   # cut link to files being overwritten
    write_transformed(combined)
    return n_new


def process_batch(spark, ingest_date, account_ids):
    raw = read_transactions(spark, f"{RAW}/transactions/ingest_date={ingest_date}")
    n_read = raw.count()

    flagged = flag_orphans(cast_and_flag(dedupe(raw, "txn_id")), account_ids).cache()
    n_dups = n_read - flagged.count()
    valid = flagged.filter(F.col("reject_reason").isNull())
    rejected = flagged.filter(F.col("reject_reason").isNotNull())
    n_rej = rejected.count()

    if n_rej:
        (rejected.select(*TXN_COLS, "reject_reason", "source_file")
            .coalesce(1).write.mode("overwrite")
            .parquet(f"{REJECTED}/transactions/ingest_date={ingest_date}"))

    clean = to_clean(valid, ingest_date)
    n_valid = clean.count()
    n_new = merge_into_transformed(spark, clean)

    ok = n_read == n_valid + n_rej + n_dups
    print(f"[ingest_date={ingest_date}] read={n_read:,} dups={n_dups:,} valid={n_valid:,} "
          f"rejected={n_rej:,} -> {'PASS' if ok else 'FAIL'}")
    print(f"    new rows added to transformed: {n_new:,} (already present: {n_valid - n_new:,})")
    flagged.unpersist()
    return ok


def main():
    t0 = time.time()
    all_dates = lake_io.list_ingest_dates(f"{RAW}/transactions")
    done = load_state()
    pending = [d for d in all_dates if d not in done]
    print(f"available: {all_dates} | already processed: {done} | pending: {pending}")
    if not pending:
        print("Nothing new to process.")
        return

    spark = lake_io.get_spark("finsight_raw_to_transformed", LOCAL)
    account_ids = read_account_ids(spark, f"{RAW}/accounts").cache()

    for d in pending:
        t = time.time()
        if process_batch(spark, d, account_ids):
            save_state(done + [d])                 # only after a successful write
            done.append(d)
        print(f"    batch seconds: {time.time() - t:.1f}")

    total = spark.read.parquet(f"{TRANSFORMED}/transactions").count()
    print(f"total rows in transformed/transactions: {total:,}")
    print(f"elapsed seconds: {time.time() - t0:.1f}")
    if LOCAL:
        spark.stop()


if __name__ == "__main__":
    main()