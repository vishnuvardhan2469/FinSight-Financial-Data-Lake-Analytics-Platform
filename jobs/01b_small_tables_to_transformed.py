"""Job 1b: raw -> transformed for customers, accounts, investments.

Reads the latest ingest_date snapshot of each (or the flat folder locally).
Local: python jobs/01b_small_tables_to_transformed.py
Glue : --base s3://finsight-de-proj-data
"""
import datetime
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


def str_schema(cols):
    return StructType([StructField(c, StringType(), True) for c in cols])


CUST_COLS = ["customer_id", "name", "email", "city", "segment", "signup_date"]
ACCT_COLS = ["account_id", "customer_id", "account_type", "open_date", "balance", "status"]
INV_COLS = ["investment_id", "customer_id", "product", "units", "price", "trade_date"]


def snapshot(dataset):
    """Path of the latest ingest_date snapshot (or the flat folder) and its date."""
    folder = f"{RAW}/{dataset}"
    dates = lake_io.list_ingest_dates(folder)
    if dates:
        return f"{folder}/ingest_date={dates[-1]}", dates[-1]
    return folder, args.ingest_date or datetime.date.today().isoformat()


def parse_date(col):
    return F.expr(f"try_to_timestamp({col}, 'yyyy-MM-dd')").cast("date")


def process_customers(spark):
    path, ingest_date = snapshot("customers")
    df = spark.read.schema(str_schema(CUST_COLS)).json(path)
    n_in = df.count()
    df = df.dropDuplicates(["customer_id"])
    n_dedup = df.count()
    df = df.withColumn("signup_date_p", parse_date("signup_date"))
    df = df.withColumn(
        "reject_reason",
        F.when(F.col("customer_id").isNull(), "null_customer_id")
         .when(F.col("signup_date_p").isNull(), "invalid_signup_date")).cache()
    valid = df.filter(F.col("reject_reason").isNull()).select(
        "customer_id",
        F.trim("name").alias("name"),
        F.lower(F.trim("email")).alias("email"),
        F.trim("city").alias("city"),
        F.initcap(F.trim("segment")).alias("segment"),
        F.col("signup_date_p").alias("signup_date"),
        F.lit(ingest_date).alias("ingest_date"))
    rejected = df.filter(F.col("reject_reason").isNotNull()).select(*CUST_COLS, "reject_reason")
    return n_in, n_dedup, valid, rejected


def process_accounts(spark):
    path, ingest_date = snapshot("accounts")
    df = spark.read.option("header", "true").schema(str_schema(ACCT_COLS)).csv(path)
    n_in = df.count()
    df = df.dropDuplicates(["account_id"])
    n_dedup = df.count()

    cust_ids = (spark.read.parquet(f"{TRANSFORMED}/customers")
                .select("customer_id").withColumn("_cust_ok", F.lit(True)))
    df = (df.withColumn("open_date_p", parse_date("open_date"))
            .withColumn("balance_n", F.expr("try_cast(balance AS DOUBLE)"))
            .join(F.broadcast(cust_ids), "customer_id", "left"))
    df = df.withColumn(
        "reject_reason",
        F.when(F.col("account_id").isNull(), "null_account_id")
         .when(F.col("customer_id").isNull(), "null_customer_id")
         .when(F.col("open_date_p").isNull(), "invalid_open_date")
         .when(F.col("balance_n").isNull(), "invalid_balance")
         .when(F.col("_cust_ok").isNull(), "orphan_customer")).cache()
    valid = df.filter(F.col("reject_reason").isNull()).select(
        "account_id", "customer_id",
        F.initcap(F.trim("account_type")).alias("account_type"),
        F.col("open_date_p").alias("open_date"),
        F.col("balance_n").cast("decimal(14,2)").alias("balance"),
        F.upper(F.trim("status")).alias("status"),
        F.lit(ingest_date).alias("ingest_date"))
    rejected = df.filter(F.col("reject_reason").isNotNull()).select(*ACCT_COLS, "reject_reason")
    return n_in, n_dedup, valid, rejected


def process_investments(spark):
    path, ingest_date = snapshot("investments")
    df = spark.read.option("header", "true").schema(str_schema(INV_COLS)).csv(path)
    n_in = df.count()
    df = df.dropDuplicates(["investment_id"])
    n_dedup = df.count()

    cust_ids = (spark.read.parquet(f"{TRANSFORMED}/customers")
                .select("customer_id").withColumn("_cust_ok", F.lit(True)))
    df = (df.withColumn("trade_date_p", parse_date("trade_date"))
            .withColumn("units_n", F.expr("try_cast(units AS INT)"))
            .withColumn("price_n", F.expr("try_cast(price AS DOUBLE)"))
            .join(F.broadcast(cust_ids), "customer_id", "left"))
    df = df.withColumn(
        "reject_reason",
        F.when(F.col("investment_id").isNull(), "null_investment_id")
         .when(F.col("customer_id").isNull(), "null_customer_id")
         .when(F.col("units_n").isNull() | (F.col("units_n") <= 0), "invalid_units")
         .when(F.col("price_n").isNull() | (F.col("price_n") <= 0), "invalid_price")
         .when(F.col("trade_date_p").isNull(), "invalid_trade_date")
         .when(F.col("_cust_ok").isNull(), "orphan_customer")).cache()
    valid = df.filter(F.col("reject_reason").isNull()).select(
        "investment_id", "customer_id",
        F.initcap(F.trim("product")).alias("product"),
        F.col("units_n").alias("units"),
        F.col("price_n").cast("decimal(12,2)").alias("price"),
        F.col("trade_date_p").alias("trade_date"),
        F.lit(ingest_date).alias("ingest_date"))
    rejected = df.filter(F.col("reject_reason").isNotNull()).select(*INV_COLS, "reject_reason")
    return n_in, n_dedup, valid, rejected


def write_and_report(spark, name, result):
    n_in, n_dedup, valid, rejected = result
    valid.coalesce(1).write.mode("overwrite").parquet(f"{TRANSFORMED}/{name}")
    n_rej = rejected.count()
    if n_rej:                       # an empty write would leave nothing readable
        rejected.coalesce(1).write.mode("overwrite").parquet(f"{REJECTED}/{name}")
    n_valid = spark.read.parquet(f"{TRANSFORMED}/{name}").count()
    n_dups = n_in - n_dedup
    ok = n_in == n_valid + n_rej + n_dups
    print(f"[{name}] read={n_in:,} dups={n_dups:,} valid={n_valid:,} rejected={n_rej:,} "
          f"-> {'PASS' if ok else 'FAIL'}")


def main():
    t0 = time.time()
    spark = lake_io.get_spark("finsight_small_tables", LOCAL)

    # customers first: accounts and investments validate against them
    write_and_report(spark, "customers", process_customers(spark))
    write_and_report(spark, "accounts", process_accounts(spark))
    write_and_report(spark, "investments", process_investments(spark))

    print(f"elapsed seconds: {time.time() - t0:.1f}")
    if LOCAL:
        spark.stop()


if __name__ == "__main__":
    main()