-- FinSight: Athena / Glue Data Catalog setup
-- Run in the Athena query editor (one statement at a time), workgroup: finsight-athena-workgroup
-- Replace YOUR-BUCKET with your S3 bucket name. Database used: finsight_main_database

CREATE DATABASE IF NOT EXISTS finsight_main_database;

-- 1. fact_transactions (partitioned by year, month)
CREATE EXTERNAL TABLE finsight_main_database.fact_transactions (
  txn_id string, customer_id string, account_id string, txn_date date,
  amount decimal(12,2), txn_type string, category string, merchant string,
  account_type string, account_status string, segment string, city string
)
PARTITIONED BY (year int, month int)
STORED AS PARQUET
LOCATION 's3://YOUR-BUCKET/curated/fact_transactions/';

MSCK REPAIR TABLE finsight_main_database.fact_transactions;

-- 2. monthly_spend_by_customer (partitioned)
CREATE EXTERNAL TABLE finsight_main_database.monthly_spend_by_customer (
  customer_id string, segment string, txn_count bigint,
  total_debit decimal(22,2), total_credit decimal(22,2), net_flow decimal(23,2)
)
PARTITIONED BY (year int, month int)
STORED AS PARQUET
LOCATION 's3://YOUR-BUCKET/curated/monthly_spend_by_customer/';

MSCK REPAIR TABLE finsight_main_database.monthly_spend_by_customer;

-- 3. category_summary_monthly (partitioned)
CREATE EXTERNAL TABLE finsight_main_database.category_summary_monthly (
  category string, txn_count bigint, total_amount decimal(22,2),
  avg_amount decimal(13,2), max_amount decimal(12,2)
)
PARTITIONED BY (year int, month int)
STORED AS PARQUET
LOCATION 's3://YOUR-BUCKET/curated/category_summary_monthly/';

MSCK REPAIR TABLE finsight_main_database.category_summary_monthly;

-- 4. account_summary (not partitioned)
CREATE EXTERNAL TABLE finsight_main_database.account_summary (
  account_id string, customer_id string, account_type string, account_status string,
  txn_count bigint, total_debit decimal(22,2), total_credit decimal(22,2),
  first_txn_date date, last_txn_date date
)
STORED AS PARQUET
LOCATION 's3://YOUR-BUCKET/curated/account_summary/';

-- 5. portfolio_summary (not partitioned)
CREATE EXTERNAL TABLE finsight_main_database.portfolio_summary (
  customer_id string, segment string, product string, num_trades bigint,
  total_units bigint, total_invested decimal(33,2), avg_price decimal(13,2)
)
STORED AS PARQUET
LOCATION 's3://YOUR-BUCKET/curated/portfolio_summary/';

-- 6. Baseline for the scan comparison: the same fact data in ONE unpartitioned table.
-- (No external_location: the workgroup enforces a central query-result location.)
CREATE TABLE finsight_main_database.fact_transactions_flat
WITH (
  format = 'PARQUET',
  parquet_compression = 'SNAPPY'
) AS
SELECT txn_id, customer_id, account_id, txn_date, amount, txn_type, category, merchant,
       account_type, account_status, segment, city, year, month
FROM finsight_main_database.fact_transactions;

-- Sanity checks (both should return 1263600)
SELECT count(*) FROM finsight_main_database.fact_transactions;
SELECT count(*) FROM finsight_main_database.fact_transactions_flat;

-- After each new incremental batch adds a month, register the new partitions:
-- MSCK REPAIR TABLE finsight_main_database.fact_transactions;
-- MSCK REPAIR TABLE finsight_main_database.monthly_spend_by_customer;
-- MSCK REPAIR TABLE finsight_main_database.category_summary_monthly;
