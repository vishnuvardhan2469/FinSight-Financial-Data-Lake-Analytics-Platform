# FinSight Benchmark Results

All numbers below were measured on the project's own runs (dates: 2026-10-07). Nothing is estimated.

## 1. Athena: data scanned, baseline vs optimized

**Setup.** Both tables hold the same 1,263,600 transactions as Snappy-compressed Parquet.

| | Baseline | Optimized |
|---|---|---|
| Table | `fact_transactions_flat` (one unpartitioned table) | `fact_transactions` (partitioned by `year`, `month`) |
| Filter | on `txn_date` | on partition columns `year`, `month` |
| Columns read | only those the query needs | only those the query needs |

Because both tables use columnar Parquet and every query selects only the columns it needs, the
difference measured here comes from **partition pruning** (and, for Q4, from querying a
pre-aggregated table). Queries are in `athena_benchmark_queries.sql`.

| Query | Months scanned | Baseline scanned | Optimized scanned | Reduction |
|---|---|---|---|---|
| Q1: spend by category | 3 of 19 | 7,518,492 B (7.52 MB) | 843,086 B (0.84 MB) | **88.8 %** |
| Q2: top 10 customers | 9 of 19 | 9,355,838 B (9.36 MB) | 4,010,769 B (4.01 MB) | **57.1 %** |
| Q3: monthly volume by segment | 6 of 19 | 7,201,893 B (7.20 MB) | 1,619,492 B (1.62 MB) | **77.5 %** |
| Q4: Q2 answered from `monthly_spend_by_customer` | 9 of 19 | 9,355,838 B (Q2 baseline) | 1,384,747 B (1.38 MB) | **85.2 %** (65.5 % vs the partitioned Q2) |

* Average reduction over Q1-Q3: **74.5 %**.
* Reduction depends on how many months a query touches: the fewer partitions needed, the larger
  the saving. The 9-month query lands at 57 %, inside the 40-60 % target band; the narrower
  3- and 6-month queries exceed it.
* Scan sizes are deterministic, so one run per query is sufficient for the scanned-bytes figures.

**Correctness check.** For every pair the baseline and optimized queries returned **identical
result sets** (Q1: 11 rows, Q2: 10, Q3: 18; Q4 identical to Q2). Row counts of both tables:
1,263,600.

**Run time (not a reliable signal at this size).** Engine time was 0.7-1.8 s for every query
(e.g. Q1 baseline 1,773 ms vs optimized 726 ms; Q2 1,023 vs 898 ms; Q3 835 vs 770 ms). With
about 25 MB of data the differences are within start-up noise, so no run-time claim is made.

## 2. ETL processing time

Local = PySpark `local[*]` on a 12-thread laptop (15.7 GB RAM). Glue = AWS Glue 5.1, G.1X,
2 workers.

| Job | Run | Local | AWS Glue |
|---|---|---|---|
| 1b: small tables (customers, accounts, investments) | full | 23.0 s | 79 s (rerun: 71 s) |
| 1: raw -> transformed (transactions) | full load, 1,212,000 rows read | 22.3 s batch (34.0 s total) | 92 s |
| 1: raw -> transformed | rerun, nothing new | skipped (Spark not started; time not measured) | 37 s |
| 1: raw -> transformed | incremental, 101,000 rows read | 14.2 s batch (25.5 s total) | 70 s |
| 2: transformed -> curated | full build (19 months) | 33.2 s | 94 s |
| 2: transformed -> curated | build of first 18 months | 34.0 s | n/a |
| 2: transformed -> curated | incremental, July 2026 only | 36.0 s | 88 s |

Job 2 detail (local): the three partition-rebuild stages took **10.5 s** for 18 months and
**3.5 s** for the single incremental month (about 3x faster). Total elapsed time did not shrink
because about 25 s per run is fixed cost that does not depend on the amount of new data: Spark
start-up, scanning `ingest_date` to find pending batches, recomputing the lifetime aggregates
(`account_summary`, `portfolio_summary`) from the whole fact table, and a final count.

What the incremental design does reduce:

* Job 1 reads and validates **8 %** of the rows (101,000 of 1,212,000) and rewrites **1** of 19
  partitions.
* Job 2 rebuilds **1** of 19 partitions of each partitioned curated table.

On Glue, container start-up dominates at this data size, so the gain in wall-clock time is
modest. The gap should widen with more data, because fixed costs stay the same while full-load
cost grows.

## 3. Storage

| Layer | Format | Size on S3 |
|---|---|---|
| raw | CSV + JSON (main batch + batch 2) | 92.8 MB |
| transformed | Parquet (Snappy) | 21.2 MB |
| curated | Parquet (Snappy) | 33.0 MB |
| rejected | Parquet | 0.7 MB |

Raw to transformed is about 4x smaller in Parquet.

## 4. Pipeline job history (AWS Glue, 2026-10-07)

| Job | Result | Duration |
|---|---|---|
| 1b small tables | Succeeded (x2) | 79 s, 71 s |
| 1 raw -> transformed | Succeeded (x3) | 92 s (full), 37 s (rerun), 70 s (incremental) |
| 2 transformed -> curated | Failed once, then succeeded (x2) | 47 s (failed: started before Job 1 finished), 94 s (full), 88 s (incremental) |
