-- FinSight: Athena baseline vs optimized benchmark queries
-- Workgroup: finsight-athena-workgroup   Database: finsight_main_database
-- BASELINE  = fact_transactions_flat (same data, unpartitioned Parquet), filter on txn_date
-- OPTIMIZED = fact_transactions (partitioned by year, month), filter on partition columns
-- Each pair must return identical results. Record "Data scanned" for each run.

-- ===== Q1: spend by category, Q1 2026 (3 of 19 months) =====
-- Q1 baseline
SELECT category, SUM(amount) AS total_spend, COUNT(*) AS txns
FROM finsight_main_database.fact_transactions_flat
WHERE txn_type = 'DEBIT'
  AND txn_date >= DATE '2026-01-01' AND txn_date < DATE '2026-04-01'
GROUP BY category
ORDER BY total_spend DESC;

-- Q1 optimized
SELECT category, SUM(amount) AS total_spend, COUNT(*) AS txns
FROM finsight_main_database.fact_transactions
WHERE txn_type = 'DEBIT'
  AND year = 2026 AND month BETWEEN 1 AND 3
GROUP BY category
ORDER BY total_spend DESC;

-- ===== Q2: top 10 customers by debit spend, Oct 2025 - Jun 2026 (9 of 19 months) =====
-- Q2 baseline
SELECT customer_id, SUM(amount) AS total_spend
FROM finsight_main_database.fact_transactions_flat
WHERE txn_type = 'DEBIT'
  AND txn_date >= DATE '2025-10-01' AND txn_date < DATE '2026-07-01'
GROUP BY customer_id
ORDER BY total_spend DESC
LIMIT 10;

-- Q2 optimized
SELECT customer_id, SUM(amount) AS total_spend
FROM finsight_main_database.fact_transactions
WHERE txn_type = 'DEBIT'
  AND ((year = 2025 AND month >= 10) OR (year = 2026 AND month <= 6))
GROUP BY customer_id
ORDER BY total_spend DESC
LIMIT 10;

-- ===== Q3: monthly debit volume by segment, H2 2025 (6 of 19 months) =====
-- Q3 baseline
SELECT month(txn_date) AS m, segment, SUM(amount) AS total_spend
FROM finsight_main_database.fact_transactions_flat
WHERE txn_type = 'DEBIT'
  AND txn_date >= DATE '2025-07-01' AND txn_date < DATE '2026-01-01'
GROUP BY month(txn_date), segment
ORDER BY m, segment;

-- Q3 optimized
SELECT month, segment, SUM(amount) AS total_spend
FROM finsight_main_database.fact_transactions
WHERE txn_type = 'DEBIT'
  AND year = 2025 AND month BETWEEN 7 AND 12
GROUP BY month, segment
ORDER BY month, segment;

-- ===== Q4: same question as Q2, answered from the pre-aggregated curated table =====
SELECT customer_id, SUM(total_debit) AS total_spend
FROM finsight_main_database.monthly_spend_by_customer
WHERE (year = 2025 AND month >= 10) OR (year = 2026 AND month <= 6)
GROUP BY customer_id
ORDER BY total_spend DESC
LIMIT 10;
