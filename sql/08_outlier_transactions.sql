-- Business question: which 2026 debits are statistical outliers for their category?
WITH s AS (
  SELECT category,
         AVG(CAST(amount AS double)) AS avg_amt,
         STDDEV(CAST(amount AS double)) AS sd_amt
  FROM finsight_main_database.fact_transactions
  WHERE txn_type = 'DEBIT' AND year = 2026
  GROUP BY category
)
SELECT f.txn_id, f.customer_id, f.category, f.amount,
       ROUND((CAST(f.amount AS double) - s.avg_amt) / s.sd_amt, 1) AS z_score
FROM finsight_main_database.fact_transactions f
JOIN s ON f.category = s.category
WHERE f.txn_type = 'DEBIT' AND f.year = 2026
  AND CAST(f.amount AS double) > s.avg_amt + 3 * s.sd_amt
ORDER BY z_score DESC
LIMIT 20
