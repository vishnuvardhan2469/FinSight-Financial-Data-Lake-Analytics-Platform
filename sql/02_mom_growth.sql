-- Business question: is customer spending growing or shrinking from one month to the next?
WITH m AS (
  SELECT year, month, CAST(SUM(amount) AS double) AS spend
  FROM finsight_main_database.fact_transactions
  WHERE txn_type = 'DEBIT'
  GROUP BY year, month
)
SELECT year, month, spend,
       LAG(spend) OVER (ORDER BY year, month) AS prev_spend,
       ROUND(100.0 * (spend - LAG(spend) OVER (ORDER BY year, month))
             / LAG(spend) OVER (ORDER BY year, month), 2) AS mom_pct
FROM m
ORDER BY year, month
