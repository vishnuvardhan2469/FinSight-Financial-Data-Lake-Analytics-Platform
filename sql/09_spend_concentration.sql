-- Business question: how concentrated is spending? What share comes from the top 10% of customers?
WITH c AS (
  SELECT customer_id, SUM(total_debit) AS spend
  FROM finsight_main_database.monthly_spend_by_customer
  GROUP BY customer_id
),
r AS (
  SELECT spend, NTILE(10) OVER (ORDER BY spend DESC) AS decile FROM c
)
SELECT decile,
       COUNT(*) AS customers,
       SUM(spend) AS spend,
       ROUND(100.0 * CAST(SUM(spend) AS double) / SUM(CAST(SUM(spend) AS double)) OVER (), 1) AS pct_of_total
FROM r
GROUP BY decile
ORDER BY decile
