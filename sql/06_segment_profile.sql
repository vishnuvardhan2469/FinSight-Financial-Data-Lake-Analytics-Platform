-- Business question: how much does a typical customer in each segment spend?
SELECT segment,
       COUNT(DISTINCT customer_id) AS customers,
       SUM(total_debit) AS total_spend,
       ROUND(CAST(SUM(total_debit) AS double) / COUNT(DISTINCT customer_id), 2) AS spend_per_customer
FROM finsight_main_database.monthly_spend_by_customer
GROUP BY segment
ORDER BY spend_per_customer DESC
