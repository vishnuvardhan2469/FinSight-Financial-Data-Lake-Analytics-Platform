-- Business question: who are our highest-spending customers?
SELECT customer_id, segment, SUM(total_debit) AS total_spend, SUM(txn_count) AS txns
FROM finsight_main_database.monthly_spend_by_customer
GROUP BY customer_id, segment
ORDER BY total_spend DESC
LIMIT 10
