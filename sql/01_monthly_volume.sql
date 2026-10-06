-- Business question: how do transaction counts and money in/out trend month by month?
SELECT year, month,
       COUNT(*) AS txns,
       SUM(CASE WHEN txn_type = 'DEBIT'  THEN amount END) AS debit_total,
       SUM(CASE WHEN txn_type = 'CREDIT' THEN amount END) AS credit_total
FROM finsight_main_database.fact_transactions
GROUP BY year, month
ORDER BY year, month
