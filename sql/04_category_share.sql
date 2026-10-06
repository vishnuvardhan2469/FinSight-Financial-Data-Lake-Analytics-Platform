-- Business question: which spending categories dominate, and what share of the total is each?
SELECT category,
       SUM(total_amount) AS total,
       ROUND(100.0 * CAST(SUM(total_amount) AS double)
             / SUM(CAST(SUM(total_amount) AS double)) OVER (), 2) AS pct_of_total
FROM finsight_main_database.category_summary_monthly
GROUP BY category
ORDER BY total DESC
