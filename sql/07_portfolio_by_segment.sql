-- Business question: how much is invested in each product, by customer segment?
SELECT segment, product,
       COUNT(DISTINCT customer_id) AS investors,
       SUM(num_trades) AS trades,
       SUM(total_invested) AS invested,
       RANK() OVER (PARTITION BY segment ORDER BY SUM(total_invested) DESC) AS rank_in_segment
FROM finsight_main_database.portfolio_summary
GROUP BY segment, product
ORDER BY segment, rank_in_segment
