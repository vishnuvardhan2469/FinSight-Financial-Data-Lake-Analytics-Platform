-- Business question: which account types are net inflow vs outflow, and how active are they?
SELECT account_type,
       COUNT(*) AS accounts,
       SUM(txn_count) AS txns,
       SUM(total_credit) AS credits,
       SUM(total_debit)  AS debits,
       SUM(total_credit) - SUM(total_debit) AS net_flow
FROM finsight_main_database.account_summary
GROUP BY account_type
ORDER BY net_flow DESC
