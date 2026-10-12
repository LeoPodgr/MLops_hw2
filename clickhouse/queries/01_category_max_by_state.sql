-- Baseline query: category belonging to the largest single transaction in each state.
SELECT
    state,
    argMax(category, amount) AS category,
    max(amount) AS max_amount
FROM transactions_db.transactions_raw
GROUP BY state
ORDER BY state;
