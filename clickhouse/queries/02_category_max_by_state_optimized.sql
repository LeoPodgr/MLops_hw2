-- Optimized query: combines the aggregate states built on every insert batch.
SELECT
    state,
    argMaxMerge(category_at_max_state) AS category,
    maxMerge(max_amount_state) AS max_amount
FROM transactions_db.state_category_max_agg
GROUP BY state
ORDER BY state;
