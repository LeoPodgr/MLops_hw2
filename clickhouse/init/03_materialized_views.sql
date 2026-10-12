-- Single consumer path: Kafka -> raw table -> optimized detail -> aggregate table.
-- Keeping a single Kafka materialized view avoids competing consumers/views.
CREATE MATERIALIZED VIEW IF NOT EXISTS transactions_db.kafka_to_raw_mv
TO transactions_db.transactions_raw
AS
SELECT
    event_time,
    state,
    category,
    amount,
    transaction_id,
    raw_payload
FROM transactions_db.kafka_transactions;

CREATE MATERIALIZED VIEW IF NOT EXISTS transactions_db.raw_to_optimized_mv
TO transactions_db.transactions_optimized
AS
SELECT
    event_time,
    toLowCardinality(state) AS state,
    toLowCardinality(category) AS category,
    toDecimal64(round(amount, 2), 2) AS amount,
    transaction_id
FROM transactions_db.transactions_raw;

CREATE MATERIALIZED VIEW IF NOT EXISTS transactions_db.optimized_to_state_agg_mv
TO transactions_db.state_category_max_agg
AS
SELECT
    state,
    maxState(amount) AS max_amount_state,
    argMaxState(toString(category), amount) AS category_at_max_state
FROM transactions_db.transactions_optimized
GROUP BY state;
