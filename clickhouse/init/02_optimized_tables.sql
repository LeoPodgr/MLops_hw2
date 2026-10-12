-- Optimized detail table: dictionary encoding for repeated strings,
-- fixed-point money type, monthly partitions and a query-aware sort key.
CREATE TABLE IF NOT EXISTS transactions_db.transactions_optimized
(
    event_time DateTime64(3) CODEC(Delta, ZSTD(1)),
    state LowCardinality(String),
    category LowCardinality(String),
    amount Decimal(18, 2) CODEC(ZSTD(1)),
    transaction_id String CODEC(ZSTD(1))
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(event_time)
ORDER BY (state, amount, event_time)
SETTINGS index_granularity = 8192;

-- Pre-aggregated states: one compact aggregate state per state (and mergeable
-- across insert batches). This is the fast path for the requested query.
CREATE TABLE IF NOT EXISTS transactions_db.state_category_max_agg
(
    state LowCardinality(String),
    max_amount_state AggregateFunction(max, Decimal(18, 2)),
    category_at_max_state AggregateFunction(argMax, String, Decimal(18, 2))
)
ENGINE = AggregatingMergeTree
ORDER BY state;
