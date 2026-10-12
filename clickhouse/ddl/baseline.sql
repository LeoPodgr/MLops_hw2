-- Baseline DDL reference for assignment point 2.
-- The runnable project initializes this version as transactions_raw in
-- clickhouse/init/01_baseline_tables.sql. This file is a standalone reference.
CREATE TABLE transactions_db.transactions_raw
(
    event_time DateTime64(3),
    state String,
    category String,
    amount Float64,
    transaction_id String,
    raw_payload String
)
ENGINE = MergeTree
ORDER BY tuple();
