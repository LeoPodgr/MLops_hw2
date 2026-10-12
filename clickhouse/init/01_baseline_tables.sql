CREATE DATABASE IF NOT EXISTS transactions_db;

-- Kafka source. The Python producer normalizes the fields needed by the task
-- and preserves the complete source row in raw_payload.
CREATE TABLE IF NOT EXISTS transactions_db.kafka_transactions
(
    event_time DateTime64(3),
    state String,
    category String,
    amount Float64,
    transaction_id String,
    raw_payload String
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:29092',
    kafka_topic_list = 'transactions',
    kafka_group_name = 'clickhouse_transactions_group',
    kafka_format = 'JSONEachRow',
    kafka_num_consumers = 1,
    kafka_skip_broken_messages = 0;

-- Baseline / comparison table: generic types, no partitioning or useful sort key.
CREATE TABLE IF NOT EXISTS transactions_db.transactions_raw
(
    event_time DateTime64(3),
    state String,
    category String,
    amount Float64,
    transaction_id String,
    raw_payload String CODEC(ZSTD(1))
)
ENGINE = MergeTree
ORDER BY tuple();
