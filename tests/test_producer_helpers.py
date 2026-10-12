"""Lightweight tests for CSV normalisation (does not require a running Kafka)."""
from scripts.publish_csv_to_kafka import normalize_record, resolve_column


def test_resolve_aliases_and_normalize_row():
    source = {
        "trans_date_trans_time": "2023-01-02 12:00:00",
        "category": "travel",
        "amt": "399.99",
        "state": "CA",
        "trans_num": "txn-002",
    }
    fields = list(source)
    columns = {
        "event_time": resolve_column(fields, ("trans_date_trans_time",), "time"),
        "state": resolve_column(fields, ("state",), "state"),
        "category": resolve_column(fields, ("category",), "category"),
        "amount": resolve_column(fields, ("amt",), "amount"),
        "transaction_id": resolve_column(fields, ("trans_num",), "id", required=False),
    }
    record = normalize_record(source, columns, 2)
    assert record["state"] == "CA"
    assert record["category"] == "travel"
    assert record["amount"] == 399.99
    assert record["transaction_id"] == "txn-002"
    assert record["event_time"].startswith("2023-01-02 12:00:00")
    assert '"amt":"399.99"' in record["raw_payload"]
