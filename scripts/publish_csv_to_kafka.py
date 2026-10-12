#!/usr/bin/env python3
"""Publish rows from a CSV to Kafka as JSONEachRow-compatible messages.

The full original row is kept in raw_payload; the named fields are normalized
for the ClickHouse DDL. Common aliases from credit-card fraud CSV datasets are
accepted (e.g. amt/amount, trans_date_trans_time/transaction_time).
"""
from __future__ import annotations

import argparse
import csv
import json
import math
import os
import re
import sys
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Dict, Iterable, Optional


def normalize_name(value: str) -> str:
    return re.sub(r"[^a-z0-9]", "", value.lower())


def resolve_column(fieldnames: list[str], aliases: tuple[str, ...], logical_name: str,
                   required: bool = True) -> Optional[str]:
    by_normalized = {normalize_name(name): name for name in fieldnames if name}
    for alias in aliases:
        found = by_normalized.get(normalize_name(alias))
        if found:
            return found
    if required:
        raise ValueError(
            f"Не найдена колонка '{logical_name}'. Ожидался один из вариантов: "
            f"{', '.join(aliases)}. Колонки CSV: {', '.join(fieldnames)}"
        )
    return None


def parse_datetime(value: str, line_number: int) -> str:
    value = (value or "").strip()
    if not value:
        raise ValueError(f"Строка {line_number}: пустая дата/время транзакции")
    candidate = value.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(candidate)
    except ValueError:
        parsed = None
        for fmt in ("%m/%d/%Y %H:%M", "%m/%d/%Y %H:%M:%S", "%Y-%m-%d"):
            try:
                parsed = datetime.strptime(value, fmt)
                break
            except ValueError:
                continue
        if parsed is None:
            raise ValueError(
                f"Строка {line_number}: не удалось разобрать дату '{value}'. "
                "Ожидается ISO YYYY-MM-DD HH:MM:SS или распространённый формат CSV."
            )
    if parsed.tzinfo is not None:
        parsed = parsed.astimezone(timezone.utc).replace(tzinfo=None)
    return parsed.isoformat(sep=" ", timespec="milliseconds")


def normalize_record(row: Dict[str, Any], columns: Dict[str, Optional[str]], line_number: int) -> dict:
    def get(logical: str) -> str:
        source = columns[logical]
        return (str(row.get(source, "")) if source else "").strip()

    state = get("state") or "UNKNOWN"
    category = get("category") or "UNKNOWN"
    try:
        amount_decimal = Decimal(get("amount").replace(",", "."))
    except (InvalidOperation, ValueError):
        raise ValueError(f"Строка {line_number}: некорректная сумма '{get('amount')}'")
    if not amount_decimal.is_finite():
        raise ValueError(f"Строка {line_number}: сумма должна быть конечным числом")
    amount = float(amount_decimal)
    if not math.isfinite(amount):
        raise ValueError(f"Строка {line_number}: сумма вне диапазона Float64")

    source_id = get("transaction_id")
    transaction_id = source_id or f"csv-row-{line_number - 1}"
    event_time = parse_datetime(get("event_time"), line_number)

    # JSON payload makes the baseline table retain original CSV fields, including
    # fields not needed for the requested aggregation.
    raw_payload = json.dumps(row, ensure_ascii=False, separators=(",", ":"), default=str)
    return {
        "event_time": event_time,
        "state": state,
        "category": category,
        "amount": amount,
        "transaction_id": transaction_id,
        "raw_payload": raw_payload,
    }


def iter_records(csv_path: Path, limit: Optional[int] = None) -> Iterable[dict]:
    with csv_path.open("r", newline="", encoding="utf-8-sig") as handle:
        reader = csv.DictReader(handle)
        if not reader.fieldnames:
            raise ValueError(f"CSV пуст или не содержит заголовка: {csv_path}")
        columns: Dict[str, Optional[str]] = {
            "event_time": resolve_column(reader.fieldnames, (
                "trans_date_trans_time", "transaction_datetime", "transaction_time",
                "transaction_date_time", "transaction_date", "timestamp", "date"
            ), "дата/время транзакции"),
            "state": resolve_column(reader.fieldnames, (
                "state", "cardholder_state", "merchant_state", "cardholder state"
            ), "штат"),
            "category": resolve_column(reader.fieldnames, (
                "category", "merchant_category", "merchant category"
            ), "категория"),
            "amount": resolve_column(reader.fieldnames, (
                "amt", "amount", "transaction_amount", "transaction amount"
            ), "сумма"),
            "transaction_id": resolve_column(reader.fieldnames, (
                "trans_num", "transaction_id", "transaction_number", "id"
            ), "ID транзакции", required=False),
        }
        print("CSV columns used:", columns, file=sys.stderr)
        sent = 0
        for line_number, row in enumerate(reader, start=2):
            if not row or not any((value or "").strip() for value in row.values()):
                continue
            yield normalize_record(row, columns, line_number)
            sent += 1
            if limit is not None and sent >= limit:
                break


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--csv", required=True, help="Path inside container, e.g. /app/data/FraudTrain.csv")
    parser.add_argument("--topic", default=os.getenv("KAFKA_TOPIC", "transactions"))
    parser.add_argument("--bootstrap-servers", default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"))
    parser.add_argument("--batch-size", type=int, default=1000)
    parser.add_argument("--limit", type=int, default=None, help="Optional row limit for smoke tests")
    args = parser.parse_args()

    if args.batch_size < 1:
        parser.error("--batch-size must be >= 1")
    csv_path = Path(args.csv)
    if not csv_path.is_file():
        parser.error(f"CSV file not found: {csv_path}")

    try:
        from kafka import KafkaProducer
    except ImportError:
        print("Missing dependency. Install requirements.txt or run via Docker Compose.", file=sys.stderr)
        return 2

    producer = KafkaProducer(
        bootstrap_servers=args.bootstrap_servers.split(","),
        value_serializer=lambda value: json.dumps(value, ensure_ascii=False, separators=(",", ":")).encode("utf-8"),
        acks="all",
        retries=10,
        max_in_flight_requests_per_connection=1,
        linger_ms=10,
        request_timeout_ms=30000,
    )
    sent = 0
    futures = []
    try:
        for record in iter_records(csv_path, args.limit):
            futures.append(producer.send(args.topic, value=record))
            sent += 1
            if len(futures) >= args.batch_size:
                producer.flush(timeout=60)
                for future in futures:
                    future.get(timeout=30)
                futures.clear()
                print(f"Published {sent:,} rows...", flush=True)
        producer.flush(timeout=120)
        for future in futures:
            future.get(timeout=30)
    except Exception as exc:  # print actionable error; fail container with non-zero exit
        print(f"Publish failed after {sent:,} rows: {exc}", file=sys.stderr)
        return 1
    finally:
        producer.close(timeout=30)
    print(f"Successfully published {sent:,} rows to topic '{args.topic}' at {args.bootstrap_servers}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
