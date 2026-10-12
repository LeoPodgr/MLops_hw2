#!/usr/bin/env python3
"""Run the per-state maximum-transaction query and export its result to CSV."""
from __future__ import annotations

import argparse
import os
from pathlib import Path

import clickhouse_connect

ROOT = Path(__file__).resolve().parents[1]
QUERIES = {
    "raw": ROOT / "clickhouse" / "queries" / "01_category_max_by_state.sql",
    "optimized": ROOT / "clickhouse" / "queries" / "02_category_max_by_state_optimized.sql",
}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mode", choices=tuple(QUERIES), default="optimized")
    parser.add_argument("--output", default="results/category_max_by_state.csv")
    parser.add_argument("--host", default=os.getenv("CLICKHOUSE_HOST", "localhost"))
    parser.add_argument("--port", type=int, default=int(os.getenv("CLICKHOUSE_PORT", "8123")))
    parser.add_argument("--database", default=os.getenv("CLICKHOUSE_DATABASE", "transactions_db"))
    parser.add_argument("--username", default=os.getenv("CLICKHOUSE_USER", "default"))
    parser.add_argument("--password", default=os.getenv("CLICKHOUSE_PASSWORD", ""))
    args = parser.parse_args()

    sql = QUERIES[args.mode].read_text(encoding="utf-8")
    # Strip comments so the ClickHouse HTTP client receives just the query.
    sql = "\n".join(line for line in sql.splitlines() if not line.lstrip().startswith("--"))
    client = clickhouse_connect.get_client(
        host=args.host,
        port=args.port,
        username=args.username,
        password=args.password,
        database=args.database,
    )
    try:
        df = client.query_df(sql)
    finally:
        client.close()

    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_path, index=False, encoding="utf-8")
    print(f"Wrote {len(df):,} state rows to {output_path.resolve()} (mode={args.mode})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
