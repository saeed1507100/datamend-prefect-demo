"""Daily orders ETL: vendor feed -> DuckDB warehouse."""

import json
from pathlib import Path

import duckdb
from prefect import flow, get_run_logger, task
from prefect.variables import Variable

FEED = Path(__file__).parent.parent / "data" / "vendor_orders.json"


@task
def extract() -> list[dict]:
    return json.loads(FEED.read_text())


@task
def transform(rows: list[dict]) -> list[tuple]:
    return [(int(r["id"]), r["customer"], float(r["amount"]), r["currency"]) for r in rows]


@task
def load(rows: list[tuple], warehouse: str, schema: str) -> int:
    con = duckdb.connect(warehouse)
    con.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
    con.execute(
        f"CREATE OR REPLACE TABLE {schema}.orders "
        "(order_id INTEGER, customer VARCHAR, amount DECIMAL(12, 2), currency VARCHAR)"
    )
    con.executemany(f"INSERT INTO {schema}.orders VALUES (?, ?, ?, ?)", rows)
    count = con.execute(f"SELECT count(*) FROM {schema}.orders").fetchone()[0]
    con.close()
    return count


@flow(name="orders-etl")
def orders_etl(target_schema: str = "analytics") -> int:
    logger = get_run_logger()
    warehouse = Variable.get("warehouse_path")
    rows = transform(extract())
    count = load(rows, warehouse, target_schema)
    logger.info("loaded %s orders into %s.orders", count, target_schema)
    return count


if __name__ == "__main__":
    orders_etl.serve(name="daily")
