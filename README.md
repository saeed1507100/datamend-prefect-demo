# datamend-prefect-demo

A small Prefect 3 pipeline used to demonstrate
[datamend](https://github.com/saeed1507100/datamend): it watches this project's
flow runs, and when one fails, an AI agent proposes a fix as a pull request with
validation evidence.

- `flows/orders.py`: the `orders-etl` flow. Reads `data/vendor_orders.json`,
  writes `<target_schema>.orders` in the DuckDB file named by the Prefect variable
  `warehouse_path`.
- `datamend.yaml`: how datamend runs this flow in shadow mode and what it compares.
