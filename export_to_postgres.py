"""
Exports DuckDB mart tables to PostgreSQL so Metabase can connect.
"""

import duckdb
import pandas as pd
from sqlalchemy import create_engine

DUCKDB_PATH = "./data/warehouse.duckdb"
PG_CONN = "postgresql://airflow:airflow@localhost:5432/airflow"

engine = create_engine(PG_CONN)
con = duckdb.connect(DUCKDB_PATH)

tables = {
    "daily_returns":    "SELECT * FROM main_marts.daily_returns",
    "moving_averages":  "SELECT * FROM main_marts.moving_averages",
    "stg_stocks":       "SELECT * FROM main_staging.stg_stocks",
}

for table_name, query in tables.items():
    print(f"Exporting {table_name}...")
    df = con.execute(query).df()
    df.to_sql(table_name, engine, if_exists="replace", index=False)
    print(f"  ✓ {len(df):,} rows exported")

con.close()
print("\nAll tables exported to Postgres!")