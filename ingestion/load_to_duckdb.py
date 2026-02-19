"""
ingestion/load_to_duckdb.py
────────────────────────────
Reads all Parquet files from the raw data directory and loads them
into the DuckDB warehouse as the raw.stock_prices table.
"""

import os
import glob
import logging

import duckdb

RAW_DIR = os.environ.get("DATA_DIR", "./data/raw")
DB_PATH = os.environ.get("DB_PATH", "./data/warehouse.duckdb")

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


def load_raw_to_duckdb() -> int:
    parquet_pattern = os.path.join(RAW_DIR, "*.parquet").replace("\\", "/")
    files = glob.glob(parquet_pattern)

    if not files:
        logger.warning(f"No Parquet files found in {RAW_DIR}")
        return 0

    logger.info(f"Found {len(files)} Parquet files to load")

    con = duckdb.connect(DB_PATH)

    try:
        con.execute("CREATE SCHEMA IF NOT EXISTS raw")

        con.execute("""
            CREATE TABLE IF NOT EXISTS raw.stock_prices (
                ticker       VARCHAR,
                trade_date   DATE,
                open         DOUBLE,
                high         DOUBLE,
                low          DOUBLE,
                close        DOUBLE,
                volume       BIGINT,
                ingested_at  TIMESTAMP,
                PRIMARY KEY (ticker, trade_date)
            )
        """)

        con.execute(f"""
            INSERT OR REPLACE INTO raw.stock_prices
            SELECT
                ticker,
                CAST(trade_date AS DATE)       AS trade_date,
                CAST(open       AS DOUBLE)     AS open,
                CAST(high       AS DOUBLE)     AS high,
                CAST(low        AS DOUBLE)     AS low,
                CAST(close      AS DOUBLE)     AS close,
                CAST(volume     AS BIGINT)     AS volume,
                CAST(ingested_at AS TIMESTAMP) AS ingested_at
            FROM read_parquet('{parquet_pattern}')
        """)

        row_count = con.execute("SELECT COUNT(*) FROM raw.stock_prices").fetchone()[0]
        logger.info(f"raw.stock_prices now has {row_count:,} rows")
        return row_count

    finally:
        con.close()


if __name__ == "__main__":
    load_raw_to_duckdb()