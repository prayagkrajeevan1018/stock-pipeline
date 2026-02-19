"""
dags/stock_pipeline_dag.py
───────────────────────────
Orchestrates the end-to-end stock data pipeline daily.
"""

import sys
import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

sys.path.insert(0, "/opt/airflow")

logger = logging.getLogger(__name__)

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="stock_market_pipeline",
    default_args=default_args,
    description="End-to-end stock market data pipeline",
    schedule_interval="0 6 * * 1-5",
    start_date=days_ago(1),
    catchup=False,
    tags=["stocks", "etl", "dbt"],
) as dag:

    def fetch_data_task(**context):
        from ingestion.fetch_stocks import run_ingestion
        files = run_ingestion(lookback_days=7)
        context["ti"].xcom_push(key="parquet_files", value=files)

    def load_duckdb_task(**context):
        from ingestion.load_to_duckdb import load_raw_to_duckdb
        row_count = load_raw_to_duckdb()
        if row_count == 0:
            raise ValueError("No rows loaded into DuckDB!")

    def quality_check_task(**context):
        import duckdb
        con = duckdb.connect("/opt/airflow/data/warehouse.duckdb")
        checks = {
            "raw":     "SELECT COUNT(*) FROM raw.stock_prices",
            "staging": "SELECT COUNT(*) FROM main_staging.stg_stocks",
            "returns": "SELECT COUNT(*) FROM main_marts.daily_returns",
            "mavg":    "SELECT COUNT(*) FROM main_marts.moving_averages",
        }
        failed = []
        for name, query in checks.items():
            count = con.execute(query).fetchone()[0]
            logger.info(f"{name}: {count:,} rows")
            if count == 0:
                failed.append(name)
        con.close()
        if failed:
            raise ValueError(f"Quality checks FAILED: {failed}")

    fetch_data = PythonOperator(
        task_id="fetch_data",
        python_callable=fetch_data_task,
    )

    load_to_duckdb = PythonOperator(
        task_id="load_to_duckdb",
        python_callable=load_duckdb_task,
    )

    run_dbt = BashOperator(
        task_id="run_dbt_models",
        bash_command=(
            "cd /opt/airflow/dbt_project && "
            "dbt run --profiles-dir . && "
            "dbt test --profiles-dir ."
        ),
    )

    data_quality_check = PythonOperator(
        task_id="data_quality_check",
        python_callable=quality_check_task,
    )

    fetch_data >> load_to_duckdb >> run_dbt >> data_quality_check