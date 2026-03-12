from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys

sys.path.insert(0, '/opt/airflow/ingestion')

from ingest import fetch_series, load_to_db
from db import get_connection, create_raw_table

SERIES = {
    "GDP": "Gross Domestic Product",
    "CPIAUCSL": "CPI - Inflation",
    "UNRATE": "Unemployment Rate",
    "FEDFUNDS": "Federal Funds Rate",
}

default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

def run_ingestion():
    conn = get_connection()
    create_raw_table(conn)
    for series_id, label in SERIES.items():
        print(f"Fetching {label} ({series_id})...")
        observations = fetch_series(series_id)
        load_to_db(conn, series_id, observations)
        print(f"  → Loaded {len(observations)} rows")
    conn.close()

with DAG(
    dag_id="fred_ingestion",
    default_args=default_args,
    description="Ingest FRED economic indicators into Postgres",
    schedule_interval="0 6 * * 1",  # every Monday at 6am
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:

    ingest_task = PythonOperator(
        task_id="ingest_fred_data",
        python_callable=run_ingestion,
    )
