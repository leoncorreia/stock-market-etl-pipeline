from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

from stock_pipeline.etl import run_extract_load_from_env
from stock_pipeline.migrations import apply_sql_migrations
from stock_pipeline.validation import validate_pipeline_outputs

DEFAULT_SYMBOLS = "AAPL,MSFT,NVDA"


def _resolve_symbols() -> list[str]:
    symbols_value = Variable.get(
        "stock_symbols",
        default_var=DEFAULT_SYMBOLS,
    )
    return [symbol.strip().upper() for symbol in symbols_value.split(",") if symbol.strip()]


default_args = {
    "owner": "data_engineer",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}


with DAG(
    dag_id="stock_market_microbatch",
    default_args=default_args,
    description="Reliable 5-minute stock ETL + analytics pipeline",
    start_date=datetime(2026, 1, 1),
    schedule="*/5 * * * *",
    catchup=False,
    max_active_runs=1,
    template_searchpath=["/opt/airflow"],
    tags=["stocks", "etl", "microbatch", "postgres"],
) as dag:
    create_schema = PostgresOperator(
        task_id="create_schema",
        postgres_conn_id="postgres_default",
        sql="sql/schema.sql",
        doc_md="Create core warehouse tables and indexes if missing.",
    )

    apply_migrations = PythonOperator(
        task_id="apply_migrations",
        python_callable=apply_sql_migrations,
        doc_md="Apply versioned SQL migrations from sql/migrations.",
    )

    extract_load_prices = PythonOperator(
        task_id="extract_load_prices",
        python_callable=run_extract_load_from_env,
        op_kwargs={"symbols": _resolve_symbols()},
        doc_md="Fetch minute bars from yfinance and upsert into stock_prices.",
    )

    compute_analytics = PostgresOperator(
        task_id="compute_analytics",
        postgres_conn_id="postgres_default",
        sql="sql/analytics.sql",
        doc_md="Compute deterministic rolling analytics into stock_analytics.",
    )

    validate_outputs = PythonOperator(
        task_id="validate_outputs",
        python_callable=validate_pipeline_outputs,
        doc_md="Fail run if row counts, null checks, or duplicate checks fail.",
    )

    create_schema >> apply_migrations >> extract_load_prices >> compute_analytics >> validate_outputs