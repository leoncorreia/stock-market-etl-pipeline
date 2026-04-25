from __future__ import annotations

import logging
import time
from datetime import datetime, timezone
from typing import Iterable

import pandas as pd

from stock_pipeline.config import DatabaseConfig, get_database_config, get_symbols_from_env

logger = logging.getLogger(__name__)

REQUIRED_COLUMNS = {"timestamp", "close", "volume"}


def stock_prices_insert_query() -> str:
    return """
        INSERT INTO stock_prices (symbol, timestamp, price, volume, ingested_at)
        VALUES %s
        ON CONFLICT (symbol, timestamp) DO NOTHING;
    """


def fetch_symbol_history(symbol: str) -> pd.DataFrame:
    import yfinance as yf

    ticker = yf.Ticker(symbol)
    attempts = [
        {"period": "1d", "interval": "1m"},
        {"period": "5d", "interval": "5m"},
    ]

    for attempt_index, params in enumerate(attempts, start=1):
        retries = 3
        for retry in range(1, retries + 1):
            try:
                logger.info(
                    "Fetching yfinance history for symbol=%s period=%s interval=%s retry=%s",
                    symbol,
                    params["period"],
                    params["interval"],
                    retry,
                )
                history = ticker.history(
                    period=params["period"],
                    interval=params["interval"],
                    auto_adjust=False,
                    actions=False,
                )
                if history.empty:
                    logger.warning(
                        "Empty yfinance response for symbol=%s period=%s interval=%s",
                        symbol,
                        params["period"],
                        params["interval"],
                    )
                    break
                return history.reset_index()
            except Exception as exc:
                logger.warning(
                    "yfinance request failed for symbol=%s attempt=%s retry=%s error=%s",
                    symbol,
                    attempt_index,
                    retry,
                    exc,
                )
                if retry < retries:
                    time.sleep(retry)

    logger.warning("No usable data returned for symbol=%s after retries and fallback.", symbol)
    return pd.DataFrame()


def transform_history(df: pd.DataFrame, symbol: str) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=["symbol", "timestamp", "price", "volume"])

    transformed = df.copy()
    transformed.columns = [column.strip().lower() for column in transformed.columns]

    if "datetime" in transformed.columns and "timestamp" not in transformed.columns:
        transformed = transformed.rename(columns={"datetime": "timestamp"})

    missing_columns = REQUIRED_COLUMNS.difference(transformed.columns)
    if missing_columns:
        raise ValueError(f"Missing required columns for symbol={symbol}: {sorted(missing_columns)}")

    transformed["timestamp"] = pd.to_datetime(transformed["timestamp"], errors="coerce", utc=True)
    transformed["price"] = pd.to_numeric(transformed["close"], errors="coerce")
    transformed["volume"] = pd.to_numeric(transformed["volume"], errors="coerce")
    transformed["symbol"] = symbol.upper()

    transformed = transformed.dropna(subset=["symbol", "timestamp", "price", "volume"]).copy()
    transformed["timestamp"] = transformed["timestamp"].dt.tz_convert(None)
    transformed["volume"] = transformed["volume"].astype("int64")
    transformed = transformed[["symbol", "timestamp", "price", "volume"]]
    transformed = transformed.drop_duplicates(subset=["symbol", "timestamp"], keep="last")

    return transformed.sort_values("timestamp")


def insert_stock_prices(rows: pd.DataFrame, db_config: DatabaseConfig) -> int:
    import psycopg2
    from psycopg2.extras import execute_values

    if rows.empty:
        logger.info("No rows to insert after transformations.")
        return 0

    query = stock_prices_insert_query()

    payload = [
        (
            row.symbol,
            row.timestamp.to_pydatetime() if hasattr(row.timestamp, "to_pydatetime") else row.timestamp,
            float(row.price),
            int(row.volume),
            datetime.now(timezone.utc),
        )
        for row in rows.itertuples(index=False)
    ]

    with psycopg2.connect(
        host=db_config.host,
        port=db_config.port,
        dbname=db_config.dbname,
        user=db_config.user,
        password=db_config.password,
    ) as conn:
        with conn.cursor() as cursor:
            execute_values(cursor, query, payload, page_size=500)
            inserted = cursor.rowcount if cursor.rowcount and cursor.rowcount > 0 else 0
        conn.commit()

    logger.info("Insert attempted for %s rows; inserted=%s", len(payload), inserted)
    return inserted


def write_etl_audit_log(
    db_config: DatabaseConfig,
    symbol: str,
    extracted_rows: int,
    loaded_rows: int,
    status: str,
    error_message: str | None = None,
) -> None:
    import psycopg2

    with psycopg2.connect(
        host=db_config.host,
        port=db_config.port,
        dbname=db_config.dbname,
        user=db_config.user,
        password=db_config.password,
    ) as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO etl_run_audit (
                    run_time,
                    symbol,
                    extracted_rows,
                    loaded_rows,
                    status,
                    error_message
                )
                VALUES (%s, %s, %s, %s, %s, %s)
                """,
                (
                    datetime.now(timezone.utc),
                    symbol,
                    extracted_rows,
                    loaded_rows,
                    status,
                    error_message,
                ),
            )
        conn.commit()


def run_extract_load(symbols: Iterable[str], db_config: DatabaseConfig | None = None) -> int:
    config = db_config or get_database_config()
    total_inserted = 0

    for symbol in symbols:
        try:
            history = fetch_symbol_history(symbol)
            cleaned = transform_history(history, symbol)
            inserted = insert_stock_prices(cleaned, config)
            total_inserted += inserted
            write_etl_audit_log(
                config,
                symbol=symbol,
                extracted_rows=len(history.index) if not history.empty else 0,
                loaded_rows=inserted,
                status="success",
            )
            logger.info("Completed extract/load for symbol=%s inserted=%s", symbol, inserted)
        except Exception as exc:
            logger.exception("Extract/load failed for symbol=%s", symbol)
            write_etl_audit_log(
                config,
                symbol=symbol,
                extracted_rows=0,
                loaded_rows=0,
                status="failed",
                error_message=str(exc)[:500],
            )
            raise

    if total_inserted == 0:
        logger.warning("Pipeline completed with zero inserts for symbols=%s", list(symbols))

    return total_inserted


def run_extract_load_from_env(symbols: list[str] | None = None) -> int:
    selected_symbols = symbols or get_symbols_from_env()
    if not selected_symbols:
        raise ValueError("No stock symbols configured. Set STOCK_SYMBOLS or Airflow Variable stock_symbols.")
    logger.info("Starting extract/load for symbols=%s", selected_symbols)
    return run_extract_load(selected_symbols)

