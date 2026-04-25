from __future__ import annotations

import logging
import os

import psycopg2

from stock_pipeline.config import get_database_config

logger = logging.getLogger(__name__)


def validate_pipeline_outputs() -> None:
    max_data_age_hours = int(os.getenv("MAX_DATA_AGE_HOURS", "72"))
    config = get_database_config()
    checks = {
        "stock_prices_has_rows": "SELECT COUNT(*) FROM stock_prices;",
        "stock_prices_duplicate_keys": """
            SELECT COUNT(*) FROM (
                SELECT symbol, timestamp
                FROM stock_prices
                GROUP BY symbol, timestamp
                HAVING COUNT(*) > 1
            ) duplicates;
        """,
        "stock_prices_null_critical_fields": """
            SELECT COUNT(*) FROM stock_prices
            WHERE symbol IS NULL OR timestamp IS NULL OR price IS NULL OR volume IS NULL;
        """,
        "stock_analytics_has_rows": "SELECT COUNT(*) FROM stock_analytics;",
        "latest_price_timestamp": "SELECT MAX(timestamp) FROM stock_prices;",
    }

    with psycopg2.connect(
        host=config.host,
        port=config.port,
        dbname=config.dbname,
        user=config.user,
        password=config.password,
    ) as conn:
        with conn.cursor() as cursor:
            results = {}
            for check_name, sql in checks.items():
                cursor.execute(sql)
                results[check_name] = cursor.fetchone()[0]

    if results["stock_prices_has_rows"] <= 0:
        raise ValueError("Validation failed: stock_prices table has no rows.")
    if results["stock_prices_duplicate_keys"] > 0:
        raise ValueError("Validation failed: duplicate symbol+timestamp rows exist in stock_prices.")
    if results["stock_prices_null_critical_fields"] > 0:
        raise ValueError("Validation failed: null critical fields found in stock_prices.")
    if results["stock_analytics_has_rows"] <= 0:
        raise ValueError("Validation failed: stock_analytics table has no rows.")
    if results["latest_price_timestamp"] is None:
        raise ValueError("Validation failed: stock_prices has no maximum timestamp.")

    with psycopg2.connect(
        host=config.host,
        port=config.port,
        dbname=config.dbname,
        user=config.user,
        password=config.password,
    ) as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT EXTRACT(EPOCH FROM (NOW() - MAX(timestamp))) / 3600.0
                FROM stock_prices;
                """
            )
            data_age_hours = float(cursor.fetchone()[0])

    if data_age_hours > max_data_age_hours:
        raise ValueError(
            f"Validation failed: latest stock_prices record is stale ({data_age_hours:.2f}h old), "
            f"max allowed is {max_data_age_hours}h."
        )

    logger.info("Validation checks passed: %s, data_age_hours=%.2f", results, data_age_hours)

