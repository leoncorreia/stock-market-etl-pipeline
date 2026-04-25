from __future__ import annotations

import logging
from pathlib import Path

import psycopg2

from stock_pipeline.config import get_database_config

logger = logging.getLogger(__name__)

MIGRATIONS_DIR = Path("/opt/airflow/sql/migrations")


def apply_sql_migrations() -> None:
    config = get_database_config()

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
                CREATE TABLE IF NOT EXISTS schema_migrations (
                    version VARCHAR(255) PRIMARY KEY,
                    applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                );
                """
            )
            conn.commit()

            if not MIGRATIONS_DIR.exists():
                logger.info("No migrations directory found at %s", MIGRATIONS_DIR)
                return

            migration_files = sorted(MIGRATIONS_DIR.glob("*.sql"))
            for migration in migration_files:
                version = migration.name
                cursor.execute("SELECT 1 FROM schema_migrations WHERE version = %s;", (version,))
                if cursor.fetchone():
                    continue

                logger.info("Applying migration %s", version)
                cursor.execute(migration.read_text(encoding="utf-8"))
                cursor.execute(
                    "INSERT INTO schema_migrations (version) VALUES (%s);",
                    (version,),
                )
                conn.commit()

