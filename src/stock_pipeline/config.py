import os
from dataclasses import dataclass


@dataclass(frozen=True)
class DatabaseConfig:
    host: str
    port: int
    dbname: str
    user: str
    password: str


def get_database_config() -> DatabaseConfig:
    return DatabaseConfig(
        host=os.getenv("POSTGRES_HOST", "postgres"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        dbname=os.getenv("POSTGRES_DB", "airflow"),
        user=os.getenv("POSTGRES_USER", "airflow"),
        password=os.getenv("POSTGRES_PASSWORD", "airflow"),
    )


def get_symbols_from_env() -> list[str]:
    raw_symbols = os.getenv("STOCK_SYMBOLS", "AAPL,MSFT,NVDA")
    return [symbol.strip().upper() for symbol in raw_symbols.split(",") if symbol.strip()]

