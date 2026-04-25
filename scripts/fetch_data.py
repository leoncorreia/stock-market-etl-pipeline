"""Backward-compatible wrapper around the src ETL module."""

from stock_pipeline.etl import run_extract_load


def fetch_and_store_stock_data(ticker_symbol: str) -> int:
    return run_extract_load([ticker_symbol])