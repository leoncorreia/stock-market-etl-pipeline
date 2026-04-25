import pandas as pd

from stock_pipeline.etl import stock_prices_insert_query, transform_history


def test_transform_history_deduplicates_symbol_timestamp() -> None:
    source = pd.DataFrame(
        {
            "Datetime": [
                "2026-01-01T10:00:00Z",
                "2026-01-01T10:00:00Z",
                "2026-01-01T10:01:00Z",
            ],
            "Close": [100.10, 101.10, 102.10],
            "Volume": [1000, 1100, 1200],
        }
    )

    transformed = transform_history(source, "AAPL")

    assert len(transformed) == 2
    ten_am = transformed[transformed["timestamp"] == pd.Timestamp("2026-01-01 10:00:00")]
    assert float(ten_am["price"].iloc[0]) == 101.10


def test_transform_history_handles_empty_data() -> None:
    transformed = transform_history(pd.DataFrame(), "MSFT")
    assert transformed.empty
    assert list(transformed.columns) == ["symbol", "timestamp", "price", "volume"]


def test_transform_history_raises_for_missing_required_columns() -> None:
    source = pd.DataFrame({"Datetime": ["2026-01-01T10:00:00Z"], "Close": [50.0]})
    try:
        transform_history(source, "NVDA")
    except ValueError as exc:
        assert "Missing required columns" in str(exc)
    else:
        raise AssertionError("Expected ValueError for missing required columns")


def test_insert_query_is_parameterized_and_idempotent() -> None:
    query = stock_prices_insert_query()
    assert "%s" in query
    assert "ON CONFLICT (symbol, timestamp) DO NOTHING" in query

