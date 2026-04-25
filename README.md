# Stock Market ETL Pipeline (Airflow + PostgreSQL)

Production-style take-home ETL project that ingests minute-level stock prices, applies transformation and quality checks, computes deterministic rolling analytics, and orchestrates everything with Apache Airflow.

## Problem Statement

Build an end-to-end micro-batch pipeline that continuously ingests stock market data and produces analytical outputs suitable for downstream reporting and monitoring.

## Why This Is Non-Trivial

- Ingestion must be **idempotent** in recurring 5-minute runs.
- External API responses can be empty, delayed, or schema-sensitive.
- Analytics must be **rerunnable and deterministic**, not append-only noise.
- Validation must fail the DAG when critical quality checks break.

## Tech Stack

- Orchestration: Apache Airflow
- Processing: Python, Pandas
- Data source: `yfinance`
- Storage: PostgreSQL
- Runtime: Docker Compose

## Architecture Diagram

```text
              +---------------------+
              |    Airflow DAG      |
              | stock_market_microbatch
              +----------+----------+
                         |
      +------------------+------------------+
      |                  |                  |
      v                  v                  v
create_schema     extract_load_prices   compute_analytics
(Postgres SQL)    (Python ETL)          (Postgres SQL)
      |                  |                  |
      +------------------+------------------+
                         |
                         v
                validate_outputs
                (Python data checks)
                         |
                         v
                    PostgreSQL
          stock_prices + stock_analytics
```

## Data Flow

1. `create_schema` creates tables and indexes from `sql/schema.sql`.
2. `extract_load_prices` reads symbols from Airflow Variable `stock_symbols` or `STOCK_SYMBOLS`.
3. ETL fetches 1-minute bars via `yfinance`, normalizes column names, validates required fields, type-casts, and deduplicates.
4. Batch insert loads into `stock_prices` using parameterized SQL and `ON CONFLICT (symbol, timestamp) DO NOTHING`.
5. `compute_analytics` executes `sql/analytics.sql` to upsert rolling metrics into `stock_analytics`.
6. `validate_outputs` enforces row count, duplicate, and null checks.
7. ETL run telemetry is persisted in `etl_run_audit` for operational visibility.

## Repository Structure

```text
.
├── dags/
│   └── stock_market_dag.py
├── scripts/
│   └── fetch_data.py                # Backward-compatible wrapper
├── src/
│   └── stock_pipeline/
│       ├── __init__.py
│       ├── config.py
│       ├── etl.py
│       └── validation.py
├── sql/
│   ├── schema.sql
│   └── analytics.sql
├── tests/
│   └── test_transformations.py
├── .env.example
├── .gitignore
├── docker-compose.yml
├── requirements.txt
└── README.md
```

## Setup

1. Copy environment template:
   - `cp .env.example .env` (Linux/macOS)  
   - `Copy-Item .env.example .env` (PowerShell)
2. Start the stack:
   - `docker compose up -d`
3. Wait for Airflow init + scheduler/webserver readiness.

## Run with Docker Compose

- Start services: `docker compose up -d`
- View logs: `docker compose logs -f airflow-scheduler`
- Stop services: `docker compose down`

## Airflow UI

- URL: [http://localhost:8080](http://localhost:8080)
- Default credentials:
  - Username: `admin`
  - Password: `admin`
- Enable DAG `stock_market_microbatch` and trigger manually or wait for schedule.

## PostgreSQL Inspection

Open a Postgres shell:

```bash
docker compose exec postgres psql -U airflow -d airflow
```

Useful queries:

```sql
SELECT COUNT(*) FROM stock_prices;
SELECT symbol, timestamp, price, volume FROM stock_prices ORDER BY timestamp DESC LIMIT 10;
SELECT * FROM stock_analytics ORDER BY window_end DESC LIMIT 10;
```

## Configuration Options

- `STOCK_SYMBOLS`: comma-separated symbols, default `AAPL,MSFT,NVDA`
- `MAX_DATA_AGE_HOURS`: freshness threshold for validation, default `72`
- `POSTGRES_HOST`, `POSTGRES_PORT`, `POSTGRES_DB`, `POSTGRES_USER`, `POSTGRES_PASSWORD`
- Airflow Variable `stock_symbols` overrides default symbol list.

## Validation / Success Criteria

Pipeline run is considered successful when:

- `stock_prices` has rows after ingestion.
- No duplicate `(symbol, timestamp)` rows exist.
- No null values in critical fields (`symbol`, `timestamp`, `price`, `volume`).
- `stock_analytics` has rows after compute step.
- Latest ingested market record is not older than `MAX_DATA_AGE_HOURS`.

Validation is enforced in DAG task `validate_outputs`.

## Tests

Run lightweight unit tests:

```bash
pytest -q
```

Current tests cover:

- Deduplication behavior in transformations
- Empty input handling
- Missing required columns
- SQL insert idempotency behavior (`ON CONFLICT DO NOTHING`)

## Operational Telemetry

Each symbol run writes an audit row into `etl_run_audit` with:

- extraction row count
- loaded row count
- run status (`success`/`failed`)
- optional error message

Useful query:

```sql
SELECT run_time, symbol, extracted_rows, loaded_rows, status, error_message
FROM etl_run_audit
ORDER BY run_time DESC
LIMIT 20;
```

## Schema Migrations

Versioned SQL migrations are supported through `sql/migrations/*.sql` and applied by DAG task `apply_migrations`. Applied files are tracked in `schema_migrations`.

## Limitations and Future Improvements

- `yfinance` remains an external dependency and can still return delayed data; this pipeline now includes retries and a fallback query window, but cannot control upstream availability.
- Market holidays/off-hours still reduce new-data frequency; freshness validation is configurable via `MAX_DATA_AGE_HOURS` to avoid false failures outside trading hours.
- Observability now includes ETL run audit logging; future work can add alert routing (email/Slack) on failures or stale data.
- Lightweight SQL file migrations are implemented; if schema churn grows significantly, this can be upgraded to Alembic.
