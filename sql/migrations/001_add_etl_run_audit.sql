CREATE TABLE IF NOT EXISTS etl_run_audit (
    id BIGSERIAL PRIMARY KEY,
    run_time TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    symbol VARCHAR(10) NOT NULL,
    extracted_rows INTEGER NOT NULL DEFAULT 0,
    loaded_rows INTEGER NOT NULL DEFAULT 0,
    status VARCHAR(20) NOT NULL,
    error_message TEXT
);

CREATE INDEX IF NOT EXISTS idx_etl_run_audit_run_time ON etl_run_audit (run_time DESC);
CREATE INDEX IF NOT EXISTS idx_etl_run_audit_symbol ON etl_run_audit (symbol);

