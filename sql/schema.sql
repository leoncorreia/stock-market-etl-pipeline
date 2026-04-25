CREATE TABLE IF NOT EXISTS stock_prices (
    id BIGSERIAL PRIMARY KEY,
    symbol VARCHAR(10) NOT NULL,
    timestamp TIMESTAMPTZ NOT NULL,
    price NUMERIC(14, 4) NOT NULL,
    volume BIGINT NOT NULL,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_stock_prices_symbol_timestamp UNIQUE (symbol, timestamp)
);

DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_name = 'stock_prices' AND column_name = 'ticker'
    ) AND NOT EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_name = 'stock_prices' AND column_name = 'symbol'
    ) THEN
        EXECUTE 'ALTER TABLE stock_prices RENAME COLUMN ticker TO symbol';
    END IF;
END $$;

ALTER TABLE stock_prices ADD COLUMN IF NOT EXISTS symbol VARCHAR(10);
ALTER TABLE stock_prices ADD COLUMN IF NOT EXISTS timestamp TIMESTAMPTZ;
ALTER TABLE stock_prices ADD COLUMN IF NOT EXISTS price NUMERIC(14, 4);
ALTER TABLE stock_prices ADD COLUMN IF NOT EXISTS volume BIGINT;
ALTER TABLE stock_prices ADD COLUMN IF NOT EXISTS ingested_at TIMESTAMPTZ DEFAULT NOW();

DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_name = 'stock_prices' AND column_name = 'ticker'
    ) THEN
        EXECUTE 'UPDATE stock_prices SET symbol = ticker WHERE symbol IS NULL';
    END IF;
END $$;

DELETE FROM stock_prices
WHERE symbol IS NULL OR timestamp IS NULL OR price IS NULL OR volume IS NULL;

WITH duplicates AS (
    SELECT
        ctid,
        ROW_NUMBER() OVER (PARTITION BY symbol, timestamp ORDER BY ingested_at DESC NULLS LAST, ctid DESC) AS rn
    FROM stock_prices
)
DELETE FROM stock_prices
WHERE ctid IN (SELECT ctid FROM duplicates WHERE rn > 1);

ALTER TABLE stock_prices ALTER COLUMN symbol SET NOT NULL;
ALTER TABLE stock_prices ALTER COLUMN timestamp SET NOT NULL;
ALTER TABLE stock_prices ALTER COLUMN price SET NOT NULL;
ALTER TABLE stock_prices ALTER COLUMN volume SET NOT NULL;
ALTER TABLE stock_prices ALTER COLUMN ingested_at SET NOT NULL;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'uq_stock_prices_symbol_timestamp'
    ) THEN
        EXECUTE 'ALTER TABLE stock_prices ADD CONSTRAINT uq_stock_prices_symbol_timestamp UNIQUE (symbol, timestamp)';
    END IF;
END $$;

CREATE INDEX IF NOT EXISTS idx_stock_prices_symbol ON stock_prices (symbol);
CREATE INDEX IF NOT EXISTS idx_stock_prices_timestamp ON stock_prices (timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_stock_prices_symbol_timestamp ON stock_prices (symbol, timestamp DESC);

CREATE TABLE IF NOT EXISTS stock_analytics (
    symbol VARCHAR(10) NOT NULL,
    window_end TIMESTAMPTZ NOT NULL,
    latest_price NUMERIC(14, 4) NOT NULL,
    ma_5 NUMERIC(14, 4) NOT NULL,
    ma_20 NUMERIC(14, 4),
    max_price_20 NUMERIC(14, 4) NOT NULL,
    volume_5_period BIGINT NOT NULL,
    calculated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT pk_stock_analytics PRIMARY KEY (symbol, window_end)
);

ALTER TABLE stock_analytics ADD COLUMN IF NOT EXISTS symbol VARCHAR(10);
ALTER TABLE stock_analytics ADD COLUMN IF NOT EXISTS window_end TIMESTAMPTZ;
ALTER TABLE stock_analytics ADD COLUMN IF NOT EXISTS latest_price NUMERIC(14, 4);
ALTER TABLE stock_analytics ADD COLUMN IF NOT EXISTS ma_5 NUMERIC(14, 4);
ALTER TABLE stock_analytics ADD COLUMN IF NOT EXISTS ma_20 NUMERIC(14, 4);
ALTER TABLE stock_analytics ADD COLUMN IF NOT EXISTS max_price_20 NUMERIC(14, 4);
ALTER TABLE stock_analytics ADD COLUMN IF NOT EXISTS volume_5_period BIGINT;
ALTER TABLE stock_analytics ADD COLUMN IF NOT EXISTS calculated_at TIMESTAMPTZ DEFAULT NOW();

CREATE INDEX IF NOT EXISTS idx_stock_analytics_symbol ON stock_analytics (symbol);
CREATE INDEX IF NOT EXISTS idx_stock_analytics_window_end ON stock_analytics (window_end DESC);

