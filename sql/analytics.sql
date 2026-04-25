WITH ranked_prices AS (
    SELECT
        symbol,
        timestamp,
        price,
        volume,
        ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY timestamp DESC) AS rn_desc
    FROM stock_prices
),
windowed AS (
    SELECT
        symbol,
        timestamp,
        price,
        volume,
        rn_desc
    FROM ranked_prices
    WHERE rn_desc <= 20
),
metrics AS (
    SELECT
        symbol,
        MAX(timestamp) AS window_end,
        MAX(price) FILTER (WHERE rn_desc = 1) AS latest_price,
        AVG(price) FILTER (WHERE rn_desc <= 5) AS ma_5,
        CASE
            WHEN COUNT(*) FILTER (WHERE rn_desc <= 20) = 20
                THEN AVG(price) FILTER (WHERE rn_desc <= 20)
            ELSE NULL
        END AS ma_20,
        MAX(price) FILTER (WHERE rn_desc <= 20) AS max_price_20,
        COALESCE(SUM(volume) FILTER (WHERE rn_desc <= 5), 0) AS volume_5_period
    FROM windowed
    GROUP BY symbol
)
INSERT INTO stock_analytics (
    symbol,
    window_end,
    latest_price,
    ma_5,
    ma_20,
    max_price_20,
    volume_5_period,
    calculated_at
)
SELECT
    symbol,
    window_end,
    latest_price,
    ma_5,
    ma_20,
    max_price_20,
    volume_5_period,
    NOW()
FROM metrics
ON CONFLICT (symbol, window_end)
DO UPDATE SET
    latest_price = EXCLUDED.latest_price,
    ma_5 = EXCLUDED.ma_5,
    ma_20 = EXCLUDED.ma_20,
    max_price_20 = EXCLUDED.max_price_20,
    volume_5_period = EXCLUDED.volume_5_period,
    calculated_at = NOW();

