-- Mart: Daily Returns and Financial Metrics

WITH base AS (

    SELECT * FROM {{ ref('stg_stocks') }}

),

with_prev_close AS (

    SELECT
        *,
        LAG(close_price) OVER (
            PARTITION BY ticker
            ORDER BY trade_date
        ) AS prev_close_price
    FROM base

),

with_returns AS (

    SELECT
        *,
        CASE
            WHEN prev_close_price IS NOT NULL AND prev_close_price > 0
            THEN ROUND((close_price - prev_close_price) / prev_close_price * 100, 4)
            ELSE NULL
        END AS daily_return_pct,

        CASE
            WHEN close_price > prev_close_price THEN 'UP'
            WHEN close_price < prev_close_price THEN 'DOWN'
            ELSE 'FLAT'
        END AS price_direction

    FROM with_prev_close

),

with_rolling AS (

    SELECT
        *,
        ROUND(STDDEV(daily_return_pct) OVER (
            PARTITION BY ticker
            ORDER BY trade_date
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ), 4) AS volatility_30d,

        ROUND(AVG(volume) OVER (
            PARTITION BY ticker
            ORDER BY trade_date
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ), 0) AS avg_volume_30d,

        ROUND(
            (close_price - FIRST_VALUE(close_price) OVER (
                PARTITION BY ticker ORDER BY trade_date
            )) / FIRST_VALUE(close_price) OVER (
                PARTITION BY ticker ORDER BY trade_date
            ) * 100
        , 2) AS cumulative_return_pct

    FROM with_returns

)

SELECT
    stock_id,
    ticker,
    trade_date,
    open_price,
    high_price,
    low_price,
    close_price,
    prev_close_price,
    volume,
    price_range,
    daily_return_pct,
    price_direction,
    volatility_30d,
    avg_volume_30d,
    cumulative_return_pct
FROM with_rolling
ORDER BY ticker, trade_date