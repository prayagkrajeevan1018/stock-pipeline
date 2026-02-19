-- Mart: Moving Averages and Technical Indicators

WITH base AS (

    SELECT
        stock_id,
        ticker,
        trade_date,
        close_price,
        volume
    FROM {{ ref('stg_stocks') }}

),

with_sma AS (

    SELECT
        *,
        ROUND(AVG(close_price) OVER (
            PARTITION BY ticker ORDER BY trade_date
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ), 4) AS sma_7,

        ROUND(AVG(close_price) OVER (
            PARTITION BY ticker ORDER BY trade_date
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ), 4) AS sma_30,

        ROUND(AVG(close_price) OVER (
            PARTITION BY ticker ORDER BY trade_date
            ROWS BETWEEN 89 PRECEDING AND CURRENT ROW
        ), 4) AS sma_90,

        ROUND(AVG(volume) OVER (
            PARTITION BY ticker ORDER BY trade_date
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ), 0) AS vol_sma_30

    FROM base

),

with_signals AS (

    SELECT
        *,
        CASE
            WHEN close_price > sma_30 THEN 'ABOVE_SMA30'
            WHEN close_price < sma_30 THEN 'BELOW_SMA30'
            ELSE 'AT_SMA30'
        END AS price_vs_sma30,

        CASE
            WHEN vol_sma_30 > 0 AND volume > (vol_sma_30 * 2)
            THEN TRUE ELSE FALSE
        END AS is_volume_spike

    FROM with_sma

)

SELECT
    stock_id,
    ticker,
    trade_date,
    close_price,
    volume,
    sma_7,
    sma_30,
    sma_90,
    vol_sma_30,
    price_vs_sma30,
    is_volume_spike
FROM with_signals
ORDER BY ticker, trade_date