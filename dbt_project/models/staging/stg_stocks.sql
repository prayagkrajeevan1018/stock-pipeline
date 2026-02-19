-- Staging layer: clean, rename, cast, and deduplicate raw stock data

WITH source AS (

    SELECT * FROM {{ source('raw', 'stock_prices') }}

),

cleaned AS (

    SELECT
        ticker || '_' || CAST(trade_date AS VARCHAR) AS stock_id,
        UPPER(ticker)                                AS ticker,
        CAST(trade_date AS DATE)                     AS trade_date,
        ROUND(CAST(open   AS DOUBLE), 4)             AS open_price,
        ROUND(CAST(high   AS DOUBLE), 4)             AS high_price,
        ROUND(CAST(low    AS DOUBLE), 4)             AS low_price,
        ROUND(CAST(close  AS DOUBLE), 4)             AS close_price,
        CAST(volume AS BIGINT)                       AS volume,
        ROUND(high - low, 4)                         AS price_range,
        CAST(ingested_at AS TIMESTAMP)               AS ingested_at

    FROM source

    WHERE
        ticker IS NOT NULL
        AND trade_date IS NOT NULL
        AND close IS NOT NULL
        AND close > 0

),

deduped AS (

    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY ticker, trade_date
            ORDER BY ingested_at DESC
        ) AS row_num
    FROM cleaned

)

SELECT
    stock_id,
    ticker,
    trade_date,
    open_price,
    high_price,
    low_price,
    close_price,
    volume,
    price_range,
    ingested_at
FROM deduped
WHERE row_num = 1