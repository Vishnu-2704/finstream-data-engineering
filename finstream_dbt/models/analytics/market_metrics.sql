{{ config(
    materialized='incremental',
    unique_key=['symbol', 'window_start', 'window_end']
) }}

WITH source_data AS (

    SELECT
        symbol,
        window_start,
        window_end,
        avg_price,
        min_price,
        max_price,
        tick_count
    FROM {{ source('finstream', 'market_metrics') }}

    {% if is_incremental() %}

    WHERE window_start >= (
        SELECT COALESCE(
            MAX(window_start) - INTERVAL '1 minute',
            '1900-01-01'
        )
        FROM {{ this }}
    )

    {% endif %}

),

price_data AS (

    SELECT
        symbol,
        window_start,
        window_end,
        avg_price,
        min_price,
        max_price,
        tick_count,

        LAG(avg_price) OVER (
            PARTITION BY symbol
            ORDER BY window_start
        ) AS previous_avg_price

    FROM source_data

)

SELECT
    symbol,
    window_start,
    window_end,
    avg_price,
    min_price,
    max_price,
    tick_count,
    previous_avg_price,

    avg_price - previous_avg_price AS price_change,

    (
        (avg_price - previous_avg_price)
        / NULLIF(previous_avg_price, 0)
    ) * 100 AS price_change_pct

FROM price_data
