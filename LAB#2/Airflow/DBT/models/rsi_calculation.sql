-- models/rsi_calculation.sql

WITH price_changes AS (
    SELECT
        date,
        close,
        LAG(close) OVER (PARTITION BY symbol ORDER BY date) AS prev_close,
        close - LAG(close) OVER (PARTITION BY symbol ORDER BY date) AS price_change,
        symbol
    FROM {{ source('stock_analysis_project', 'market_data') }}
),

gain_loss AS (
    SELECT
        date,
        symbol,
        CASE WHEN price_change > 0 THEN price_change ELSE 0 END AS gain,
        CASE WHEN price_change < 0 THEN ABS(price_change) ELSE 0 END AS loss
    FROM price_changes
),

avg_gain_loss AS (
    SELECT
        date,
        symbol,
        AVG(gain) OVER (PARTITION BY symbol ORDER BY date ROWS BETWEEN 13 PRECEDING AND CURRENT ROW) AS avg_gain_14d,
        AVG(loss) OVER (PARTITION BY symbol ORDER BY date ROWS BETWEEN 13 PRECEDING AND CURRENT ROW) AS avg_loss_14d
    FROM gain_loss
),

rsi_calculation AS (
    SELECT
        date,
        symbol,
        avg_gain_14d,
        avg_loss_14d,
        CASE
            WHEN avg_loss_14d = 0 THEN 100
            ELSE 100 - (100 / (1 + (avg_gain_14d / avg_loss_14d)))
        END AS rsi_14d
    FROM avg_gain_loss
)

SELECT * FROM rsi_calculation
