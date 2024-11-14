-- models/stock_data.sql
WITH raw_stock_data AS (
    SELECT 
        date,
        symbol,
        open,
        max,
        min,
        close,
        volume
    FROM {{ source('stock_analysis_project', 'market_data') }} 
)

SELECT
    date,
    symbol,
    open,
    max,
    min,
    close,
    volume
FROM raw_stock_data
