{% snapshot stock_prices_snapshot %}

{{
    config(
        target_schema='SNAPSHOTS',              
        target_database='New_Data',             
        unique_key='date_symbol',                    
        strategy='timestamp',
        updated_at='updated_at'                       
    )
}}

SELECT
    SYMBOL,
    DATE,
    OPEN,
    MAX,
    MIN,
    CLOSE,
    VOLUME,
    CONCAT(date, '_', symbol) AS date_symbol,
    CURRENT_TIMESTAMP AS updated_at
FROM {{ source('stock_analysis_project', 'market_data') }}

{% endsnapshot %}

