-- models/moving_average.sql

select
    m.date,
    m.symbol,
    avg(m.close) over (partition by m.symbol order by m.date rows between 6 preceding and current row) as moving_avg_7d
from
    {{ source('stock_analysis_project', 'market_data') }} as m
