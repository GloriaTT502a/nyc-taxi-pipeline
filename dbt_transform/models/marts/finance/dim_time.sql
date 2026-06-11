{{ 
    config(
        materialized='table',
        tags=['marts', 'dimension']
    ) 
}}
with hours as (
    select explode(sequence(0, 23)) as hour_of_day
),
minutes as (
    select explode(sequence(0, 59)) as minute_of_hour
)
select
    -- 主键：HHMM 格式的整数，如 830, 1745
    (h.hour_of_day * 100) + m.minute_of_hour as time_id,
    
    h.hour_of_day,
    m.minute_of_hour,
    
    -- 格式化为字符串用于展示 (如 '08:30')
    lpad(cast(h.hour_of_day as string), 2, '0') || ':' || lpad(cast(m.minute_of_hour as string), 2, '0') as time_string,
    
    -- 业务分段 (Dayparting)
    case 
        when h.hour_of_day >= 6 and h.hour_of_day < 12 then 'Morning'
        when h.hour_of_day >= 12 and h.hour_of_day < 16 then 'Afternoon'
        when h.hour_of_day >= 16 and h.hour_of_day < 20 then 'Evening'
        else 'Night'
    end as time_period,
    
    -- 粗略的高峰期标记 (仅看时钟时间，结合 dim_date.is_weekend 一起用才完美)
    case 
        when h.hour_of_day in (7, 8, 9) then 'Morning Rush'
        when h.hour_of_day in (16, 17, 18, 19) then 'Evening Rush'
        else 'Off-Peak'
    end as peak_period_type
from hours h
cross join minutes m