{{ 
    config(
        materialized='table',
        cluster_by=['time_id'], 
        tags=['dim', 'core', 'dimension', 'static']
    ) 
}}

with hours as (
    select explode(sequence(0, 23)) as hour_of_day
),
minutes as (
    select explode(sequence(0, 59)) as minute_of_hour
),
-- 将笛卡尔积转化为虚拟时间戳，以便极速调用原生格式化函数
time_spine as (
    select 
        h.hour_of_day,
        m.minute_of_hour,
        -- 生成一个 1970-01-01 的虚拟时间戳，专门用来做字符串格式化
        make_timestamp(1970, 1, 1, h.hour_of_day, m.minute_of_hour, 0) as dummy_ts
    from hours h
    cross join minutes m
), 
calculated_times as ( 
    select
        (hour_of_day * 100) + minute_of_hour as time_id,
        
        hour_of_day as hour_24,
        cast(date_format(dummy_ts, 'h') as int) as hour_12,
        minute_of_hour,
        date_format(dummy_ts, 'a') as am_pm,
        
        date_format(dummy_ts, 'HH:mm') as time_string_24h,     -- '14:30'
        date_format(dummy_ts, 'hh:mm a') as time_string_12h,   -- '02:30 PM'
        
        case 
            when hour_of_day >= 6 and hour_of_day < 12 then 'Morning'
            when hour_of_day >= 12 and hour_of_day < 16 then 'Afternoon'
            when hour_of_day >= 16 and hour_of_day < 20 then 'Evening'
            else 'Night'
        end as time_period,
        
        case 
            when hour_of_day >= 6 and hour_of_day < 12 then 1
            when hour_of_day >= 12 and hour_of_day < 16 then 2
            when hour_of_day >= 16 and hour_of_day < 20 then 3
            else 4
        end as time_period_sort_key, -- BI 工具根据此字段对 time_period 进行正确排序
        
        case 
            when hour_of_day in (7, 8, 9) then 'Morning Rush'
            when hour_of_day in (16, 17, 18, 19) then 'Evening Rush'
            else 'Off-Peak'
        end as peak_period_type

    from time_spine 
),

unknown_member as (
    select 
        -1 as time_id,
        
        -1 as hour_24,
        -1 as hour_12,
        -1 as minute_of_hour,
        'Unknown' as am_pm,
        
        'Unknown' as time_string_24h,
        'Unknown' as time_string_12h,
        
        'Unknown' as time_period,
        -1 as time_period_sort_key,
        'Unknown' as peak_period_type
)

select * from calculated_times
union all
select * from unknown_member 

