{{ 
    config(
        materialized='table',
        cluster_by=['date_id', 'year_month_num'],
        tags=['marts', 'dimension', 'static']
    ) 
}}

with date_spine as (
    -- 利用 Databricks 的 sequence 函数生成连续日期序列 (0 成本极速生成)
    select explode(sequence(to_date('2009-01-01'), to_date('2030-12-31'), interval 1 day)) as date_actual
)

select
    -- 工业标准主键：YYYYMMDD 格式的整数型 ID
    cast(date_format(date_actual, 'yyyyMMdd') as int) as date_id,
    
    date_actual,
    year(date_actual) as year_num,
    month(date_actual) as month_num,
    
    -- 新增：年月组合，例如 200901，或者 '2009-01'。对齐我们在 stg 层使用的 partition_year_month
    cast(date_format(date_actual, 'yyyyMM') as int) as year_month_num,
    date_format(date_actual, 'yyyy-MM') as year_month_name,
    
    day(date_actual) as day_of_month,
    dayofyear(date_actual) as day_of_year,       
    
    dayofweek(date_actual) as day_of_week_num,   -- 1=Sunday, 2=Monday...
    
    date_format(date_actual, 'E') as day_of_week_short, -- 'Sun', 'Mon'

    date_format(date_actual, 'EEEE') as day_of_week_name,
    
    case when dayofweek(date_actual) in (1, 7) then true else false end as is_weekend,
    
    concat('Q', quarter(date_actual)) as quarter_name

from date_spine 
