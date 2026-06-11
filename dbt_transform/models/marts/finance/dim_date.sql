{{ 
    config(
        materialized='table',
        tags=['marts', 'dimension']
    ) 
}}
with date_spine as (
    -- 利用 Databricks 的 sequence 函数生成连续日期序列
    select explode(sequence(to_date('2020-01-01'), to_date('2030-12-31'), interval 1 day)) as date_actual
)
select
    -- 🌟 工业标准主键：YYYYMMDD 格式的整数型 ID，比 Date 类型 Join 更快
    cast(date_format(date_actual, 'yyyyMMdd') as int) as date_id,
    
    date_actual,
    year(date_actual) as year_num,
    month(date_actual) as month_num,
    day(date_actual) as day_of_month,
    dayofweek(date_actual) as day_of_week_num, -- 1=Sunday, 2=Monday, etc.
    date_format(date_actual, 'EEEE') as day_of_week_name,
    
    -- 业务 Flag
    case when dayofweek(date_actual) in (1, 7) then true else false end as is_weekend,
    case when month(date_actual) in (1, 2, 3) then 'Q1'
         when month(date_actual) in (4, 5, 6) then 'Q2'
         when month(date_actual) in (7, 8, 9) then 'Q3'
         else 'Q4' end as quarter_name
         
    -- 如果有节假日 seed 表，可以在这里 LEFT JOIN 进来生成 is_us_holiday flag
from date_spine