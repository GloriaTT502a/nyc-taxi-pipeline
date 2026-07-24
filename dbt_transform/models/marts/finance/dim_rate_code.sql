{{ 

    config(

        materialized='table',

        tags=['dim', 'core', 'static']

    ) 

}}

with source_data as (
    select 
        cast(rate_code_id as int) as rate_code_id,
        cast(trim(rate_code_name) as string) as rate_code_name
    from {{ ref('seed_rate_code_mapping') }}
    where rate_code_id is not null 
      -- 剔除 seed 中可能已经手工配置的 -1，避免后续 UNION 产生重复主键
      and cast(rate_code_id as int) != -1 
),

-- 自动注入未知成员兜底，保证与 Fact 表的 Outer/Inner Join 绝对安全
unknown_member as (
    select 
        -1 as rate_code_id,
        'Unknown / Invalid' as rate_code_name
),

combined_rates as (
    select * from source_data
    union all
    select * from unknown_member
) 

select

    rate_code_id,
    rate_code_name,
    
    -- 派生维度：机场行程标记 (通常 2=JFK, 3=Newark)
    case 
        when rate_code_id in (2, 3) then true
        else false
    end as is_airport_trip,
    
    -- 派生维度：非法/未知代码标记
    case 
        when rate_code_id = -1 then true
        else false
    end as is_invalid_code

from combined_rates 
