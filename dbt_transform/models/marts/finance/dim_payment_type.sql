{{ 
    config(
        materialized='table',
        tags=['dim', 'core']
    ) 
}}

with source_data as (
    select * from {{ ref('seed_payment_mapping') }}
),

-- 1. 去重：从多对一的映射表中，提取出唯一的维度列表
unique_payments as (
    select distinct 
        payment_code,
        standard_payment_type
    from source_data
)

select
    -- 2. 重命名并对齐数据类型 (我们在中间层已将 payment_type 设为 String)
    cast(payment_code as string) as payment_type_id, 
    
    -- 3. 规范化业务描述字段
    standard_payment_type as payment_type_name,                
    
    -- 4. 派生维度标签：方便 BI 报表直接通过 true/false 做切片 (2 代表 Cash)
    case 
        when payment_code = 2 then true 
        else false 
    end as is_cash_payment                                  

from unique_payments

-- 保证最终输出的维度表没有任何空值主键
where payment_code is not null
