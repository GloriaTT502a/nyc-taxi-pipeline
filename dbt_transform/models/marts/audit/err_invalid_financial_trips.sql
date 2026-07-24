-- Databricks 推荐：对于审计日志型数据，按月分区可大幅提升后续查询性能
-- partition_by={'field': 'partition_year_month', 'data_type': 'int'}

{{ 
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite', 
        cluster_by=['partition_year_month'],  
        tags=['audit', 'dq']
        
    ) 
}}

with int_trips as (
    
    select * from {{ ref('int_nyc_taxi__yellow_trips_cleaned') }}
    {% if is_incremental() and var('target_month', none) is not none %}
        where partition_year_month = {{ var('target_month') }}
    {% endif %}
)

select
    trip_id,
    partition_year_month, -- 建议透传此字段，方便审计表做时间分区
    pickup_at,
    vendor_id,
    fare_amount,
    mta_tax_amount,
    payment_type,
    
    'INVALID_FINANCIAL_LOGIC' as dq_error_type,
    'mta_tax_amount or payment_type logic violation' as dq_error_message,
    
    -- 携带审计字段，方便追溯源头
    meta_bronze_run_id,
    meta_int_processed_at
from int_trips
-- 拦截掉进不了 Fact 表的脏数据
where is_valid_financial_logic = false 

{% if is_incremental() %}
    and meta_int_processed_at > (select max(meta_int_processed_at) from {{ this }})
{% endif %}