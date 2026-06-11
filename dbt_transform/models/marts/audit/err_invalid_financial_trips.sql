{{ 
    config(
        materialized='incremental',
        unique_key='trip_id', 
        tags=['audit', 'dq']
    ) 
}}

with int_trips as (
    select * from {{ ref('int_nyc_taxi__yellow_trips_cleaned') }}
)

select
    trip_id,
    pickup_at,
    vendor_id,
    fare_amount,
    mta_tax_amount,
    payment_type,
    
    -- 🌟 工业界规范：显式标记死亡原因 (Error Reason)
    'INVALID_FINANCIAL_LOGIC' as dq_error_type,
    'mta_tax_amount or payment_type logic violation' as dq_error_message,
    
    -- 携带审计字段，方便追责
    meta_bronze_run_id,
    meta_int_processed_at
from int_trips
-- 拦截掉进不了 Fact 表的脏数据
where is_valid_financial_logic = false