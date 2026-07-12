-- models/marts/core/int_nyc_taxi__yellow_trips_cleaned.sql

{{ 
    config(
        materialized='incremental',
        unique_key='trip_id',
        meta={
            'zorder': 'dq_error_type'
        },
        tags=['audit', 'dq']
    ) 
}}

with staging_trips as (
    select * from {{ ref('stg_nyc_taxi__yellow_trips') }}
)

select
    -- 1. 基础字段透传
    trip_id,
    taxi_type, 
    case 
        when vendor_id in ('1', '2', '6', 'CMT', 'VTS', 'DDS') then vendor_id
        else '99' -- 修正：将 8, 33, 99, 128 等所有非法值统一归化为 -1 (未知成员)
    end as vendor_id,
    pickup_location_id,
    dropoff_location_id,
    pickup_at,
    dropoff_at,
    pickup_at_utc, 
    dropoff_at_utc, 
    passenger_count,
    trip_distance_miles,
    trip_duration_minutes,
    
    -- ==========================================
    -- 2. Clean A: Dictionary Normalization
    -- ==========================================
    case 
        when rate_code_id in (1, 2, 3, 4, 5, 6) then rate_code_id
        else -1 -- 修正：将 8, 33, 99, 128 等所有非法值统一归化为 -1 (未知成员)
    end as rate_code_id,
    
    payment_type,
    has_store_and_fwd,

    -- ==========================================
    -- 3. Clean B: Financial Soft Correction
    -- ==========================================
    fare_amount,
    surcharge_amount,
    mta_tax_amount,
    
    -- 现金支付不记录小费，强制归零修正
    case 
        when payment_type = 2 then cast(0.00 as decimal(9,2))
        else tip_amount
    end as tip_amount,
    
    tolls_amount,
    improvement_surcharge_amount,
    congestion_surcharge_amount,
    airport_fee_amount,
    cbd_congestion_fee_amount,
    total_amount,

    -- 空间与退化维度透传
    pickup_h3_index,
    dropoff_h3_index,
    is_pickup_fallback,
    is_dropoff_fallback,
    efficiency_score,
    partition_year_month,  

    -- ==========================================
    -- 4. Clean C: Soft Flagging (配置驱动设计)
    -- ==========================================
    -- 动态调用业务校验规则，彻底解耦硬编码
    case 
        when {{ get_dq_rule('mta_tax_validation') }} then true
        else false
    end as is_valid_financial_logic, 

    -- ==========================================
    -- 5. Audit & Lineage
    -- ==========================================
    meta_bronze_run_id, 
    meta_silver_run_id, 
    meta_input_file_name, 
    meta_bronze_load_at, 
    meta_silver_processed_at, 
    meta_dbt_staging_invocation_id, 
    meta_staging_processed_at, 
    
    -- 添加当前层的运行元数据
    '{{ invocation_id }}' as meta_dbt_int_invocation_id,
    {{ current_timestamp() }} as meta_int_processed_at

from staging_trips