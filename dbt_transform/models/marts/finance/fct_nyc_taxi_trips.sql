{% set default_month = run_started_at.strftime("%Y%m") %} 
{% set target_month = var('target_yyyymm', default_month) %}

{{ 
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite', 
        unique_key='trip_id',
        schema='core',
        partition_by=['partition_year_month'], 
        tags=['marts', 'fact', 'core', 'nyc_yellow_taxi']
    ) 
}}

with cleaned_data as (
    select * from {{ ref('int_nyc_taxi__yellow_trips_cleaned') }}
    {% if this is not none %}
        {% if is_incremental() and var('target_month', none) is not none %}
            where partition_year_month = {{ var('target_month') }}
        {% endif %}
    {% endif %}
)

select
    
    partition_year_month, 

    -- 1. 基础字段与主键
    trip_id,
    taxi_type,
    vendor_id,
    pickup_location_id,
    dropoff_location_id,
    
    -- 报表字段 (原始时间戳)
    pickup_at,
    dropoff_at,
    
    -- 转换 ID (用于关联维度表 dim_date / dim_time)
    cast(date_format(pickup_at, 'yyyyMMdd') as int) as pickup_date_id,
    cast(date_format(pickup_at, 'HHmm') as int) as pickup_time_id,
    cast(date_format(dropoff_at, 'yyyyMMdd') as int) as dropoff_date_id,
    cast(date_format(dropoff_at, 'HHmm') as int) as dropoff_time_id,
    
    -- 算法用 UTC 时间
    pickup_at_utc,
    dropoff_at_utc,
    
    passenger_count,
    trip_distance_miles,
    trip_duration_minutes,
    
    -- 2. Dictionary Normalization
    rate_code_id,
    payment_type,
    has_store_and_fwd,

    -- 3. Financial Soft Correction
    fare_amount,
    surcharge_amount,
    mta_tax_amount,
    tip_amount,
    tolls_amount,
    improvement_surcharge_amount,
    congestion_surcharge_amount,
    airport_fee_amount,
    cbd_congestion_fee_amount,
    total_amount,

    -- 空间与退化维度
    pickup_h3_index,
    dropoff_h3_index,
    is_pickup_fallback,
    is_dropoff_fallback,
    efficiency_score,

    -- 4. Clean C: Soft Flagging
    is_valid_financial_logic,

    -- 5. Audit & Lineage
    meta_bronze_run_id,
    meta_silver_run_id,
    meta_input_file_name,
    meta_bronze_load_at,
    meta_silver_processed_at,
    meta_dbt_staging_invocation_id,
    meta_staging_processed_at,
    meta_dbt_int_invocation_id,
    meta_int_processed_at,
    current_timestamp() as meta_dbt_fct_processed_at

from cleaned_data 