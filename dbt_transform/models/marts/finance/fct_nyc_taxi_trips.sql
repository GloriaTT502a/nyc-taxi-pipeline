{{ config(
    materialized='incremental',
    unique_key='trip_id',
    partition_by={'field': 'pickup_date', 'data_type': 'date'}
) }}

with int_trips as (
    select * from {{ ref('int_nyc_taxi__yellow_trips_cleaned') }}
    {% if is_incremental() %}
    -- 增量逻辑：只处理上一批次之后处理过的数据
    where meta_int_processed_at > (select max(meta_int_processed_at) from {{ this }})
    {% endif %}
)

select
    -- ==========================================
    -- 1. 业务主键
    -- ==========================================
    trip_id,

    -- ==========================================
    -- 2. 维度外键 (Foreign Keys，严格遵循 _id 后缀规范)
    -- ==========================================
    -- 物理日期外键 (将 UTC 时间截断至天)
    cast(date_trunc('day', pickup_at_utc) as date) as pickup_date_id,
    cast(date_trunc('day', dropoff_at_utc) as date) as dropoff_date_id,
    
    -- 空间维度外键 (H3 索引本身就是极其优秀的主键)
    pickup_h3_index as pickup_h3_id,
    dropoff_h3_index as dropoff_h3_id,
    
    -- 业务维度外键
    payment_type as payment_type_id,
    rate_code_id,
    vendor_id,

    -- ==========================================
    -- 3. 财务度量值 (Financial Measures)
    -- ==========================================
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

    -- ==========================================
    -- 4. 运营度量值 (Operational Measures)
    -- ==========================================
    passenger_count,
    trip_distance_miles,
    trip_duration_minutes,
    efficiency_score

from int_trips
-- 核心过滤：事实表只允许存在财务逻辑干净、合法的数据
where is_valid_financial_logic = true