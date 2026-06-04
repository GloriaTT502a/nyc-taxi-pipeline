{{ 
    config(
        materialized='view',
        tags=['staging', 'nyc_yellow_taxi'],
        enabled=true
    ) 
}}

-- 策略：将其创建为逻辑视图
-- 标签：方便后续只运行特定标签的模型
-- 开关：设为 false 即可停用该模型


with source as (
select * from {{ source('databricks_ingest', 'yellow_trips') }}
), 
renamed as (
select
        -- ==========================================
        -- 主键与外键 (Primary & Foreign Keys)
        -- ==========================================
        YYYYMM as partition_year_month, 
        'yellow' AS taxi_type,
        cast(trip_key as string) as trip_id,                    -- 规范：主键以 _id 结尾
        cast(vendor_id as string) as vendor_id,
        cast(PULocationID as int) as pickup_location_id,        -- 规范：避免拼音/简写，全拼更清晰
        cast(DOLocationID as int) as dropoff_location_id,
        cast(rate_code as int) as rate_code_id,                 -- 规范：指向维表的外键补充 _id
        -- ==========================================
        -- 时间戳 (Timestamps)
        -- ==========================================
        cast(pickup_datetime as timestamp) as pickup_at,        -- 规范：时间戳以 _at 结尾
        cast(dropoff_datetime as timestamp) as dropoff_at,
        -- ==========================================
        -- 维度与分类标志 (Dimensions & Flags)
        -- ==========================================
        {{ map_payment_type('payment_type') }} as payment_type, 
        payment_type as raw_payment_type, 
        -- 优化：将 'Y'/'N' 的字符串转换为原生 BOOLEAN 类型
        store_and_fwd_flag as raw_store_and_fwd_flag, 
        {{ cast_to_boolean('store_and_fwd_flag') }} as has_store_and_fwd, 
        -- ==========================================
        -- 物理度量指标 (Metrics - Physical)
        -- ==========================================
        cast(passenger_count as int) as passenger_count,
        cast(trip_distance as double) as trip_distance_miles,   -- 规范：数值型指标明确单位
        cast(duration_min as double) as trip_duration_minutes,
        -- ==========================================
        -- 财务金额明细 (Metrics - Financial)
        -- ==========================================
        fare_amount as raw_fare_amount, 
        cast(fare_amount as decimal(9,2)) as fare_amount,
        
        surcharge as raw_surcharge, 
        cast(surcharge as decimal(9,2)) as surcharge_amount,

        mta_tax as raw_mta_tax, 
        cast(mta_tax as decimal(9,2)) as mta_tax_amount,

        tip_amount as raw_tip_amount, 
        cast(tip_amount as decimal(9,2)) as tip_amount,

        tolls_amount as raw_tolls_amount, 
        cast(tolls_amount as decimal(9,2)) as tolls_amount,

        improvement_surcharge as raw_improvement_surcharge, 
        cast(improvement_surcharge as decimal(9,2)) as improvement_surcharge_amount,
        
        congestion_surcharge as raw_congestion_surcharge, 
        cast(congestion_surcharge as decimal(9,2)) as congestion_surcharge_amount,
        
        airport_fee as raw_airport_fee, 
        cast(airport_fee as decimal(9,2)) as airport_fee_amount,
        
        cbd_congestion_fee as raw_cbd_congestion_fee, 
        cast(cbd_congestion_fee as decimal(9,2)) as cbd_congestion_fee_amount,
        
        total_amount as raw_total_amount, 
        cast(total_amount as decimal(9,2)) as total_amount,
        -- ==========================================
        -- 空间索引及数据质量标记 (Spatial & DQ Flags)
        -- ==========================================
        cast(h3_pickup as string) as pickup_h3_index,
        cast(h3_dropoff as string) as dropoff_h3_index,
        -- 优化：将 INT 类型的 0/1 转换为 BOOLEAN 类型
        cast(is_pickup_fallback = 1 as boolean) as is_pickup_fallback,
        cast(is_dropoff_fallback = 1 as boolean) as is_dropoff_fallback,
        -- ==========================================
        -- 特征项 (Features)
        -- ==========================================
        -- 优化：彻底消除临时命名，赋予确切的业务含义 
        cast(temp_eff as double) as efficiency_score, 
        cast(bronze_run_id as string)           as meta_bronze_run_id,
        cast(_run_id as string)                 as meta_silver_run_id,
        cast(_input_file as string)             as meta_input_file_name,
    
        -- 时间戳统一使用 _at 后缀，并显式 CAST 保证类型安全
        cast(bronze_load_timestamp as TIMESTAMP) as meta_bronze_load_at,
        cast(_processed_at as TIMESTAMP)         as meta_silver_processed_at, 
        
        -- dbt data lineage 
        '{{ invocation_id }}' as meta_dbt_staging_invocation_id,
        {{ current_timestamp() }} as meta_staging_processed_at 
from source t 
)
select * from renamed
