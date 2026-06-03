{{ 
    config(
        tags=['intermediate', 'nyc_yellow_taxi'],
        enabled=true
    ) 
}}

with staging_trips as (
    select * from {{ ref('stg_nyc_taxi__yellow_trips') }}
)

select
    -- 1. 基础字段透传
    trip_id,
    vendor_id,
    pickup_location_id,
    dropoff_location_id,
    pickup_at,
    dropoff_at,
    passenger_count,
    trip_distance_miles,
    trip_duration_minutes,
    
    -- ==========================================
    -- 2. 业务清洗 A：数据字典归化 (Dictionary Normalization)
    -- ==========================================
    -- 修复 rate_code_id 的野值 (PySpark 没有做这件事)
    case 
        when rate_code_id in (1, 2, 3, 4, 5, 6) then rate_code_id
        else null -- 将 8, 33, 99, 128 等所有非法值统一归化为 NULL
    end as rate_code_id,
    
    -- payment_type 已经在 staging 层用宏/seed 转成了 1-6 的标准 INT
    payment_type,
    has_store_and_fwd,

    -- ==========================================
    -- 3. 业务清洗 B：财务逻辑智能修正 (Financial Soft Correction)
    -- ==========================================
    fare_amount,
    surcharge_amount,
    mta_tax_amount,
    
    -- 跨列逻辑修正：现金支付不允许有系统小费记录
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

    -- 其他维度透传
    pickup_h3_index,
    dropoff_h3_index,
    is_pickup_fallback,
    is_dropoff_fallback,
    efficiency_score,
    partition_year_month,

    -- ==========================================
    -- 4. 业务清洗 C：软隔离打标 (Soft Flagging)
    -- ==========================================
    -- 对于那些金额不合理的单子，不要像 PySpark 那样直接删掉，而是打上标签供分析
    case 
        -- 比如：MTA 只能是 0 或 0.50，如果出现 31.72 这种乱码，打为 false
        when mta_tax_amount not in (0.00, 0.50) 
             and not (mta_tax_amount = -0.50 and payment_type in (4, 6))
        then false
        
        -- 你可以继续在这里添加那些你不忍心直接在 Spark 里 drop 掉的业务规则
        
        else true
    end as is_valid_financial_logic, 

    -- Audit columns 
    meta_bronze_run_id, 
    meta_silver_run_id, 
    meta_input_file_name, 
    meta_bronze_load_at, 
    meta_silver_processed_at, 
    meta_dbt_staging_invocation_id, 
    meta_staging_processed_at, 
    
    -- Add intermediate layer audit columns 
    '{{ invocation_id }}' as meta_dbt_int_invocation_id,
    {{ current_timestamp() }} as meta_int_processed_at


from staging_trips
