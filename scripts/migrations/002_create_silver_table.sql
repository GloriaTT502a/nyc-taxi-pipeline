-- ==============================================================================
-- Migration Script: 002_create_silver_tables
-- Description: Initialize NYC Taxi Silver Schema Delta 表
-- 
-- ==============================================================================

-- 1. Make sure schema exists
CREATE SCHEMA IF NOT EXISTS process_silver;

CREATE TABLE IF NOT EXISTS process_silver.silver_yellow_taxi (
    -- 1. Key and Index 
    trip_key STRING NOT NULL COMMENT '基于 vendor, time, loc, amount 生成的 SHA-256 唯一键',
    h3_pickup STRING COMMENT '上车点 H3 索引 (Res 8)',
    h3_dropoff STRING COMMENT '下车点 H3 索引 (Res 8)',

    -- 2. Core Business Columns
    vendor_id STRING,
    pickup_datetime TIMESTAMP,
    dropoff_datetime TIMESTAMP,
    passenger_count BIGINT,
    trip_distance DOUBLE,
    
    -- 3. Financial Detail Columns (Used to calculate total_amount)
    fare_amount DOUBLE,
    surcharge DOUBLE,
    mta_tax DOUBLE,
    tip_amount DOUBLE,
    tolls_amount DOUBLE,
    improvement_surcharge DOUBLE,
    
    -- 4. Financial Summary Columns 
    total_amount DOUBLE NOT NULL COMMENT '财务总额: fare + surcharge + tax + tip + tolls + improvement',

    -- 5. Spatical and Location 
    PULocationID BIGINT,
    DOLocationID BIGINT,
    pickup_longitude DOUBLE,
    pickup_latitude DOUBLE,
    dropoff_longitude DOUBLE,
    dropoff_latitude DOUBLE,

    -- Partition Key
    YYYY INT,
    YYYYMM INT,

    -- 6. Audit Columns
    _load_timestamp TIMESTAMP COMMENT '原始数据加载时间',
    _silver_run_id STRING COMMENT 'Silver层批次ID',
    _silver_processed_at TIMESTAMP COMMENT '处理完成时间'
    
)
USING DELTA
PARTITIONED BY (YYYYMM)
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true', -- 开启 CDF 方便下游 Gold 层增量同步
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true', 
    'comment' = 'NYC Yellow Taxi Silver Table (clean Data)'
);
