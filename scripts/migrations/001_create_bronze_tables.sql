-- ==============================================================================
-- Migration Script: 001_create_bronze_tables
-- Description: Initialize NYC Taxi Bronze Schema Delta 表
-- 
-- ==============================================================================

-- 1. Make sure schema exists
CREATE SCHEMA IF NOT EXISTS process_bronze;

-- 2. Create Bronze Delta Table
CREATE TABLE IF NOT EXISTS process_bronze.brz_yellow_nyc_taxi (
    vendor_id STRING,
    pickup_datetime TIMESTAMP,
    dropoff_datetime TIMESTAMP,
    passenger_count BIGINT,
    trip_distance DOUBLE,
    rate_code BIGINT,
    store_and_fwd_flag STRING,
    pickup_longitude DOUBLE,
    pickup_latitude DOUBLE,
    dropoff_longitude DOUBLE,
    dropoff_latitude DOUBLE,
    PULocationID BIGINT,
    DOLocationID BIGINT,
    payment_type STRING,
    fare_amount DOUBLE,
    surcharge DOUBLE,
    mta_tax DOUBLE,
    tip_amount DOUBLE,
    tolls_amount DOUBLE,
    improvement_surcharge DOUBLE,
    congestion_surcharge DOUBLE,
    airport_fee DOUBLE,
    cbd_congestion_fee DOUBLE,
    total_amount DOUBLE,
    
    -- Partition Key
    YYYY INT,
    YYYYMM INT, 
    
    -- Meta Data (Lineage)
    _run_id STRING, 
    _load_timestamp TIMESTAMP,  
    _input_file STRING 
)
USING DELTA
PARTITIONED BY (YYYYMM)
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'comment' = 'NYC Yellow Taxi Bronze Table (Raw Data)'
);
