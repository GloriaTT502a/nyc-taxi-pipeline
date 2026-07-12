-- ==============================================================================
-- Migration Script: 002_create_silver_tables
-- Description: Initialize NYC Taxi Silver Schema Delta Table (Strict Contract Version)
-- Architecture: Medallion (Silver Enriched)
-- Target Table: process_silver.slv_yellow_nyc_taxi
-- ==============================================================================

-- 1. Ensure the schema exists
CREATE SCHEMA IF NOT EXISTS process_silver;

-- 2. Create the Enriched Silver Table
CREATE TABLE IF NOT EXISTS process_silver.slv_yellow_nyc_taxi (
    
    -- ==========================================
    -- 8. Partition Keys
    -- ==========================================
    YYYY INT COMMENT 'Year partition derived from physical time',
    YYYYMM INT NOT NULL COMMENT 'Year-Month partition key (e.g., 202605)',

    -- ==========================================
    -- 1. Unique Key and Spatial Index
    -- ==========================================
    trip_key STRING NOT NULL COMMENT 'SHA-256 unique key generated based on vendor, time, location, and amount',
    h3_pickup STRING COMMENT 'H3 spatial grid index for pickup location (Resolution 8)',
    h3_dropoff STRING COMMENT 'H3 spatial grid index for dropoff location (Resolution 8)',

    -- ==========================================
    -- 2. Core Business Fields
    -- ==========================================
    vendor_id STRING COMMENT 'TPEP provider identification code',
    rate_code BIGINT COMMENT 'Final rate code in effect at the end of the trip (e.g., 1=Standard, 2=JFK)',
    store_and_fwd_flag STRING COMMENT 'Indicates whether the trip record was held in vehicle memory before sending (Y/N)',
    payment_type STRING COMMENT 'How the passenger paid for the trip (e.g., Credit Card, Cash)',
    passenger_count BIGINT COMMENT 'The number of passengers in the vehicle',
    trip_distance DOUBLE COMMENT 'The elapsed trip distance in miles reported by the taximeter',
    
    -- ==========================================
    -- 3. Dual-Track Time Integration
    -- ==========================================
    pickup_datetime TIMESTAMP COMMENT 'Pickup time (Local - Preserved for business calendar analytics)',
    dropoff_datetime TIMESTAMP COMMENT 'Dropoff time (Local - Preserved for business calendar analytics)',
    pickup_datetime_utc TIMESTAMP NOT NULL COMMENT 'Pickup time (UTC - Baseline for cross-system integration and deterministic computation)',
    dropoff_datetime_utc TIMESTAMP NOT NULL COMMENT 'Dropoff time (UTC - Baseline for cross-system integration and deterministic computation)',

    -- ==========================================
    -- 4. Business Derived (Physical Facts)
    -- ==========================================
    duration_min DOUBLE COMMENT 'Physical trip duration in minutes, calculated based on UTC to avoid DST anomalies',
    fare_per_minute DOUBLE COMMENT 'Revenue efficiency per minute (fare_amount / duration_min)',

    -- ==========================================
    -- 5. Financial Detail Columns
    -- ==========================================
    fare_amount DOUBLE COMMENT 'The time-and-distance fare calculated by the meter',
    surcharge DOUBLE COMMENT 'Miscellaneous extra charges (e.g., rush hour or overnight fees)',
    mta_tax DOUBLE COMMENT '0.50 USD MTA tax',
    tip_amount DOUBLE COMMENT 'Tip amount (Automatically populated for credit card tips)',
    tolls_amount DOUBLE COMMENT 'Total amount of all tolls paid in trip',
    improvement_surcharge DOUBLE COMMENT '0.30 USD improvement surcharge',
    congestion_surcharge DOUBLE COMMENT 'Congestion surcharge collected in NY State',
    airport_fee DOUBLE COMMENT 'Airport pickup/dropoff fee (e.g., JFK/LaGuardia)',
    cbd_congestion_fee DOUBLE COMMENT 'Manhattan Central Business District (CBD) toll',
    total_amount DOUBLE NOT NULL COMMENT 'The total amount charged to passengers (sum of all fees)',

    -- ==========================================
    -- 6. Spatial and Location
    -- ==========================================
    PULocationID BIGINT COMMENT 'TLC Taxi Zone in which the taximeter was engaged',
    DOLocationID BIGINT COMMENT 'TLC Taxi Zone in which the taximeter was disengaged',
    pickup_longitude DOUBLE COMMENT 'Pickup location longitude',
    pickup_latitude DOUBLE COMMENT 'Pickup location latitude',
    dropoff_longitude DOUBLE COMMENT 'Dropoff location longitude',
    dropoff_latitude DOUBLE COMMENT 'Dropoff location latitude',

    -- ==========================================
    -- 7. Telemetry for RCA (Root Cause Analysis)
    -- ==========================================
    is_pickup_fallback INT COMMENT 'Flag indicating if H3 pickup calculation fell back to LocationID (1=Yes, 0=No)',
    is_dropoff_fallback INT COMMENT 'Flag indicating if H3 dropoff calculation fell back to LocationID (1=Yes, 0=No)',

    -- ==========================================
    -- 9. Platform Lineage and Audit
    -- ==========================================
    _bronze_run_id STRING COMMENT 'Ingestion batch ID from the upstream Bronze layer',
    _bronze_load_timestamp TIMESTAMP COMMENT 'Original timestamp when data landed in the Bronze layer',
    _input_file STRING COMMENT 'Source Parquet/CSV file name',
    _silver_run_id STRING NOT NULL COMMENT 'Run ID of the current Silver processing pipeline',
    _silver_load_timestamp TIMESTAMP NOT NULL COMMENT 'Absolute timestamp when written to the Silver layer'
    
)
USING DELTA
CLUSTER BY (YYYYMM, h3_pickup)
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',        
    'delta.autoOptimize.optimizeWrite' = 'true',  
    'delta.autoOptimize.autoCompact' = 'true',    
    'delta.columnMapping.mode' = 'name',          
    'comment' = 'NYC Yellow Taxi Silver Table (Cleaned, Conformed & Enriched with Dual-Track Time)'
);