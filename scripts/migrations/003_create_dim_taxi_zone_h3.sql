-- ==============================================================================
-- Migration Script: 003_create_dim_taxi_zone_h3.sql
-- Description: Initialize NYC Taxi Dim taxi zone h3 Schema Delta table
-- 
-- ==============================================================================

-- 1. Make sure schema exists
CREATE SCHEMA IF NOT EXISTS process_silver; 

CREATE TABLE IF NOT EXISTS dim_taxi_zone_h3 (
    LocationID BIGINT COMMENT 'Taxi zone location ID',
    borough STRING COMMENT 'Borough name (e.g., Manhattan, Queens)',
    zone STRING COMMENT 'Taxi zone name',
    centroid_lat DOUBLE COMMENT 'Latitude of the zone centroid',
    centroid_lng DOUBLE COMMENT 'Longitude of the zone centroid',
    h3_cell STRING COMMENT 'H3 resolution 8 index for the centroid'
)
USING DELTA
COMMENT 'Taxi zone spatial dimension table with H3 index';