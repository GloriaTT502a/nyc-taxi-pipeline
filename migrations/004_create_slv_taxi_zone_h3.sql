-- ==============================================================================
-- Migration Script: 004_create_slv_taxi_zone_h3.sql
-- Description: Initialize NYC Taxi silver taxi zone h3 Schema Delta table
-- 
-- ⚠️ IMPORTANT: 
-- This script depends on the active Session Catalog context.
-- DO NOT hardcode the catalog here. 
-- If running manually in SQL Editor, ensure you run `USE CATALOG <your_catalog>;` first.
-- ==============================================================================


-- 1. Make sure schema exists
CREATE SCHEMA IF NOT EXISTS process_silver; 

CREATE TABLE IF NOT EXISTS process_silver.slv_taxi_zone_h3 (
  LocationID BIGINT COMMENT 'Unique identifier for the location',
  borough STRING COMMENT 'Borough name',
  zone STRING COMMENT 'Zone name',
  raw_boundary_wkt STRING COMMENT 'Well-Known Text representation of the zone boundary',
  h3_cell STRING COMMENT 'H3 geospatial index string at the specified resolution'
)
USING DELTA
CLUSTER BY (LocationID); 
