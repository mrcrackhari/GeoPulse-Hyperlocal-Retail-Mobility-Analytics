-- =====================================================================
-- GeoPulse: Snowflake DDL Setup Script (Idempotent)
-- Owner: Member 3 (BI / Frontend Engineer)
-- =====================================================================

-- 1. Create Warehouse and Database
CREATE WAREHOUSE IF NOT EXISTS GEOPULSE_WH 
  WAREHOUSE_SIZE = 'XSMALL' 
  AUTO_SUSPEND = 60 
  AUTO_RESUME = TRUE;

CREATE DATABASE IF NOT EXISTS GEOPULSE_DB;
USE DATABASE GEOPULSE_DB;

-- 2. Create Schemas
CREATE SCHEMA IF NOT EXISTS RAW;
CREATE SCHEMA IF NOT EXISTS STAGING;
CREATE SCHEMA IF NOT EXISTS MARTS;

USE SCHEMA RAW;

-- 3. Create RAW Tables with GEOGRAPHY types
CREATE TABLE IF NOT EXISTS RAW_GPS_PINGS (
    device_id VARCHAR,
    lat FLOAT,
    lon FLOAT,
    ts TIMESTAMP_NTZ,
    geog GEOGRAPHY
);

CREATE TABLE IF NOT EXISTS RAW_STORES (
    store_id VARCHAR,
    store_name VARCHAR,
    center_lat FLOAT,
    center_lon FLOAT,
    radius_m INT,
    catchment_geog GEOGRAPHY
);

-- 4. Create Internal Stage for Data Loading
CREATE STAGE IF NOT EXISTS geopulse_stage
  FILE_FORMAT = (TYPE = PARQUET);