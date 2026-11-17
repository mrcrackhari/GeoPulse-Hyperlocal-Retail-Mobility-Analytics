# GeoPulse Database Schema Design

## Overview
This document defines the Snowflake schema and column-level architecture for the GeoPulse project, using native `GEOGRAPHY` data types[cite: 2].

---

## 1. RAW Schema Tables

### `RAW.GPS_PINGS`
Stores raw synthetic GPS pings generated for mobility tracking.
* `device_id` (VARCHAR) - Unique identifier for the mobile device/user.
* `lat` (FLOAT) - Latitude coordinate.
* `lon` (FLOAT) - Longitude coordinate.
* `ts` (TIMESTAMP_NTZ) - Local timestamp of the GPS ping[cite: 8].
* `geog` (GEOGRAPHY) - Point geography object generated using `ST_MakePoint(lon, lat)`[cite: 8].

### `RAW.STORES`
Stores store location and catchment polygon metadata.
* `store_id` (VARCHAR) - Unique identifier for the retail store.
* `store_name` (VARCHAR) - Name of the retail store.
* `center_lat` (FLOAT) - Store center latitude.
* `center_lon` (FLOAT) - Store center longitude.
* `radius_m` (INT) - Catchment radius in meters (500m)[cite: 8].
* `catchment_geog` (GEOGRAPHY) - Polygon geography object representing the 500m catchment area[cite: 2].

---

## 2. Marts & Analytics Models

### `FOOTFALL_HOURLY`
Aggregated hourly visitor traffic per store.
* `store_id` (VARCHAR) - Retail store identifier.
* `hour_ts` (TIMESTAMP_NTZ) - Hourly timestamp bucket[cite: 8].
* `hour_of_day` (INT) - Hour of the day (0-23)[cite: 8].
* `is_weekend` (BOOLEAN) - Flag indicating Saturday or Sunday[cite: 8].
* `unique_visitors` (INT) - Count of unique devices visiting during the hour[cite: 8].
* `total_pings` (INT) - Total GPS pings recorded in the catchment[cite: 8].

### `STORE_CANNIBALIZATION_METRICS`
Tracks customer overlap and store cannibalization between store pairs.
* `store_id` (VARCHAR) - Focal store ID.
* `other_store_id` (VARCHAR) - Comparison store ID.
* `shared_visitors` (INT) - Number of unique devices visiting both stores[cite: 8].
* `pct_store_visitors_also_visit_other` (FLOAT) - Percentage overlap ratio[cite: 8].