{{ config(materialized='view') }}

SELECT
    "device_id" as device_id,
    "store_id" as store_id,
    "timestamp" as ping_time,
    "lat" as lat,
    "lon" as lon
FROM {{ source('geopulse_raw', 'PING_STORE_MATCHES') }}
