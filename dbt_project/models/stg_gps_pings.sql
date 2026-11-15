{{ config(materialized='view') }}

SELECT
    "device_id",
    "lat",
    "lon",
    "timestamp" as ping_time
FROM {{ source('geopulse_raw', 'GPS_PINGS') }}