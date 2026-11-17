SELECT *
FROM {{ source('geopulse_raw', 'GPS_PINGS') }}
WHERE "device_id" IS NULL
   OR "lat" IS NULL
   OR "lon" IS NULL
   OR "timestamp" IS NULL