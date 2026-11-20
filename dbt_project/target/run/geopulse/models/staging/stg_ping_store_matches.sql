
  create or replace   view GEOPULSE_DB.DBT_STAGING.stg_ping_store_matches
  
  
  
  
  as (
    

SELECT
    "device_id" as device_id,
    "store_id" as store_id,
    "timestamp" as ping_time,
    "lat" as lat,
    "lon" as lon
FROM GEOPULSE_DB.RAW.PING_STORE_MATCHES
  );

