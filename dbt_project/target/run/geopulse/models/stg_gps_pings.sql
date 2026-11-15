
  create or replace   view GEOPULSE_DB.DBT_STAGING.stg_gps_pings
  
  
  
  
  as (
    

SELECT
    "device_id",
    "lat",
    "lon",
    "timestamp" as ping_time
FROM GEOPULSE_DB.RAW.GPS_PINGS
  );

