
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  SELECT *
FROM GEOPULSE_DB.RAW.GPS_PINGS
WHERE "device_id" IS NULL
   OR "lat" IS NULL
   OR "lon" IS NULL
   OR "timestamp" IS NULL
  
  
      
    ) dbt_internal_test