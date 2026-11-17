
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select lat
from GEOPULSE_DB.RAW.GPS_PINGS
where lat is null



  
  
      
    ) dbt_internal_test