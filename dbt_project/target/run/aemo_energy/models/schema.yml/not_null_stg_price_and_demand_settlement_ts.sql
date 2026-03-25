
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select settlement_ts
from "dev"."main_staging"."stg_price_and_demand"
where settlement_ts is null



  
  
      
    ) dbt_internal_test