
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select rrp_aud_mwh
from "dev"."main_staging"."stg_price_and_demand"
where rrp_aud_mwh is null



  
  
      
    ) dbt_internal_test