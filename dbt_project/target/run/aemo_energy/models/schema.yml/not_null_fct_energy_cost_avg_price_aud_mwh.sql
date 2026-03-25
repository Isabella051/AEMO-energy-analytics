
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select avg_price_aud_mwh
from "dev"."main_marts"."fct_energy_cost"
where avg_price_aud_mwh is null



  
  
      
    ) dbt_internal_test