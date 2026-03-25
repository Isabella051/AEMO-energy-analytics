
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select region_id
from "dev"."main_marts"."fct_energy_cost"
where region_id is null



  
  
      
    ) dbt_internal_test