
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select settlement_date
from "dev"."main_marts"."fct_energy_cost"
where settlement_date is null



  
  
      
    ) dbt_internal_test