
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

select
    region_id as unique_field,
    count(*) as n_records

from "dev"."main_marts"."dim_region"
where region_id is not null
group by region_id
having count(*) > 1



  
  
      
    ) dbt_internal_test