
    
    

with all_values as (

    select
        region_id as value_field,
        count(*) as n_records

    from "dev"."main_staging"."stg_price_and_demand"
    group by region_id

)

select *
from all_values
where value_field not in (
    'NSW1','VIC1','QLD1','SA1','TAS1'
)


