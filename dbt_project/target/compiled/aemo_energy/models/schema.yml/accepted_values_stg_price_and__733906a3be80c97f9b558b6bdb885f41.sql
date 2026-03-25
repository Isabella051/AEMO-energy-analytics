
    
    

with all_values as (

    select
        time_of_use_band as value_field,
        count(*) as n_records

    from "dev"."main_staging"."stg_price_and_demand"
    group by time_of_use_band

)

select *
from all_values
where value_field not in (
    'peak','off_peak'
)


