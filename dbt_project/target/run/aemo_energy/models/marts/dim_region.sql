
  
    
    

    create  table
      "dev"."main_marts"."dim_region__dbt_tmp"
  
    as (
      -- models/marts/dim_region.sql
-- Gold layer: region dimension table with business context

with regions as (

    select * from (
        values
            ('NSW1', 'New South Wales', 'East Coast', 'NEM'),
            ('VIC1', 'Victoria',        'East Coast', 'NEM'),
            ('QLD1', 'Queensland',      'East Coast', 'NEM'),
            ('SA1',  'South Australia', 'East Coast', 'NEM'),
            ('TAS1', 'Tasmania',        'East Coast', 'NEM')
    ) as t(region_id, region_name, interconnect_zone, market)

)

select * from regions
    );
  
  