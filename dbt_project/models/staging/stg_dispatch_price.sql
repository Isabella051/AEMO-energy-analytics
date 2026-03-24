-- models/staging/stg_dispatch_price.sql
-- Silver layer: clean and standardise 5-min dispatch prices from Bronze

with source as (

    select * from {{ source('bronze', 'dispatch_price') }}

),

renamed as (

    select
        -- Keys
        "REGIONID"                          as region_id,
        "INTERVENTION"                      as intervention_flag,

        -- Timestamps
        cast("SETTLEMENTDATE" as timestamp) as settlement_ts,
        date_trunc('day',
            cast("SETTLEMENTDATE" as timestamp))  as settlement_date,
        date_trunc('month',
            cast("SETTLEMENTDATE" as timestamp))  as settlement_month,
        extract(hour from
            cast("SETTLEMENTDATE" as timestamp))  as hour_of_day,

        -- Price (AUD/MWh)
        cast("RRP" as double)               as rrp_aud_mwh,

        -- Demand (MW)
        cast("TOTALDEMAND" as double)       as total_demand_mw,

        -- Metadata
        "_ingested_at"                      as ingested_at

    from source
    where "RRP" is not null

)

select * from renamed
