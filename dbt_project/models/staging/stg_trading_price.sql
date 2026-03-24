-- models/staging/stg_trading_price.sql
-- Silver layer: clean 30-min trading (settlement) prices

with source as (

    select * from {{ source('bronze', 'wholesale_price') }}

),

renamed as (

    select
        "REGIONID"                              as region_id,
        cast("SETTLEMENTDATE" as timestamp)     as settlement_ts,
        date_trunc('day',
            cast("SETTLEMENTDATE" as timestamp))    as settlement_date,
        date_trunc('month',
            cast("SETTLEMENTDATE" as timestamp))    as settlement_month,

        -- Classify peak / off-peak / shoulder by hour
        case
            when extract(hour from cast("SETTLEMENTDATE" as timestamp))
                 between 7 and 21 then 'peak'
            else 'off_peak'
        end                                     as time_of_use_band,

        cast("RRP" as double)                   as rrp_aud_mwh,
        cast("TOTALDEMAND" as double)           as total_demand_mw,
        cast("PERIODTYPE" as varchar)           as period_type,

        "_ingested_at"                          as ingested_at

    from source
    where "RRP" is not null

)

select * from renamed
