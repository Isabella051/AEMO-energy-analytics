-- models/staging/stg_price_and_demand.sql
-- Silver layer: clean and standardise AEMO price & demand data from Bronze

with source as (

    select * from read_parquet('/Users/isabella/Downloads/aemo-energy-analytics/data/bronze/price_and_demand/*.parquet')

),

cleaned as (

    select
        "REGION"                                            as region_id,
        strptime("SETTLEMENTDATE", '%Y/%m/%d %H:%M:%S')    as settlement_ts,
        date_trunc('day',
            strptime("SETTLEMENTDATE", '%Y/%m/%d %H:%M:%S'))  as settlement_date,
        date_trunc('month',
            strptime("SETTLEMENTDATE", '%Y/%m/%d %H:%M:%S'))  as settlement_month,
        extract('hour' from
            strptime("SETTLEMENTDATE", '%Y/%m/%d %H:%M:%S'))  as hour_of_day,
        case
            when extract('hour' from
                strptime("SETTLEMENTDATE", '%Y/%m/%d %H:%M:%S'))
                between 7 and 21
            then 'peak'
            else 'off_peak'
        end                                                 as time_of_use_band,
        "RRP"                                               as rrp_aud_mwh,
        "TOTALDEMAND"                                       as total_demand_mw,
        "PERIODTYPE"                                        as period_type,
        case when "RRP" > 300 then true else false end      as is_price_spike,
        "_ingested_at"                                      as ingested_at

    from source
    where "RRP" is not null
      and "TOTALDEMAND" is not null

)

select * from cleaned