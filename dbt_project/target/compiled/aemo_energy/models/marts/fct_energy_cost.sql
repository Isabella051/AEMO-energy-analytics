-- models/marts/fct_energy_cost.sql
-- Gold layer: daily energy cost fact table, ready for Power BI

with staging as (

    select * from "dev"."main_staging"."stg_price_and_demand"

),

daily_agg as (

    select
        region_id,
        settlement_date,
        settlement_month,
        time_of_use_band,

        -- Price metrics (AUD/MWh)
        round(avg(rrp_aud_mwh), 2)                          as avg_price_aud_mwh,
        round(min(rrp_aud_mwh), 2)                          as min_price_aud_mwh,
        round(max(rrp_aud_mwh), 2)                          as max_price_aud_mwh,
        round(median(rrp_aud_mwh), 2)                       as median_price_aud_mwh,

        -- Demand metrics (MW)
        round(avg(total_demand_mw), 2)                      as avg_demand_mw,
        round(max(total_demand_mw), 2)                      as peak_demand_mw,

        -- Spike analysis
        count(case when is_price_spike then 1 end)          as spike_intervals,
        count(*)                                            as total_intervals,

        -- Estimated wholesale market cost (AUD)
        -- 5-min intervals: MWh = MW × (5/60)
        round(sum(rrp_aud_mwh * total_demand_mw * 5/60), 0) as est_market_cost_aud

    from staging
    group by 1, 2, 3, 4

),

with_derived as (

    select
        *,
        -- Spike ratio %
        round(spike_intervals * 100.0 / nullif(total_intervals, 0), 2) as spike_pct,

        -- Day-over-day price change
        round(
            avg_price_aud_mwh - lag(avg_price_aud_mwh) over (
                partition by region_id, time_of_use_band
                order by settlement_date
            ), 2
        )                                                   as price_change_dod

    from daily_agg

)

select * from with_derived