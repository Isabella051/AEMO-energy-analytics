-- models/marts/fct_energy_cost.sql
-- Gold layer: daily energy cost fact table, ready for Power BI

with trading as (

    select * from {{ ref('stg_trading_price') }}

),

daily_agg as (

    select
        region_id,
        settlement_date,
        settlement_month,
        time_of_use_band,

        -- Price metrics (AUD/MWh)
        avg(rrp_aud_mwh)                        as avg_price_aud_mwh,
        min(rrp_aud_mwh)                        as min_price_aud_mwh,
        max(rrp_aud_mwh)                        as max_price_aud_mwh,
        percentile_cont(0.5) within group
            (order by rrp_aud_mwh)              as median_price_aud_mwh,

        -- Demand metrics (MW)
        avg(total_demand_mw)                    as avg_demand_mw,
        max(total_demand_mw)                    as peak_demand_mw,

        -- Price spike flag: RRP > $300/MWh is considered a spike
        count(case when rrp_aud_mwh > 300 then 1 end)  as price_spike_intervals,
        count(*)                                        as total_intervals,

        -- Estimated cost for a 1 MW consumer (AUD)
        -- Cost = avg_price × demand × 0.5 hr (30-min intervals)
        sum(rrp_aud_mwh * total_demand_mw * 0.5)       as estimated_market_cost_aud

    from trading
    group by 1, 2, 3, 4

),

with_derived as (

    select
        *,
        -- Price spike ratio for risk scoring
        round(
            price_spike_intervals * 100.0 / nullif(total_intervals, 0),
            2
        )                                               as spike_pct,

        -- Month-over-month price change (window function)
        avg_price_aud_mwh - lag(avg_price_aud_mwh) over (
            partition by region_id, time_of_use_band
            order by settlement_date
        )                                               as price_change_vs_prev_day

    from daily_agg

)

select * from with_derived
