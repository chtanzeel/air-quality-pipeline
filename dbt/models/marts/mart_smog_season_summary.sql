-- Cities compared across smog seasons (1 Oct - 31 Jan, local time).
with hourly as (
    select * from {{ ref('stg_air_quality_hourly') }}
    where smog_season is not null
),

daily as (
    select * from {{ ref('fct_air_quality_daily') }}
    where smog_season is not null
),

data_through as (
    select max(date_pkt) as last_day from {{ ref('stg_air_quality_hourly') }}
),

season_hours as (
    select
        city_id,
        smog_season,
        count(*) as hours_present,
        round(avg(pm2_5_ug_m3)::numeric, 1) as mean_pm2_5_ug_m3,
        count(*) filter (where us_aqi > 150) as hours_unhealthy_or_worse,
        count(*) filter (where us_aqi > 300) as hours_hazardous
    from hourly
    group by 1, 2
),

season_days as (
    select
        city_id,
        smog_season,
        count(*) as days_present,
        count(*) filter (where is_complete) as complete_days,
        -- WHO 2021 24-hour PM2.5 guideline: 15 µg/m³.
        count(*) filter (where is_complete and mean_pm2_5_ug_m3 > 15) as days_above_who_24h,
        max(mean_pm2_5_ug_m3) filter (where is_complete) as worst_day_mean_pm2_5_ug_m3
    from daily
    group by 1, 2
),

worst_day as (
    select distinct on (city_id, smog_season)
        city_id, smog_season, day as worst_day
    from daily
    where is_complete
    order by city_id, smog_season, mean_pm2_5_ug_m3 desc, day
),

joined as (
    select
        h.city_id,
        h.smog_season,
        make_date(left(h.smog_season, 4)::int, 10, 1) as season_start,
        make_date(left(h.smog_season, 4)::int + 1, 1, 31) as season_end,
        h.hours_present,
        d.days_present,
        d.complete_days,
        h.mean_pm2_5_ug_m3,
        d.worst_day_mean_pm2_5_ug_m3,
        w.worst_day,
        d.days_above_who_24h,
        h.hours_unhealthy_or_worse,
        h.hours_hazardous
    from season_hours h
    join season_days d using (city_id, smog_season)
    left join worst_day w using (city_id, smog_season)
)

select
    j.*,
    -- 123 days x 24 hours in a full season.
    round(100.0 * j.hours_present / ((j.season_end - j.season_start + 1) * 24), 1)
        as coverage_pct,
    j.season_end > t.last_day as is_partial,
    rank() over (partition by j.smog_season order by j.mean_pm2_5_ug_m3 desc)
        as rank_most_polluted,
    lag(j.mean_pm2_5_ug_m3) over (partition by j.city_id order by j.smog_season)
        as prev_season_mean_pm2_5_ug_m3,
    -- Not computed for a partial season: a few weeks of October against a full season would
    -- mostly measure the calendar, not the air.
    case when j.season_end <= t.last_day then round(
        100.0 * (j.mean_pm2_5_ug_m3
            - lag(j.mean_pm2_5_ug_m3) over (partition by j.city_id order by j.smog_season))
        / nullif(lag(j.mean_pm2_5_ug_m3) over (partition by j.city_id order by j.smog_season), 0),
        1
    ) end as pct_change_vs_prev_season
from joined j
cross join data_through t
