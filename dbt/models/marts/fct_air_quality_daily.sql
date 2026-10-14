-- One row per city and local (PKT) day.
select
    city_id,
    date_pkt as day,
    smog_season,
    count(*) as hours_present,
    count(*) filter (where is_native_step) as native_steps_present,
    count(*) >= {{ var('min_hours_complete_day') }} as is_complete,
    round(avg(pm2_5_ug_m3)::numeric, 1) as mean_pm2_5_ug_m3,
    max(pm2_5_ug_m3) as max_pm2_5_ug_m3,
    round(avg(pm10_ug_m3)::numeric, 1) as mean_pm10_ug_m3,
    max(us_aqi) as max_us_aqi
from {{ ref('stg_air_quality_hourly') }}
group by city_id, date_pkt, smog_season
