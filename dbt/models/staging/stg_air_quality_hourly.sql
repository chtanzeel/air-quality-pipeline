-- Clean hourly rows: everything that passed the quarantine checks.
select
    city_id,
    measured_at,
    measured_at_pkt,
    date_pkt,
    hour_utc,
    is_native_step,
    smog_season,
    pm2_5_ug_m3,
    pm10_ug_m3,
    no2_ug_m3,
    so2_ug_m3,
    o3_ug_m3,
    co_ug_m3,
    dust_ug_m3,
    us_aqi,
    grid_latitude,
    grid_longitude,
    _source_file,
    _loaded_at,
    _batch_id
from {{ ref('stg_air_quality_hourly_all') }}
where quarantine_reason is null
