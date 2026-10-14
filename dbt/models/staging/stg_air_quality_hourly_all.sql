{#- Every raw row, typed and renamed, with a quarantine_reason (null when the row is fine).
    stg_air_quality_hourly keeps the clean rows; quarantine_air_quality_hourly keeps the rest. -#}

{%- set measures = {
    'pm2_5': 'pm2_5_ug_m3',
    'pm10': 'pm10_ug_m3',
    'nitrogen_dioxide': 'no2_ug_m3',
    'sulphur_dioxide': 'so2_ug_m3',
    'ozone': 'o3_ug_m3',
    'carbon_monoxide': 'co_ug_m3',
    'dust': 'dust_ug_m3',
} -%}
{%- set ceilings = {
    'pm2_5': var('max_particulate_ug_m3'),
    'pm10': var('max_particulate_ug_m3'),
    'dust': var('max_particulate_ug_m3'),
    'nitrogen_dioxide': var('max_gas_ug_m3'),
    'sulphur_dioxide': var('max_gas_ug_m3'),
    'ozone': var('max_gas_ug_m3'),
    'carbon_monoxide': var('max_carbon_monoxide_ug_m3'),
} -%}

with source as (
    select * from {{ source('raw', 'air_quality_hourly') }}
),

renamed as (
    select
        city_id,
        measured_at,
        -- Pakistan Standard Time (UTC+5, no daylight saving). Days and seasons are local.
        measured_at at time zone 'Asia/Karachi' as measured_at_pkt,
        (measured_at at time zone 'Asia/Karachi')::date as date_pkt,
        extract(hour from measured_at at time zone 'UTC')::int as hour_utc,
        -- CAMS global is 3-hourly; the API interpolates the hours in between.
        extract(hour from measured_at at time zone 'UTC')::int % 3 = 0 as is_native_step,
        {%- for raw_name, name in measures.items() %}
        {{ raw_name }} as {{ name }},
        {%- endfor %}
        round(us_aqi)::int as us_aqi,
        grid_latitude,
        grid_longitude,
        nullif(array_to_string(array_remove(array[
            {%- for raw_name in measures %}
            case when {{ raw_name }} < 0 then 'negative {{ raw_name }}' end,
            {%- endfor %}
            case when us_aqi < 0 then 'negative us_aqi' end,
            {%- for raw_name, ceiling in ceilings.items() %}
            case when {{ raw_name }} > {{ ceiling }} then '{{ raw_name }} above {{ ceiling }}' end,
            {%- endfor %}
            -- PM2.5 is a subset of PM10, so it can never be larger.
            case when pm2_5 > pm10 then 'pm2_5 exceeds pm10' end
        ], null), '; '), '') as quarantine_reason,
        _source_file,
        _loaded_at,
        _batch_id
    from source
)

select
    city_id,
    measured_at,
    measured_at_pkt,
    date_pkt,
    hour_utc,
    is_native_step,
    {{ smog_season('date_pkt') }} as smog_season,
    {%- for name in measures.values() %}
    {{ name }},
    {%- endfor %}
    us_aqi,
    grid_latitude,
    grid_longitude,
    quarantine_reason,
    _source_file,
    _loaded_at,
    _batch_id
from renamed
