-- Hours per city, local month and US AQI band. Hours without an AQI value are left out.
select
    city_id,
    date_trunc('month', date_pkt)::date as month,
    {{ aqi_band('us_aqi') }} as aqi_band,
    {{ aqi_band_order('us_aqi') }} as aqi_band_order,
    count(*) as hours
from {{ ref('stg_air_quality_hourly') }}
where us_aqi is not null
group by 1, 2, 3, 4
