-- Detects upstream dropouts like 2022-12-13: hours where at least 4 of the 5 cities report
-- near-zero PM2.5 and NO2 at once. Each must be listed in seeds/upstream_incidents.csv (so it's
-- quarantined) after a human has looked at it. An unlisted one fails the build.
with dropout_hours as (
    select measured_at
    from {{ source('raw', 'air_quality_hourly') }}
    where pm2_5 < 2 and nitrogen_dioxide < 0.5
    group by measured_at
    having count(distinct city_id) >= 4
)

select d.measured_at
from dropout_hours d
left join {{ ref('upstream_incidents') }} i
    on d.measured_at between i.start_utc and i.end_utc
where i.incident_id is null
