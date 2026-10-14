-- Fails the build when too many rows are quarantined: a sign that something upstream changed,
-- not just a few bad values.
with counts as (
    select
        (select count(*) from {{ ref('quarantine_air_quality_hourly') }}) as quarantined,
        (select count(*) from {{ ref('stg_air_quality_hourly_all') }}) as total
)
select
    quarantined,
    total,
    quarantined::numeric / nullif(total, 0) as share,
    {{ var('quarantine_max_share') }} as max_share
from counts
where quarantined::numeric / nullif(total, 0) > {{ var('quarantine_max_share') }}
