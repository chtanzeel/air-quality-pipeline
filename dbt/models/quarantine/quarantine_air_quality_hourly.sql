-- Rows that failed a validity check. Kept with the reason, never silently dropped.
-- The build fails if their share passes var('quarantine_max_share')
-- (tests/assert_quarantine_share_below_threshold.sql).
select *
from {{ ref('stg_air_quality_hourly_all') }}
where quarantine_reason is not null
