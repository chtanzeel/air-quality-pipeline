select
    city_id,
    city,
    province,
    latitude,
    longitude,
    'Asia/Karachi' as timezone
from {{ ref('cities') }}
