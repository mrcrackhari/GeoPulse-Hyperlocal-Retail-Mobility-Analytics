select
    *
from {{ source('geopulse_raw', 'RAW_GPS_PINGS') }}
