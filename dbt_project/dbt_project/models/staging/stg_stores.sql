select
    *
from {{ source('geopulse_raw', 'RAW_STORES') }}
