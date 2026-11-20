
{{ config(materialized='table') }}

WITH total_visitors AS (

    -- Get total unique visitors per store
    SELECT
        store_id,
        COUNT(DISTINCT device_id) AS store_visitors
    FROM {{ ref('stg_ping_store_matches') }}
    GROUP BY store_id

),

cannibalization AS (

    -- Get shared visitors from intermediate model
    SELECT *
    FROM {{ ref('int_cannibalization') }}

)

SELECT
    c.focal_store_id AS store_id,
    c.other_store_id,

    -- Focal store visitors
    t.store_visitors,

    -- Other store visitors (using LEFT JOIN)
    ot.store_visitors AS other_store_visitors,

    -- Shared visitors
    c.shared_visitors,

    -- Calculate percentage of focal store visitors
    -- who also visit the other store
    ROUND(
        (c.shared_visitors * 100.0) / NULLIF(t.store_visitors, 0),
        2
    ) AS pct_store_visitors_also_visit_other

FROM cannibalization c

-- Join focal store visitors
INNER JOIN total_visitors t
    ON c.focal_store_id = t.store_id

-- Join other store visitors
LEFT JOIN total_visitors ot
    ON c.other_store_id = ot.store_id

WHERE c.shared_visitors > 0
