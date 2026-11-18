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
    -- Get the shared visitors from our intermediate model
    SELECT * FROM {{ ref('int_cannibalization') }}
)

SELECT
    c.focal_store_id AS store_id,
    c.other_store_id,
    t.store_visitors,
    -- Get the other store's visitors
    (SELECT store_visitors FROM total_visitors WHERE store_id = c.other_store_id) AS other_store_visitors,
    c.shared_visitors,
    
    -- Calculate the percentage
    ROUND((c.shared_visitors / t.store_visitors) * 100, 2) AS pct_store_visitors_also_visit_other

FROM cannibalization c
JOIN total_visitors t 
    ON c.focal_store_id = t.store_id
WHERE c.shared_visitors > 0