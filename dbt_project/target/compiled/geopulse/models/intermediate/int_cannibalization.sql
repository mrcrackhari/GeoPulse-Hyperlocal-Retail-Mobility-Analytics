

WITH store_visits AS (
    -- Get the unique devices that visited each store
    SELECT DISTINCT
        store_id,
        device_id
    FROM GEOPULSE_DB.DBT_STAGING.stg_ping_store_matches 
    -- Note: This references Member 2's table which we will load soon
)

SELECT
    a.store_id AS focal_store_id,
    b.store_id AS other_store_id,
    COUNT(DISTINCT a.device_id) AS shared_visitors
FROM store_visits a
JOIN store_visits b 
    ON a.device_id = b.device_id 
    AND a.store_id != b.store_id
GROUP BY 
    a.store_id, 
    b.store_id