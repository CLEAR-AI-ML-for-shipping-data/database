CREATE OR REPLACE VIEW aggregated_ais_data_voyage AS
WITH aggregated AS (
    SELECT
        a.ship_id,
        a.timestamp,
        a.lat,
        a.lon,
        a.speed,
        -- 1 minute interval
        date_trunc('minute', a.timestamp) AS interval_1min,
        -- 5 minute interval
        date_trunc('minute', a.timestamp) - ((EXTRACT(MINUTE FROM a.timestamp)::int % 5) * interval '1 minute') AS interval_5min,
        -- 30 minute interval
        date_trunc('hour', a.timestamp) + ((EXTRACT(MINUTE FROM a.timestamp)::int / 30) * interval '30 minute') AS interval_30min,
        -- 1 hour interval
        date_trunc('hour', a.timestamp) AS interval_1hr,
        v.voyage_id
    FROM
        ais_data a
    JOIN
        voyages v ON a.ship_id = v.ship_id
    WHERE
        a.timestamp BETWEEN v.start_dt AND v.end_dt
)
-- Aggregate data at 1-minute intervals
SELECT
    voyage_id,
    interval_1min AS timestamp,
    '1min' AS resolution_type,
    AVG(lat) AS avg_latitude,
    AVG(lon) AS avg_longitude,
    AVG(speed) AS avg_speed
FROM aggregated
GROUP BY voyage_id, interval_1min

UNION ALL

-- Aggregate data at 5-minute intervals
SELECT
    voyage_id,
    interval_5min AS timestamp,
    '5min' AS resolution_type,
    AVG(lat) AS avg_latitude,
    AVG(lon) AS avg_longitude,
    AVG(speed) AS avg_speed
FROM aggregated
GROUP BY voyage_id, interval_5min

UNION ALL

-- Aggregate data at 30-minute intervals
SELECT
    voyage_id,
    interval_30min AS timestamp,
    '30min' AS resolution_type,
    AVG(lat) AS avg_latitude,
    AVG(lon) AS avg_longitude,
    AVG(speed) AS avg_speed
FROM aggregated
GROUP BY voyage_id, interval_30min

UNION ALL

-- Aggregate data at 1-hour intervals
SELECT
    voyage_id,
    interval_1hr AS timestamp,
    '1hr' AS resolution_type,
    AVG(lat) AS avg_latitude,
    AVG(lon) AS avg_longitude,
    AVG(speed) AS avg_speed
FROM aggregated
GROUP BY voyage_id, interval_1hr;




SELECT 
    voyage_id,
    timestamp,
    resolution_type,
    avg_latitude,
    avg_longitude,
    avg_speed
FROM 
    aggregated_ais_data_voyage
WHERE 
    voyage_id = 1234        
    AND resolution_type = '5min';  


SELECT 
    voyage_id,
    timestamp,
    resolution_type,
    avg_latitude,
    avg_longitude,
    avg_speed
FROM 
    aggregated_ais_data_voyage
WHERE 
    voyage_id = 1234; 
