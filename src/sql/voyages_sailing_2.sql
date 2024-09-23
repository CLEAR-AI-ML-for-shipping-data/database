WITH voyage_data AS (
    SELECT
        ship_id,
        timestamp AS point_time,
        ST_SetSRID(ST_MakePoint(lon, lat), 4326) AS point_geom,
        nav_status,
        LEAD(timestamp) OVER (PARTITION BY ship_id ORDER BY timestamp) AS next_time,
        LEAD(nav_status) OVER (PARTITION BY ship_id ORDER BY timestamp) AS next_status
    FROM
        ais_data
	WHERE speed < 0.3
),
segments AS (
    SELECT
        ship_id,
        point_time AS start_dt,
        next_time AS end_dt,
        point_geom AS origin,
        LEAD(point_geom) OVER (PARTITION BY ship_id ORDER BY point_time) AS destination,
        ST_MakeLine(point_geom, LEAD(point_geom) OVER (PARTITION BY ship_id ORDER BY point_time)) AS ais_data
    FROM
        voyage_data
    WHERE
        nav_status in (1,3) 
        AND next_status not in (1,3)
)
INSERT INTO voyage_segments (ship_id, start_dt, end_dt, origin, destination, ais_data)
SELECT
    ship_id,
    start_dt,
    end_dt,
    origin,
    destination,
    ais_data
FROM
    segments
WHERE
    origin IS NOT NULL
    AND destination IS NOT NULL;