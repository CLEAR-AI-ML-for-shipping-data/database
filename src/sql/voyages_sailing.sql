CREATE OR REPLACE FUNCTION validate_nav_status(nav_status TEXT, speed FLOAT)
RETURNS TEXT LANGUAGE plpgsql AS $$
BEGIN
    RETURN CASE 
		WHEN nav_status IN ('Engine', 'Sailing') THEN 'Engine'
        WHEN nav_status IN ('Moored' , 'Anchor') THEN nav_status
        WHEN nav_status IN ( 'No command', 'Restricted', 'Constrained', 'Reserved', 'Reserved (HSC)', 'Reserved (WIG)', 'Fishing') AND speed > 0.2 THEN 'Engine'
		WHEN nav_status IN ( 'No command', 'Restricted', 'Constrained', 'Reserved', 'Reserved (HSC)', 'Reserved (WIG)', 'Fishing') AND speed <= 0.2 THEN 'Anchor'
		ELSE 'unknown'
    END;
END;
$$;

-- Ensure the necessary extensions are available
CREATE EXTENSION IF NOT EXISTS cube;
CREATE EXTENSION IF NOT EXISTS earthdistance;

-- Temporary table to hold the voyage segments
WITH voyage_segments AS

    SELECT
        a.ship_id,
        a.timestamp AS start_dt,
        LEAD(a.timestamp) OVER (PARTITION BY a.ship_id ORDER BY timestamp) AS end_dt,
        ST_SetSRID(ST_MakePoint(a.lon, a.lat), 4326) AS origin,
        ST_SetSRID(ST_MakePoint(
            LEAD(a.lon) OVER (PARTITION BY a.ship_id ORDER BY a.timestamp),
            LEAD(a.lat) OVER (PARTITION BY a.ship_id ORDER BY a.timestamp)
        ), 4326) AS destination,
        validate_nav_status(n.code, a.speed) AS nav_status,
        speed,
		validate_nav_status(
	        LAG(n.code) OVER (PARTITION BY a.ship_id ORDER BY timestamp),
	        LAG(a.speed) OVER (PARTITION BY a.ship_id ORDER BY timestamp) 
		) AS prev_nav_status
    FROM ais_data a
    JOIN 
        nav_status n ON a.nav_status = n.id
	WHERE speed < 0.3

-- Identify the start and end of each voyage
voyages AS (
    SELECT 
        ship_id,
        event_time AS start_time,
        next_event_time AS end_time,
        lat AS start_lat,
        lon AS start_lon,
        next_lat AS end_lat,
        next_lon AS end_lon
    FROM 
        voyage_segments
	WHERE (nav_status IN ('Engine', 'Sailing') AND prev_nav_status NOT IN ('Engine', 'Sailing')) 
		OR (nav_status NOT IN ('Engine', 'Sailing') AND prev_nav_status IN ('Engine', 'Sailing'))

),

-- Final preparation of voyage data to be inserted
prepared_voyages AS (
    SELECT 
        ship_id,
        start_time,
        end_time,
        ST_SetSRID(ST_MakePoint(start_lon, start_lat), 4326) AS origin_geom,
        ST_SetSRID(ST_MakePoint(end_lon, end_lat), 4326) AS destination_geom
    FROM 
        voyages
    WHERE 
        start_time IS NOT NULL 
        AND end_time IS NOT NULL
        -- AND end_time - start_time >= INTERVAL '30 minutes' 
)

-- Insert the prepared voyage data into the voyages table
INSERT INTO voyages (ship_id, voyage_model_id, start_dt, end_dt, origin, destination)
SELECT 
    ship_id, 
    2,
    start_time, 
    end_time, 
    origin_geom, 
    destination_geom
FROM 
    prepared_voyages;



UPDATE voyage_segments vs
SET ais_data = COALESCE(
    -- Attempt to create a LINESTRING with all points between start_dt and end_dt
    (
        SELECT
            ST_MakeLine(ST_SetSRID(ST_MakePoint(lon, lat), 4326) ORDER BY timestamp)
        FROM
            ais_data ad
        WHERE
            ad.ship_id = vs.ship_id
            AND ad.timestamp BETWEEN vs.start_dt AND vs.end_dt
    ),
    -- If no points are found, create a LINESTRING with just the start and end points
    ST_MakeLine(
        ARRAY[
            ST_SetSRID(ST_MakePoint(ST_X(vs.origin), ST_Y(vs.origin)), 4326),
            ST_SetSRID(ST_MakePoint(ST_X(vs.destination), ST_Y(vs.destination)), 4326)
        ]
    )
);