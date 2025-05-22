CREATE OR REPLACE FUNCTION validate_nav_status(nav_status TEXT, speed FLOAT)
RETURNS TEXT LANGUAGE plpgsql AS $$
BEGIN
    RETURN CASE 
        WHEN nav_status IN ('Engine', 'Sailing') AND speed > 0.5 THEN 'Engine'
        WHEN nav_status IN ('Engine', 'Sailing') AND speed <= 0.5 THEN 'Anchor'
        WHEN nav_status IN ('Moored' , 'Anchor') AND speed <= 0.5 THEN nav_status
        WHEN nav_status IN ('unknown', 'No command', 'Restricted', 'Constrained', 'Reserved', 'Reserved (HSC)', 'Reserved (WIG)', 'Fishing') AND speed > 0.5 THEN 'Engine'
        WHEN nav_status IN ('unknown', 'No command','Restricted', 'Constrained', 'Reserved', 'Reserved (HSC)', 'Reserved (WIG)', 'Fishing') AND speed <= 0.5 THEN 'Anchor'
        ELSE 'unknown'
    END;
END;
$$;

-- Ensure the necessary extensions are available
CREATE EXTENSION IF NOT EXISTS cube;
CREATE EXTENSION IF NOT EXISTS earthdistance;

-- Temporary table to hold the voyage segments
WITH voyage_segments AS (
    SELECT 
        s.id AS ship_id,  -- Get the ship_id from ships table
        a.timestamp AS event_time,
        a.lat,
        a.lon,
        a.nav_status,
         -- Validate the current nav_status using the function
        validate_nav_status(a.nav_status, a.speed) AS validated_nav_status,
        -- Retrieve and validate the previous nav_status and speed using the function
        validate_nav_status(
            LAG(a.nav_status) OVER (PARTITION BY s.id ORDER BY a.timestamp),
            LAG(a.speed) OVER (PARTITION BY s.id ORDER BY a.timestamp)
        ) AS validated_prev_nav_status,
        LEAD(a.timestamp) OVER (PARTITION BY s.id ORDER BY a.timestamp) AS next_event_time,
        LEAD(a.lat) OVER (PARTITION BY s.id ORDER BY a.timestamp) AS next_lat,
        LEAD(a.lon) OVER (PARTITION BY s.id ORDER BY a.timestamp) AS next_lon
    FROM 
        ais_data a
    JOIN 
        ships s ON a.mmsi = s.mmsi  -- Join ais_data with ships to get ship_id
),

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
    WHERE 
        (validated_nav_status = 'Engine' AND validated_prev_nav_status IN ('Moored' , 'Anchor' ))
        OR (validated_prev_nav_status = 'Engine' AND validated_nav_status IN ('Moored' , 'Anchor'))
),

-- Final preparation of voyage data to be inserted
prepared_voyages AS (
    SELECT 
        ship_id,
        start_time,
        end_time,
        -- CONCAT(start_lat, ',', start_lon) AS origin,
        -- CONCAT(end_lat, ',', end_lon) AS destination
        ST_SetSRID(ST_MakePoint(start_lon, start_lat), 4326) AS origin_geom,
        ST_SetSRID(ST_MakePoint(end_lon, end_lat), 4326) AS destination_geom
    FROM 
        voyages
    WHERE 
        start_time IS NOT NULL 
        AND end_time IS NOT NULL
        AND end_time - start_time >= INTERVAL '30 minutes' 
)

-- Insert the prepared voyage data into the voyages table
INSERT INTO voyages (ship_id, voyage_model_id, start_dt, end_dt, origin, destination)
SELECT 
    ship_id, 
    4,
    start_time, 
    end_time, 
    origin_geom, 
    destination_geom
FROM 
    prepared_voyages;
