ALTER TABLE public.voyages
    ADD COLUMN origin_port VARCHAR(255),
    ADD COLUMN destination_port VARCHAR(255);
    ADD COLUMN IF NOT EXISTS origin_port_distance FLOAT,
    ADD COLUMN IF NOT EXISTS destination_port_distance FLOAT;

-- Drop the columns if necessary
ALTER TABLE public.voyages
    DROP COLUMN IF EXISTS origin,
    DROP COLUMN IF EXISTS destination;

-- Re-add them as geometry types
ALTER TABLE public.voyages
    ADD COLUMN origin geometry(Point, 4326),
    ADD COLUMN destination geometry(Point, 4326);

ALTER TABLE public.ais_data
ALTER COLUMN nav_status TYPE INT USING nav_status::integer

-- CHANGE GEOMETRY type
ALTER TABLE voyage_segments
    ALTER COLUMN destination TYPE geometry(POINT, 4326)
    USING ST_SetSRID(destination::geometry, 4326);


-- Step 1: Add the ship_id column to the ais_data table
ALTER TABLE ais_data
ADD COLUMN ship_id INTEGER;

-- Step 2: Update ais_data with the corresponding ship_id by matching MMSI
UPDATE ais_data a
SET ship_id = s.id
FROM ships s
WHERE a.MMSI = s.MMSI;



-- Optionally, if you want to ensure data consistency, you can add a foreign key constraint:
ALTER TABLE ais_data
ADD CONSTRAINT fk_ship_id
FOREIGN KEY (ship_id) REFERENCES ships(ship_id);


UPDATE voyages v
SET mmsi = s.mmsi
FROM ships s
WHERE v.ship_id = s.id;

ALTER TABLE public.voyage_segments
    ADD COLUMN data_point_count INT;

UPDATE public.voyage_segments v
SET data_point_count = (
    SELECT COUNT(*)
    FROM ais_data a
    WHERE a.ship_id = v.ship_id
    AND a.timestamp BETWEEN v.start_dt AND v.end_dt
);

SELECT * FROM public.voyages
WHERE end_dt - start_dt >= INTERVAL '30 minutes'
ORDER BY id ASC, ship_id ASC 

CREATE OR REPLACE VIEW long_voyages AS
SELECT * FROM public.voyage_segments
WHERE end_dt - start_dt >= INTERVAL '90 days' AND data_point_count > 1000
ORDER BY voyage_id ASC, ship_id ASC 

DELETE FROM public.voyages

CREATE OR REPLACE VIEW specific_voyage_ais_data AS
SELECT
    a.mmsi,
    a.timestamp,
    a.lat,
    a.lon,
    a.speed,
    a.nav_status
FROM
    ais_data a
JOIN
    voyages v ON a.mmsi = v.mmsi
WHERE
    a.timestamp BETWEEN v.start_dt AND v.end_dt
    AND v.id = 173694;


