WITH nearest_origin_ports AS (
    SELECT 
        v.voyage_id,
        p.name AS origin_port_name,
        ST_Distance(v.origin, p.geometry) AS origin_distance
    FROM 
        voyage_segments v
    LEFT JOIN 
        ne_ports p ON ST_DWithin(v.origin, p.geometry, 10000) -- 10000 meters (10 km) tolerance, adjust as needed
    ORDER BY 
        origin_distance
)
UPDATE voyage_segments v
SET 
    origin_port = nop.origin_port_name,
    origin_port_distance = nop.origin_distance
FROM 
    nearest_origin_ports nop
WHERE 
    v.voyage_id = nop.voyage_id
    AND nop.origin_port_name IS NOT NULL; -- Ensure only rows with a port match are updated

--  Update voyage_segments table with nearest port names and distances for destination
WITH nearest_destination_ports AS (
    SELECT 
        v.voyage_id,
        p.name AS destination_port_name,
        ST_Distance(v.destination, p.geometry) AS destination_distance
    FROM 
        voyage_segments v
    LEFT JOIN 
        ne_ports p ON ST_DWithin(v.destination, p.geometry, 10000) -- 10000 meters (10 km) tolerance, adjust as needed
    ORDER BY 
        destination_distance
)
UPDATE voyage_segments v
SET 
    destination_port = ndp.destination_port_name,
    destination_port_distance = ndp.destination_distance
FROM 
    nearest_destination_ports ndp
WHERE 
    v.voyage_id = ndp.voyage_id
    AND ndp.destination_port_name IS NOT NULL; -- Ensure only rows with a port match are updated
