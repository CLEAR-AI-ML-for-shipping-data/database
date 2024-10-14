WITH voyage_data AS (
        SELECT
            ship_id,
            timestamp AS point_time,
            ST_SetSRID(ST_MakePoint(longitude, latitude), 4326) AS point_geom,
            navigational_status,
            LEAD(timestamp) OVER (PARTITION BY ship_id ORDER BY timestamp) AS next_time,
            LEAD(navigational_status) OVER (PARTITION BY ship_id ORDER BY timestamp) AS next_status
        FROM
            ais_data
        WHERE
            speed_over_ground < {speed_threshold}
            AND ship_id IN ({ship_ids_str})  -- Process this batch of ship IDs
    ),
    segments AS (
        SELECT
            ship_id,
            point_time AS start_dt,
            next_time AS end_dt,
            point_geom AS origin,
            LEAD(point_geom) OVER (PARTITION BY ship_id ORDER BY point_time) AS destination,
            ST_MakeLine(point_geom, LEAD(point_geom) OVER (PARTITION BY ship_id ORDER BY point_time)) AS ais_data,
            next_time - point_time AS duration
        FROM
            voyage_data
        WHERE
            navigational_status in (1,8)
            AND next_status not in (1,8)
    )
    INSERT INTO voyage_segments (ship_id, start_dt, end_dt, origin, destination, ais_data, duration, count)
    SELECT
        ship_id,
        start_dt,
        end_dt,
        origin,
        destination,
        ais_data,
        duration,
        ST_NumPoints(ais_data) 
    FROM
        segments
    WHERE
        origin IS NOT NULL
        AND destination IS NOT NULL
        AND end_dt - start_dt >= INTERVAL '30 minutes';


