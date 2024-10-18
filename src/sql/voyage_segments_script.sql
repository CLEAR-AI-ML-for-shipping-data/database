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
            AND ship_id IN ({ship_ids_batch})  
    ),
    segments AS (
        SELECT
            ship_id,
            point_time AS start_dt,
            next_time AS end_dt,
            point_geom AS origin,
            LEAD(point_geom) OVER (PARTITION BY ship_id ORDER BY point_time) AS destination,
            subquery.ais_data ,  
            subquery.ais_timestamps, 
            next_time - point_time AS duration, 
            subquery.num_points 
        FROM
            voyage_data vd
        CROSS JOIN LATERAL (
            SELECT 
                ST_MakeLine(ST_SetSRID(ST_MakePoint(ad.longitude, ad.latitude), 4326)ORDER BY ad.timestamp) AS ais_data, 
                ARRAY_AGG(ad.timestamp ORDER BY ad.timestamp) AS ais_timestamps,  
                COUNT(ad.timestamp) AS num_points 
            FROM
                ais_data ad
            WHERE
                ad.ship_id = vd.ship_id
                AND ad.timestamp >= vd.point_time
                AND ad.timestamp < vd.next_time
        ) AS subquery
        WHERE
            navigational_status in ({include_nav_statuses})
            AND next_status not in ({include_nav_statuses})
    )
    INSERT INTO voyage_segments (ship_id, start_dt, end_dt, origin, destination, ais_data, ais_timestamps, duration, count)
    SELECT
        ship_id,
        start_dt,
        end_dt,
        origin,
        destination,
        ais_data,
        ais_timestamps,
        duration,
        num_points
    FROM
        segments
    WHERE
        origin IS NOT NULL
        AND destination IS NOT NULL
        AND end_dt - start_dt >= INTERVAL {min_duration}
        AND num_points>{min_points};


