CREATE VIEW voyage_resolutions_custom AS
SELECT
    v.id,
    CASE
        WHEN interval_type = '1min' THEN date_trunc('minute', a.timestamp)
        WHEN interval_type = '5min' THEN date_trunc('minute', a.timestamp) - ((EXTRACT(MINUTE FROM a.timestamp)::int % 5) * interval '1 minute')
        WHEN interval_type = '30min' THEN date_trunc('hour', a.timestamp) + ((EXTRACT(MINUTE FROM a.timestamp)::int / 30) * interval '30 minute')
        WHEN interval_type = '1hr' THEN date_trunc('hour', a.timestamp)
    END AS truncated_timestamp,
    AVG(a.lat) AS avg_latitude,
    AVG(a.lon) AS avg_longitude,
    AVG(a.speed) AS avg_speed
FROM
    voyages v
JOIN
    ais_data a ON v.mmsi = a.mmsi AND a.timestamp BETWEEN v.start_dt AND v.end_dt
GROUP BY
    v.id, truncated_timestamp, interval_type
ORDER BY
    v.id, truncated_timestamp;