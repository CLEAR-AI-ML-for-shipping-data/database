CREATE OR REPLACE FUNCTION get_trajectories_in_bbox(
    bbox geometry,
    table_list text[]
)
RETURNS TABLE (
    id integer,
    mmsi VARCHAR,
    route_id VARCHAR,
    start_dt TIMESTAMP WITHOUT TIME ZONE,
    end_dt TIMESTAMP WITHOUT TIME ZONE,
    origin geometry,
    destination geometry,
    count integer,
    duration interval,
    missing_data boolean,
    missing_data_info VARCHAR,
    coordinates geometry,
    timestamps TIMESTAMP WITHOUT TIME ZONE[],
    speed_over_ground FLOAT[],
    navigational_status int[],
    course_over_ground FLOAT[],
    heading FLOAT[]
) AS $$
DECLARE
    tbl text;
BEGIN
    FOREACH tbl IN ARRAY table_list
    LOOP
        RETURN QUERY EXECUTE format('
            SELECT 
                id,
                mmsi,
                route_id,
                start_dt,
                end_dt,
                origin,
                destination,
                count,
                duration,
                missing_data,
                missing_data_info,
                coordinates,
                timestamps,
                speed_over_ground,
                navigational_status,
                course_over_ground,
                heading
            FROM %I
            WHERE ST_Intersects(coordinates, $1)
        ', tbl)
        USING bbox;
    END LOOP;
END;
$$ LANGUAGE plpgsql;


SELECT * FROM get_trajectories_in_bbox(
    ST_MakeEnvelope(11.7, 57.5, 11.8, 57.5, 4326),
    ARRAY[ 'trajectories_2023_02','trajectories_2023_03','trajectories_2023_04', 'trajectories_2023_08']
);