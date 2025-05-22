WITH traj1 AS (
    SELECT coordinates AS geom FROM public.trajectories_2023_01 WHERE id = 100
),
traj2 AS (
    SELECT coordinates AS geom FROM public.trajectories_2023_12 WHERE id = 5000
)
SELECT
    ST_Distance(t1.geom, t2.geom) AS distance_meters
FROM traj1 t1, traj2 t2;