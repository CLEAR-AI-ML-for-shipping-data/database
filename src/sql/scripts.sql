SELECT *
FROM ais_data_geom
WHERE lonlat && ST_MakeEnvelope(11.7, 57.5, 11.8, 57.5, 4326);