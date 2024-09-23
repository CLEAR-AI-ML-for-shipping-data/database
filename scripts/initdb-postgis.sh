#!/bin/bash
set -e

# Activate PostGIS extensions
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
  CREATE EXTENSION IF NOT EXISTS postgis;
  CREATE EXTENSION IF NOT EXISTS postgis_topology;
  CREATE EXTENSION IF NOT EXISTS postgis_raster;
  CREATE EXTENSION IF NOT EXISTS postgis_sfcgal;
  CREATE EXTENSION IF NOT EXISTS postgis_tiger_geocoder;
EOSQL