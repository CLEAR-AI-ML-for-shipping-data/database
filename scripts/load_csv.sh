#!/bin/bash

. .env

set -e

filepath="data/ais2018_chemical_tanker.csv"


echo $POSTGRES_DATA_DIR $filepath
ogr2ogr -f "PostgreSQL" -lco GEOMETRY_NAME=geom -lco FID=gid PG:"dbname='$POSTGRES_DB' host='localhost' port='$POSTGRES_PORT' user='$POSTGRES_USER' password='$POSTGRES_PASSWORD'" $filepath -nlt PROMOTE_TO_MULTI -nln ais2018 -oo X_POSSIBLE_NAMES=longitude* -oo Y_POSSIBLE_NAMES=latitude*