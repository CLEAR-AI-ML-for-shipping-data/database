#!/bin/bash

. .env

set -e

filepath="./data/sweden-latest.osm.pbf"


echo $POSTGRES_DATA_DIR $filepath
osm2pgsql -c -d $POSTGRES_DB -U $POSTGRES_USER -H localhost -P $POSTGRES_PORT -W -r pbf  $filepath