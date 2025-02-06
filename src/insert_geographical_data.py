import argparse

import geopandas as gpd
from loguru import logger
from sqlalchemy import create_engine


def insert_geodata(coastline_file: str, tablename: str):
    POSTGRES_DB = "gis"
    POSTGRES_USER = "clear"
    POSTGRES_PASSWORD = "clear"
    POSTGRES_PORT = 5432
    POSTGRES_HOST = "localhost"
    database_url = f"postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"

    gdf = gpd.read_file(coastline_file)

    engine = create_engine(database_url)

    gdf.to_postgis(name=tablename, con=engine, if_exists="replace", index=False)

    logger.info(f"Succesfully inserted {coastline_file} into PostGIS database")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument("-s", "--shapefile", type=str, required=True)
    parser.add_argument("-n", "--tablename", type=str, required=True)

    args = parser.parse_args()

    insert_geodata(coastline_file=args.shapefile, tablename=args.tablename)
