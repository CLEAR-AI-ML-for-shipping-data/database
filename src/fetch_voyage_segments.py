import argparse, os, json, math

from db_schema import ClearAIS_DB, Ships, AIS_Data, Nav_Status, Voyage_Segments
from utils import find_files_in_folder, try_except
from logger import getLogger

logger = getLogger(__file__)


def main():
    db = ClearAIS_DB(database_url)

    ship_ids = [ x[0] for x in db.excecute(query="SELECT DISTINCT on (ship_id) ship_id FROM voyage_segments ORDER BY ship_id;").fetchall()]

    
    with db.Session() as session:
        for ship_id in ship_ids:
            voyage_segemnts_per_ship = session.query(Voyage_Segments).where(Voyage_Segments.ship_id == ship_id).all()

            
            for voyage_per_ship in voyage_segemnts_per_ship:
                ## additional code here?
                
                print(ship_id, voyage_per_ship.duration, voyage_per_ship.count, voyage_per_ship.start_dt, voyage_per_ship.end_dt)

                # print(len(voyage_per_ship.ais_data), len(voyage_per_ship.ais_timestamps))






if __name__=='__main__':

    POSTGRES_DB="gis"
    POSTGRES_USER="clear"
    POSTGRES_PASSWORD="clear"
    POSTGRES_PORT=5432
    POSTGRES_HOST = "localhost"
    database_url = f"postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"


    parser = argparse.ArgumentParser()
    parser.add_argument('--db_url', type=str, default=database_url, help="Postgres database url")

    main(database_url=database_url)