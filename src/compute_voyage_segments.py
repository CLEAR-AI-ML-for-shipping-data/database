from tqdm import tqdm
import argparse, os, json, math

from db_schema import ClearAIS_DB, Ships, AIS_Data, Nav_Status
from utils import find_files_in_folder, try_except
from logger import getLogger

logger = getLogger(__file__)




def main(database_url, chunk_size_percent=1, script_path="voyage_segments_script.sql"):

    db = ClearAIS_DB(database_url)

    ship_ids = [ x[0] for x in db.excecute(query="SELECT ship_id FROM ships ORDER BY ship_id;").fetchall()]

    total_ships = len(ship_ids)
    chunk_size = math.ceil(total_ships * (chunk_size_percent/100))  
    progress_bar = tqdm(total=total_ships, desc="Computing voyage segments: ")

    script_str = ''.join(open(script_path,'r').readlines())


    for i in range(0, total_ships, chunk_size):
        ship_chunk = ship_ids[i:i + chunk_size]
       
        ship_ids_str = ', '.join([str(ship_id) for ship_id in ship_chunk])

        query = script_str.format(speed_threshold=0.3, ship_ids_str=ship_ids_str)


        db.excecute(query=query)

        progress_bar.update(chunk_size)


if __name__=='__main__':

    POSTGRES_DB="gis"
    POSTGRES_USER="clear"
    POSTGRES_PASSWORD="clear"
    POSTGRES_PORT=5432
    POSTGRES_HOST = "localhost"
    database_url = f"postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"


    parser = argparse.ArgumentParser()
    parser.add_argument('--db_url', type=str, default=database_url, help="Postgres database url")

    main(database_url=database_url, chunk_size_percent=0.1, script_path="src/sql/voyage_segments_script.sql")

