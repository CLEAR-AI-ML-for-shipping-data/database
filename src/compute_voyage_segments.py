from tqdm import tqdm
import argparse, os, json, math

from db_schema import ClearAIS_DB, Ships, AIS_Data, Nav_Status
from utils import find_files_in_folder, try_except
from logger import getLogger

logger = getLogger(__file__)




def main(database_url, chunk_size_percent=1, script_path="voyage_segments_script.sql"):

    db = ClearAIS_DB(database_url)

    ship_ids = [ x[0] for x in db.execute(query="SELECT ship_id FROM ships ORDER BY ship_id;").fetchall()]

    nav_status_include_list = ['Engine', 'Sailing','No command', 'Restricted', 'Constrained', 'Reserved', 'Reserved (HSC)', 'Reserved (WIG)', 'Fishing','Power-driven pushing']
    nav_statuses = ', '.join([ str(x[0]) for x in db.execute(query="SELECT id, code FROM nav_status ORDER BY id;").fetchall() if x[1] in nav_status_include_list])

    total_ships = len(ship_ids)
    chunk_size = math.ceil(total_ships * (chunk_size_percent/100))  
    progress_bar = tqdm(total=total_ships, desc="Computing voyage segments: ")

    script_str = ''.join(open(script_path,'r').readlines())

    speed_threshold = 0.3 # speed throshold for slicing data
    min_duration="'2 hours'" # minimum duration for the voyage segment ex: 30 minutes, 1 hour, 5 minutes
    min_points=5 # minimum number of points accross the voyage segment



    for i in range(0, total_ships, chunk_size):
        ship_chunk = ship_ids[i:i + chunk_size]
       
        ship_ids_batch = ', '.join([str(ship_id) for ship_id in ship_chunk])

        query = script_str.format(
            speed_threshold=speed_threshold, 
            ship_ids_batch=ship_ids_batch,
            include_nav_statuses = nav_statuses,
            min_duration=min_duration, 
            min_points=min_points 
        )

        db.execute(query=query)

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

