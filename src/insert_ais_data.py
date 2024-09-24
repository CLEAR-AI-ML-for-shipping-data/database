import pandas as pd
from sqlalchemy.orm import Session
from sqlalchemy import select, create_engine
from datetime import datetime
import csv, traceback
from tqdm import tqdm
import argparse, os, json

from db_schema import ClearAIS_DB, Ships, AIS_Data, Nav_Status
from utils import find_files_in_folder, try_except
from logger import getLogger

logger = getLogger(__file__)


csv_to_db_mapping = json.load(open("src/csv_to_db_mapping.json",'r'))

from datetime import datetime
dateparse = lambda x: datetime.strptime(x, "%d %b %Y %H:%M:%S %Z")

@try_except(logger=logger)
def read_and_transform_csv_chunk(file_path, chunk_size=10000):
    """
    Generator to read and transform CSV data in chunks and collect unique navigational statuses.
    
    :param file_path: The path to the CSV file.
    :param chunk_size: Number of rows per chunk.
    """
    nav_status_set = set()

    for chunk in pd.read_csv(file_path, chunksize=chunk_size, parse_dates=['Base station time stamp'], date_format=dateparse):
        # Ensure the columns are named correctly
        chunk.drop(chunk.columns[chunk.columns.str.contains('unnamed', case=False)], axis=1, inplace=True)

        chunk = chunk.rename(columns=csv_to_db_mapping)


        chunk.dropna(subset=['type_of_ship_and_cargo','type_of_ship', 'type_of_cargo','draught'], inplace=True)
        chunk = chunk.astype({'mmsi':str, 'imo':str, 'type_of_ship':int, 'type_of_cargo':int, 'type_of_ship_and_cargo':int})
        # Collect unique navigational statuses
        nav_status_set.update(chunk['navigational_status_text'].unique())

        ship_data_cols = ['mmsi', 'imo', 'size_a', 'size_b', 'size_c', 'size_d', 'type_of_ship', 'type_of_cargo', 'type_of_ship_and_cargo', 'draught']
        # Transform data into Ships table format
        ships_data = chunk.filter(items=ship_data_cols)
        ships_data = ships_data.drop_duplicates(subset=["mmsi"]).to_dict(orient='records')

        # Transform data into AIS_Data table format
        ais_data_cols = ['timestamp', 'mmsi', 'latitude', 'longitude', 'navigational_status', 'navigational_status_text', 'speed_over_ground', 'heading','course_over_ground','country_ais', 'destination']
        ais_data = chunk.filter(items=ais_data_cols).copy()
        # ais_data = ais_data.rename(columns={'Speed_over_ground':'speed'})

        ## NOTE change this later to have nav staus as ID instead
        # ais_data = ais_data.rename(columns={'Nav_status_text':'nav_status'}) 

        
        # Additional fields required for the AIS_Data table can be set to default or calculated values if not available in the CSV
        fileds_list = ['course_over_ground', 'heading', 'destination','rot', 'eot']

        ais_data = ais_data.to_dict(orient='records')
        
        yield ships_data, ais_data, nav_status_set


@try_except(logger=logger)
def bulk_insert_data_chunked(file_path, database_url, chunk_size=10000):
    """
    Function to bulk insert data from CSV file into the database in chunks and handle navigational statuses.
    
    :param file_path: The path to the CSV file.
    :param database_url: The database connection URL.
    :param chunk_size: Number of rows per chunk.
    """
    bulk_inserter = ClearAIS_DB(database_url)

    # Initialize progress bar
    total_rows = sum(1 for _ in open(file_path)) - 1  # Subtract 1 for the header row
    num_chunks = (total_rows // chunk_size) + 1
    progress_bar = tqdm(total=num_chunks, desc="Processing and Inserting Data")
    
    # Process chunks and collect all unique navigational statuses
    for ships_data, ais_data, nav_status_set in read_and_transform_csv_chunk(file_path, chunk_size):

        with bulk_inserter.Session() as session:
            new_nav_status_set = remove_existing_nav_status_ids(session, nav_status_set)

            new_ships_data = remove_existing_ships(session,ships_data)
        
        # Insert New Nav Statuses found
        if len(new_nav_status_set) > 0:
            bulk_inserter.bulk_insert(Nav_Status, new_nav_status_set)

        # Insert Ships data
        if len(new_ships_data)>0:
            bulk_inserter.bulk_insert(Ships, new_ships_data)
        
        # Insert AIS_Data with navigational status ID conversion
        with bulk_inserter.Session() as session:
            mmsi2ship_id_map = {ship.mmsi:ship.ship_id for ship in session.query(Ships).all()}
            nav_status = session.query(Nav_Status).all()
            nav_status_id_map = {status.code: status.id for status in nav_status}
            for ais_record in ais_data:
                ais_record['navigational_status'] = nav_status_id_map.get(ais_record['navigational_status_text'], 0)

                ais_record['ship_id'] = mmsi2ship_id_map.get(str(ais_record['mmsi']),0)

                del ais_record['navigational_status_text']
                del ais_record['mmsi']
            

            bulk_inserter.bulk_insert(AIS_Data, ais_data)

        # Update progress bar
        progress_bar.update(1)
    
    progress_bar.close()
    

def remove_existing_nav_status_ids(session, nav_status_set:set):
    """
    Function to get a map of navigational status text to ID.
    
    :param session: The SQLAlchemy session object.
    :param nav_status_set: Set of unique navigational status texts.
    :return: Dictionary mapping navigational status text to ID.
    """
    existing_nav_status = [status.code for status in session.query(Nav_Status).all()]
    nav_status_set.update(existing_nav_status)

    new_nav_status = nav_status_set - set(existing_nav_status)

    return [{'code': code} for code in new_nav_status]

# FIXME remove duplicate mmsi and verify it!!
def remove_existing_ships(session, ships_data):
    """
    Function to get a map of mmsi to ship_id.
    
    :param session: The SQLAlchemy session object.
    :param mmsi_set: Set of unique mmsi values.
    :return: Dictionary mapping mmsi to ship_id.
    """
    
    existing_ships = [ship.mmsi for ship in session.query(Ships).all()]
    
    if len(existing_ships)>0:
        # Filter ships_data to only include new mmsi
        ships_data = [ship for ship in ships_data if ship['mmsi'] not in existing_ships]
    return ships_data


if __name__=='__main__':

    POSTGRES_DB="gis"
    POSTGRES_USER="clear"
    POSTGRES_PASSWORD="clear"
    POSTGRES_PORT=5432
    POSTGRES_HOST = "localhost"
    database_url = f"postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"

    file_path = 'data/ais2018_chemical_tanker.csv'
    folder_path = 'data'

    parser = argparse.ArgumentParser()
    parser.add_argument('--datapath', type=str, default=folder_path, help='csv files directory')
    parser.add_argument('--db_url', type=str, default=database_url, help="Postgres database url")
    parser.add_argument('--save_results', default=False, action='store_true', help='save window detction results in json format')

    args =  parser.parse_args()

    path = args.datapath
    if os.path.exists(path):
        bulk_inserter = ClearAIS_DB(database_url)
        bulk_inserter.create_tables(drop_existing=False)
        bulk_inserter.save_schema()

        if os.path.isfile(path):

            bulk_insert_data_chunked(path, database_url, chunk_size=10000)
        else:
            csv_files = find_files_in_folder(path, extension=('.csv'))

            for csv_file in csv_files:
                bulk_insert_data_chunked(csv_file, database_url, chunk_size=10000)
                

        

        