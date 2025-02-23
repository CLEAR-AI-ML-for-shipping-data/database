import pandas as pd
from sqlalchemy.orm import Session
from sqlalchemy import select, create_engine
from datetime import datetime
import csv, traceback
from tqdm import tqdm
import argparse, os, json
from collections import defaultdict
import gc

from db_schema import ClearAIS_DB, Ships, AIS_Data, Nav_Status
from utils import find_files_in_folder, try_except
from logger import getLogger

logger = getLogger(__file__)


csv_to_db_mapping = json.load(open("src/csv_to_db_mapping.json",'r'))

from datetime import datetime
dateparse = lambda x: datetime.strptime(x, "%d %b %Y %H:%M:%S %Z")


SOG_THRESHOLD = 0.3 # Speed Over Ground
NAV_STATUS_MOVING =  ['Engine', 'Sailing','No command', 'Restricted', 'Constrained', 'Reserved', 'Reserved (HSC)', 'Reserved (WIG)', 'Fishing','Power-driven pushing']
NAV_STATUS_STATIONARY = ["Moored", "Anchor", "No command", "unknown"]

temp_tracking_storage = defaultdict(list)
complete_voyages = defaultdict(list)
nav_status_set = {}
ships_data_df = pd.DataFrame([])


def insert_complete_voyages_to_db():

    while True:
        if len(complete_voyages)>0:
            print(len(complete_voyages))
        else:
            pass


def track_voyages(chunk):
    global temp_tracking_storage

    for mmsi, group in chunk.groupby('mmsi'):
        voyage_segment_end = False

        if mmsi in temp_tracking_storage:
            temp_tracking_storage[mmsi] = pd.concat([temp_tracking_storage[mmsi], group], ignore_index=True)
        else:
            temp_tracking_storage[mmsi] = group


        if temp_tracking_storage[mmsi].shape[0] > 100:
            average_speed = temp_tracking_storage[mmsi].iloc[:100]['speed_over_ground'].mean()
            check_data = temp_tracking_storage[mmsi]
            if average_speed > 1:

                last_rows = temp_tracking_storage[mmsi].tail(5)

                mean_speed = last_rows['speed_over_ground'].mean()
                median_speed = last_rows['speed_over_ground'].median()
                
                mean_nav_status = last_rows['navigational_status'].mean()
                median_nav_status = last_rows['navigational_status'].median()

                
                if nav_status_set[median_nav_status] in NAV_STATUS_STATIONARY and mean_speed < SOG_THRESHOLD:
                    voyage_segment_end = True
                
                if voyage_segment_end:
                    complete_voyages[mmsi].append(group)
     
            


@try_except(logger=logger)
def read_and_transform_csv_chunk(file_path, chunk_size=10000):
    """
    Generator to read and transform CSV data in chunks and collect unique navigational statuses.
    
    :param file_path: The path to the CSV file.
    :param chunk_size: Number of rows per chunk.
    """
    global ships_data_df
    global nav_status_set

    for chunk in pd.read_csv(file_path, chunksize=chunk_size, parse_dates=['Base station time stamp'], date_format=dateparse):
        chunk.drop(chunk.columns[chunk.columns.str.contains('unnamed', case=False)], axis=1, inplace=True)

        chunk = chunk.rename(columns=csv_to_db_mapping)

        chunk.dropna(subset=['type_of_ship_and_cargo','type_of_ship', 'type_of_cargo','draught'], inplace=True)
        chunk = chunk.astype({'mmsi':str, 'imo':str, 'type_of_ship':int, 'type_of_cargo':int, 'type_of_ship_and_cargo':int})
        
        # Collect unique navigational statuses
        nav_status_set.update(pd.Series(chunk['navigational_status_text'].values, index=chunk['navigational_status']).to_dict())
        
        # Transform data into Ships table format
        ship_data_cols = ['mmsi', 'imo', 'size_a', 'size_b', 'size_c', 'size_d', 'type_of_ship', 'type_of_cargo', 'type_of_ship_and_cargo', 'draught']
        ships_data = chunk.filter(items=ship_data_cols)
        ships_data = ships_data.drop_duplicates(subset=["mmsi"])
        ships_data_df = pd.concat([ships_data_df, ships_data]) #.to_dict(orient='records')

        # Transform data into AIS_Data table format
        ais_data_cols = ['timestamp', 'mmsi', 'latitude', 'longitude', 'navigational_status', 'navigational_status_text', 'speed_over_ground', 'heading','course_over_ground','country_ais', 'destination']
        ais_data = chunk.filter(items=ais_data_cols).copy()
        ais_data_sorted = ais_data.sort_values(['mmsi', 'timestamp'], ascending=[False, True])
        
        track_voyages(ais_data_sorted)





if __name__=='__main__':

    POSTGRES_DB="gis"
    POSTGRES_USER="clear"
    POSTGRES_PASSWORD="clear"
    POSTGRES_PORT=5432
    POSTGRES_HOST = "localhost"
    database_url = f"postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"

    file_path = 'data/999Baltic7204999-20230615-72.csv'
    folder_path = 'data/AIS 2023 SFV'

    parser = argparse.ArgumentParser()
    parser.add_argument('--datapath', type=str, default=file_path, help='csv files directory')
    parser.add_argument('--db_url', type=str, default=database_url, help="Postgres database url")

    read_and_transform_csv_chunk(file_path=file_path, chunk_size=10000)