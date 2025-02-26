import pandas as pd
from sqlalchemy.orm import Session
from sqlalchemy import select, create_engine
from datetime import datetime
import csv, traceback
from tqdm import tqdm
import argparse, os, json, time, re, threading, gc
from dateutil.parser import parse
from collections import defaultdict, OrderedDict
from geoalchemy2 import Geometry, WKTElement
from shapely.geometry import Point, LineString
import numpy as np
from itertools import chain

from db_schema import ClearAIS_DB, Ships, AIS_Data, Nav_Status, Trajectories
from utils import find_files_in_folder, try_except
from logger import getLogger

logger = getLogger(__file__)


csv_to_db_mapping = json.load(open("src/csv_to_db_mapping.json",'r'))

from datetime import datetime
dateparse = lambda x: datetime.strptime(x, "%d %b %Y %H:%M:%S %Z")


SOG_THRESHOLD = 0.3 # Speed Over Ground
UPPER_SOG_THRESHOLD = 1.5
NAV_STATUS_MOVING =  ['Engine', 'Sailing','No command', 'Restricted', 'Constrained', 'Reserved', 'Reserved (HSC)', 'Reserved (WIG)', 'Fishing','Power-driven pushing']
# "No command", "unknown"
NAV_STATUS_STATIONARY = ["Moored", "Anchor"]

temp_tracking_storage = defaultdict(list)
complete_trajectories = defaultdict(list)
nav_status_set = {}
ships_data_df = pd.DataFrame([])

def create_geom_from_latlon(lat, lon):
    point_wkt = f'POINT({lon} {lat})'
    geom = WKTElement(point_wkt, srid=4326)
    return geom

def insert_complete_trajectories_to_db():
    global complete_trajectories
    global temp_tracking_storage

    bulk_inserter = ClearAIS_DB(database_url)
    trajectories_list = []
    while True:
        if len(complete_trajectories)>10:
            
            for mmsi, ais_data in complete_trajectories.items():
                if len(ais_data)>0:
                    for _ in range(len(ais_data)):
                        traj = ais_data.pop()
                        first = traj.iloc[0]
                        last = traj.iloc[-1]
                        coordinates = LineString(list(zip(traj['longitude'], traj['latitude']))).wkt
                        row = {
                            "mmsi": mmsi,
                            "start_dt": first.timestamp,
                            "end_dt": last.timestamp,
                            "origin": create_geom_from_latlon(first.latitude, first.longitude),
                            "destination": create_geom_from_latlon(last.latitude, last.longitude),
                            "count": traj.shape[0],
                            "duration": last.timestamp - first.timestamp,
                            "coordinates": coordinates,
                            "timestamps": traj.timestamp,
                            "speed_over_ground": traj.speed_over_ground,
                            "navigational_status": traj.navigational_status
                        }

                        trajectories_list.append(row)

            if len(trajectories_list)>500: 
                bulk_inserter.bulk_insert(Trajectories, trajectories_list)
                

                print(f"Inserted {len(trajectories_list)} Trajectories " )

                trajectories_list = []

        time.sleep(1)


missing_data = []

def split_trajectories(chunk):
    global temp_tracking_storage

    for mmsi, group in chunk.groupby('mmsi'):
        voyage_segment_end = False
        # sorted_group = group.sort_values([ 'timestamp'], ascending=[True])
        if mmsi in temp_tracking_storage:
            temp_tracking_storage[mmsi] = pd.concat([temp_tracking_storage[mmsi], group], ignore_index=True).sort_values([ 'timestamp'], ascending=[True])
        else:

            temp_tracking_storage[mmsi] = group

        if temp_tracking_storage[mmsi].shape[0] > 500 :
            voyage_data = temp_tracking_storage[mmsi]
            diff = voyage_data['timestamp'].diff()          

            gap_threshold_days = pd.Timedelta('1 day')  # 1 day
            gap_threshold_months = pd.Timedelta(days=27)  # Approx. 1 month

            large_gaps_days = diff > gap_threshold_days
            large_gaps_months = diff > gap_threshold_months

            if not any(large_gaps_months):

                middle_index = len(voyage_data) // 2 
                middle_df = voyage_data.iloc[middle_index-10:middle_index+10]
                middle_avg_speed = middle_df['speed_over_ground'].mean()
                middle_nav_status = middle_df['navigational_status'].mode()[0]

                last_rows = temp_tracking_storage[mmsi].tail(20)

                mean_speed = last_rows['speed_over_ground'].mean()

                median_nav_status = last_rows['navigational_status'].mode()[0]

                if nav_status_set[middle_nav_status] not in NAV_STATUS_STATIONARY and middle_avg_speed > UPPER_SOG_THRESHOLD:
                    if nav_status_set[median_nav_status] in NAV_STATUS_STATIONARY and mean_speed < SOG_THRESHOLD:
                        voyage_segment_end = True
                
                if voyage_segment_end:
                    complete_trajectories[mmsi].append(voyage_data)
                    del temp_tracking_storage[mmsi]
            else:
                index1 = np.where(large_gaps_months==True)
                index2 = np.where(large_gaps_days==True)
                missig_data_indices = list(chain.from_iterable(index1)) + list(chain.from_iterable(index2))
                row = [mmsi, missig_data_indices]
                for i in missig_data_indices:
                    middle_df = voyage_data.iloc[i-5:i+5].values
                    row.append(middle_df)
                missing_data.append(row)
                # TODO Save this data somewhere
    
            


@try_except(logger=logger)
def read_and_transform_csv_chunk(file_path, chunk_size=10000):
    """
    Generator to read and transform CSV data in chunks and collect unique navigational statuses.
    
    :param file_path: The path to the CSV file.
    :param chunk_size: Number of rows per chunk.
    """
    global ships_data_df
    global nav_status_set

    for chunk in pd.read_csv(file_path, chunksize=chunk_size):
        chunk.drop(chunk.columns[chunk.columns.str.contains('unnamed', case=False)], axis=1, inplace=True)

        chunk = chunk.rename(columns=csv_to_db_mapping)

        chunk.dropna(subset=['type_of_ship_and_cargo','type_of_ship', 'type_of_cargo','draught'], inplace=True)
        chunk = chunk.astype({'mmsi':str, 'imo':str, 'type_of_ship':int, 'type_of_cargo':int, 'type_of_ship_and_cargo':int})
        chunk['timestamp'] = pd.to_datetime(chunk['timestamp'].str.replace(r'\s+[A-Za-z]+$', '', regex=True), format='%d %b %Y %H:%M:%S')

        chunk['timestamp'] = chunk['timestamp'].dt.tz_localize('Europe/Berlin')

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
        # ais_data['timestamp'] = pd.to_datetime(ais_data['timestamp'])
        # ais_data['timestamp'].dt.tz_convert('CET')
        # ais_data_sorted = ais_data.sort_values(['mmsi', 'timestamp'], ascending=[False, True])
        
        split_trajectories(ais_data)


    # insert_complete_trajectories_to_db()

def sort_filenames_unixstyle(filenames:list):
    def natural_sort_key(filename):
        return [int(text) if text.isdigit() else text.lower() for text in re.split('(\d+)', filename)]

    return sorted(filenames,key=natural_sort_key)

def sort_file_names_by_year_month(filenames:list):
    date_pattern = re.compile(r'(\b(19\d{2}|20\d{2})[-_\.]?\d{2}[-_\.]?\d{2}|\b(19\d{2}|20\d{2})\d{6})')

    def extract_date(filename):
        matches = date_pattern.findall(filename)
        date_str = re.sub(r'[-/_]', ' ', matches[0][0])  
        try:
            return parse(date_str, fuzzy=True)  
        except ValueError:
            pass
        return None  
    temp_files_by_month = defaultdict(list)

    for file in filenames:
        date_obj = extract_date(file)  
        if date_obj:
            year_month = f"{date_obj.year}-{date_obj.month:02d}" 
        else:
            year_month = "Unknown"  

        temp_files_by_month[year_month].append(file)

    sorted_year_months = sorted(temp_files_by_month, key=lambda ym: tuple(map(int, ym.split('-'))) if ym != "Unknown" else (float('inf'),))

    sorted_file_list = OrderedDict()
    for year_month in sorted_year_months:
        sorted_file_list[year_month] =  sort_filenames_unixstyle(temp_files_by_month[year_month])

    return sorted_file_list

if __name__=='__main__':

    POSTGRES_DB="gis"
    POSTGRES_USER="clear"
    POSTGRES_PASSWORD="a4DaW96L85HU"
    POSTGRES_PORT=5432
    POSTGRES_HOST = "localhost"
    database_url = f"postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"

    file_path = 'data/999Baltic7204999-20230615-72.csv'
    folder_path = 'data/AIS 2023 SFV'

    parser = argparse.ArgumentParser()
    parser.add_argument('--datapath', type=str, default=folder_path, help='csv files directory')
    parser.add_argument('--db_url', type=str, default=database_url, help="Postgres database url")
    args =  parser.parse_args()

    path = args.datapath
    if os.path.exists(path):
        bulk_inserter = ClearAIS_DB(database_url)
        bulk_inserter.create_tables(drop_existing=False)
        bulk_inserter.save_schema()

        if os.path.isfile(path):
            # bulk_insert_data_chunked(path, database_url, chunk_size=10000)
            read_and_transform_csv_chunk(file_path=path, chunk_size=10000)
        else:
            csv_files = find_files_in_folder(path, extension=('.csv'))
            
            # Sort the files using the custom natural sort key
            sorted_csv_files = sort_file_names_by_year_month(csv_files)
            n_files = len(csv_files)
            t = threading.Thread(target=insert_complete_trajectories_to_db)
            t.start()

            count = 0
            for year_month, file_paths in sorted_csv_files.items():
                for file_path in file_paths:
                    count+=1
                    print(count, '/',n_files, ': ', file_path)
                    # bulk_insert_data_chunked(csv_file, database_url, chunk_size=50000)
                    read_and_transform_csv_chunk(file_path=file_path, chunk_size=100000)
                

                
                

    