import pandas as pd
from sqlalchemy.orm import Session
from sqlalchemy import select, create_engine, text
from datetime import datetime
import csv, traceback
from tqdm import tqdm
import argparse, os, json, time, re, threading, gc, uuid
from dateutil.parser import parse
from collections import defaultdict, OrderedDict
from geoalchemy2 import Geometry, WKTElement
from shapely.geometry import Point, LineString
import numpy as np
from itertools import chain
from concurrent.futures import ThreadPoolExecutor, as_completed
import queue, dotenv
from pathlib import Path
from database_schema import ClearAIS_DB, Ships, AIS_Data, Nav_Status, Trajectories, MissingData, MissingDataTable
from sqlalchemy.schema import UniqueConstraint, MetaData
from sqlalchemy import Column, Integer, String, ForeignKey, Date, Enum, Boolean, DateTime, Float, BigInteger, ARRAY, Interval, JSON, Table, MetaData

from utils import find_files_in_folder, try_except
from logger import getLogger, TqdmToLogger
from multiprocessing import Process, Queue
import signal
import sys

logger = getLogger(__file__,log_file_name='progress.log')

csv_to_db_mapping = json.load(open("src/csv_to_db_mapping.json",'r'))

from datetime import datetime
dateparse = lambda x: datetime.strptime(x, "%d %b %Y %H:%M:%S %Z")

SOG_THRESHOLD = 0.3 # Speed Over Ground
UPPER_SOG_THRESHOLD = 1.5
NAV_STATUS_MOVING =  ['Engine', 'Sailing','No command', 'Restricted', 'Constrained', 'Reserved', 'Reserved (HSC)', 'Reserved (WIG)', 'Fishing','Power-driven pushing']
# "No command", "unknown"
NAV_STATUS_STATIONARY = ["Moored", "Anchor"]

# Global variables for parallel processing
monthly_tables = {}
processing_queue = queue.Queue()
completed_files = set()
route_id_tracker = {}
missing_data = []

temp_tracking_storage = defaultdict(list)
complete_trajectories = defaultdict(list)
nav_status_set = {}
ships_data_df = pd.DataFrame([])

def create_monthly_table(session, year_month):
    """Create a monthly table for trajectories if it doesn't exist"""
    table_name = f"trajectories_{year_month}"
    
    # Check if table exists
    exists = session.execute(text(
        "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = :table_name)"
    ), {"table_name": table_name}).scalar()
    
    if not exists:
        # Create table with same schema as Trajectories
        metadata = MetaData()
        monthly_table = Table(
            table_name, metadata,
            Column('id', Integer, primary_key=True, autoincrement=True),
            Column('mmsi', String),
            Column('route_id', String),
            Column('start_dt', DateTime),
            Column('end_dt', DateTime),
            Column('origin', Geometry('POINT', 4326)),
            Column('destination', Geometry('POINT', 4326)),
            Column('count', Integer),
            Column('duration', Interval),
            Column('missing_data', Boolean),
            Column('missing_data_info', String,nullable=True),
            Column('coordinates', Geometry('LINESTRING', 4326)),
            Column('timestamps', ARRAY(DateTime)),
            Column('speed_over_ground', ARRAY(Float)),
            Column('navigational_status', ARRAY(Integer)),
            Column('course_over_ground', ARRAY(Float)),
            Column('heading', ARRAY(Float)),
            UniqueConstraint('mmsi', 'start_dt', name=f'uix_mmsi_start_dt_{year_month}')
        )
        monthly_table.create(session.get_bind(), checkfirst=True)
    
    return table_name

def create_geom_from_latlon(lat, lon):
    point_wkt = f'POINT({lon} {lat})'
    geom = WKTElement(point_wkt, srid=4326)
    return geom

def generate_route_id():
    return str(uuid.uuid4())[:6]

def signal_handler(signum, frame):
    print("Received signal to terminate. Cleaning up...")
    sys.exit(0)

def insert_complete_trajectories_to_db(trajectory_queue, database_url):
    """Process trajectories from queue and insert into database"""
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    bulk_inserter = ClearAIS_DB(database_url)
    trajectories_list = []
    
    try:
        while True:
            try:
                # Get data from queue with timeout
                data = trajectory_queue.get(timeout=1)
                if data is None:  # Poison pill to stop the process
                    break
                    
                mmsi, ais_data = data
                complete_trajectories[mmsi] = []
                ais_data_len = len(ais_data)
                
                if ais_data_len > 0:
                    for _ in range(ais_data_len):
                        (traj, route_id, year_month, missing_data_bool, missing_data_info ) = ais_data.pop()
                        first = traj.iloc[0]
                        last = traj.iloc[-1]
                        coordinates = LineString(list(zip(traj['longitude'], traj['latitude']))).wkt
                        row = {
                            "mmsi": mmsi,
                            'route_id': route_id,
                            "start_dt": first.timestamp,
                            "end_dt": last.timestamp,
                            "origin": create_geom_from_latlon(first.latitude, first.longitude),
                            "destination": create_geom_from_latlon(last.latitude, last.longitude),
                            "count": traj.shape[0],
                            "duration": last.timestamp - first.timestamp,
                            "missing_data": missing_data_bool,
                            "missing_data_info": json.dumps(missing_data_info),
                            "coordinates": coordinates,
                            "timestamps": traj.timestamp,
                            "speed_over_ground": traj.speed_over_ground,
                            "navigational_status": traj.navigational_status,
                            "course_over_ground": traj.course_over_ground,
                            "heading": traj.heading
                        }
                        trajectories_list.append((row, year_month))

                if len(trajectories_list) > 100:
                    # Group trajectories by month
                    monthly_trajectories = defaultdict(list)
                    for row, year_month in trajectories_list:
                        monthly_trajectories[year_month].append(row)
                    
                    # Insert into respective monthly tables
                    with bulk_inserter.Session() as session:
                        for year_month, month_data in monthly_trajectories.items():
                            table_name = create_monthly_table(session, year_month)
                            bulk_inserter.bulk_insert(table_name, month_data)
                    
                    # logger.info(f"Inserted {len(trajectories_list)} Trajectories")
                    trajectories_list = []

            except queue.Empty:
                # Queue is empty, check if we should continue
                if processing_queue.empty() and len(completed_files) > 0:
                    break
                continue
                
    except Exception as e:
        logger.error(f"Error in insert_complete_trajectories_to_db: {str(e)}")
        logger.exception("Full traceback:")
    finally:
        # Insert any remaining trajectories
        if trajectories_list:
            monthly_trajectories = defaultdict(list)
            for row, year_month in trajectories_list:
                monthly_trajectories[year_month].append(row)
            
            with bulk_inserter.Session() as session:
                for year_month, month_data in monthly_trajectories.items():
                    table_name = create_monthly_table(session, year_month)
                    bulk_inserter.bulk_insert(table_name, month_data)
            
            logger.info(f"Inserted final batch of {len(trajectories_list)} Trajectories")

def process_file(file_path, year_month, trajectory_queue):
    """Process a single CSV file and extract year-month from filename"""
    try:
        # Get total number of lines in file for progress bar
        total_lines = sum(1 for _ in open(file_path, 'r')) - 1  # Subtract header line
        chunk_size = 100000
        total_chunks = (total_lines + chunk_size - 1) // chunk_size
        
        # Create progress bar for this file
        with tqdm(total=total_chunks, desc=f"Processing {os.path.basename(file_path)}", 
                 unit="chunk", leave=False) as pbar:
            read_and_transform_csv_chunk(
                file_path=file_path,
                chunk_size=chunk_size,
                year_month=year_month,
                filename=file_path,
                trajectory_queue=trajectory_queue,
                progress_bar=pbar
            )
        completed_files.add(file_path)
    except Exception as e:
        logger.error(f"Error processing file {file_path}: {str(e)}")

def split_trajectories(chunk, year_month, filename, trajectory_queue):
    global temp_tracking_storage

    for mmsi, group in chunk.groupby('mmsi'):
        voyage_segment_end = False
        if mmsi in temp_tracking_storage:
            temp_tracking_storage[mmsi] = pd.concat([temp_tracking_storage[mmsi], group], ignore_index=True).sort_values(['timestamp'], ascending=[True])
        else:
            if mmsi not in route_id_tracker:
                route_id_tracker[mmsi] = generate_route_id()
            temp_tracking_storage[mmsi] = group

        # TODO: check the limit and also deal with shorter segments
        if temp_tracking_storage[mmsi].shape[0] > 500:
            traj_data = temp_tracking_storage[mmsi]
            diff = traj_data['timestamp'].diff()

            gap_threshold_days = pd.Timedelta('1 day')
            gap_threshold_months = pd.Timedelta(days=27)

            large_gaps_days = diff > gap_threshold_days
            large_gaps_months = diff > gap_threshold_months

            missing_data_bool = any(large_gaps_days) or any(large_gaps_months)

            # if not any(large_gaps_months):
            middle_index = len(traj_data) // 2
            middle_df = traj_data.iloc[middle_index-10:middle_index+10]
            middle_avg_speed = middle_df['speed_over_ground'].mean()
            middle_nav_status = middle_df['navigational_status'].mode()[0]

            last_rows = traj_data.tail(20)
            mean_speed = last_rows['speed_over_ground'].mean()
            median_nav_status = last_rows['navigational_status'].mode()[0]

            if nav_status_set[middle_nav_status] not in NAV_STATUS_STATIONARY and middle_avg_speed > UPPER_SOG_THRESHOLD:
                if nav_status_set[median_nav_status] in NAV_STATUS_STATIONARY and mean_speed < SOG_THRESHOLD:
                    voyage_segment_end = True

            route_id = route_id_tracker[mmsi]
            if voyage_segment_end:
                del route_id_tracker[mmsi]

            
            missing_data_info = {}
            if missing_data_bool:
                if any(large_gaps_months):
                    gap_duration = 'more than 27 day' 
                else:
                    gap_duration = 'more than a day'

                index1 = np.where(large_gaps_months==True)
                index2 = np.where(large_gaps_days==True)
                missing_data_indices = list(chain.from_iterable(index1)) + list(chain.from_iterable(index2))
                missing_data_indices  = [val.item() for val in missing_data_indices]

                missing_data_info = {'indices': missing_data_indices, "gap_duration":gap_duration}

            # Put trajectory data in queue
            trajectory_queue.put((mmsi, [(traj_data, route_id, year_month,missing_data_bool, missing_data_info )]))
            
            del temp_tracking_storage[mmsi]


@try_except(logger=logger)
def read_and_transform_csv_chunk(file_path, chunk_size=10000, year_month=None, filename=None, 
                               trajectory_queue=None, progress_bar=None):
    """
    Generator to read and transform CSV data in chunks and collect unique navigational statuses.
    """
    global ships_data_df
    global nav_status_set

    bulk_inserter = ClearAIS_DB(database_url)

    for chunk in pd.read_csv(file_path, chunksize=chunk_size):
        try:
            chunk.drop(chunk.columns[chunk.columns.str.contains('unnamed', case=False)], axis=1, inplace=True)
            chunk = chunk.rename(columns=csv_to_db_mapping)
            chunk.dropna(subset=['type_of_ship_and_cargo','type_of_ship', 'type_of_cargo','draught'], inplace=True)
            chunk = chunk.astype({'mmsi':str, 'imo':str, 'type_of_ship':int, 'type_of_cargo':int, 'type_of_ship_and_cargo':int})
            
            chunk['timestamp'] = pd.to_datetime(chunk['timestamp'].str.replace(r'\.\d{1,6}|\s+[A-Za-z]+$', '', regex=True), format='%d %b %Y %H:%M:%S')
            chunk['timestamp'] = chunk['timestamp'].dt.tz_localize('Europe/Berlin')

            if year_month is None:
                date_obj = chunk['timestamp'].iloc[:-1][0]
                year_month = f"{date_obj.year}_{date_obj.month:02d}" 

            if not "navigational_status" in chunk.columns:
                ids, uniques = pd.factorize(chunk['navigational_status_text'].values)
                chunk['navigational_status'] = ids

            nav_status_set.update(pd.Series(chunk['navigational_status_text'].values, index=chunk['navigational_status']).to_dict())
            
            # Process ships data first
            ship_data_cols = ['mmsi', 'ship_name', 'imo', 'size_a', 'size_b', 'size_c', 'size_d', 'type_of_ship', 'type_of_cargo', 'type_of_ship_and_cargo', 'draught']
            ships_data = chunk.filter(items=ship_data_cols)
            ships_data = ships_data.drop_duplicates(subset=["mmsi"]).to_dict(orient='records')

            if len(ships_data) > 0:
                bulk_inserter.bulk_insert_ships(ships_data)

            # Process AIS data
            ais_data_cols = ['timestamp', 'mmsi', 'latitude', 'longitude', 'navigational_status', 'navigational_status_text', 'speed_over_ground', 'heading','course_over_ground','country_ais', 'destination']
            ais_data = chunk.filter(items=ais_data_cols).copy()
            
            split_trajectories(ais_data, year_month, filename, trajectory_queue)
            
            # Update progress bar
            if progress_bar:
                progress_bar.update(1)
            
        except Exception as e:
            logger.error(f"Error processing chunk from {file_path}: {str(e)}")
            logger.exception("Full traceback:")
            continue

def sort_filenames_unixstyle(filenames:list):
    def natural_sort_key(filename):
        return [int(text) if text.isdigit() else text.lower() for text in re.split(r'(\d+)', filename)]

    return sorted(filenames,key=natural_sort_key)

def get_year_moth_from_filename(filename):
    date_pattern = re.compile(r'(\b(19\d{2}|20\d{2})[-_\.]?\d{2}[-_\.]?\d{2}|\b(19\d{2}|20\d{2})\d{6})')

    def extract_date(filename):
        matches = date_pattern.findall(filename)
        date_str = re.sub(r'[-/_]', ' ', matches[0][0])  
        try:
            return parse(date_str, fuzzy=True)  
        except ValueError:
            pass
        return None 
    
    date_obj = extract_date(filename)  
    if date_obj:
        year_month = f"{date_obj.year}_{date_obj.month:02d}" 
    else:
        year_month = "Unknown" 
    
    return year_month

def sort_file_names_by_year_month(filenames:list):
     
    temp_files_by_month = defaultdict(list)

    for file in filenames:
        year_month = get_year_moth_from_filename(file) 

        temp_files_by_month[year_month].append(file)

    sorted_year_months = sorted(temp_files_by_month, key=lambda ym: tuple(map(int, ym.split('-'))) if ym != "Unknown" else (float('inf'),))

    sorted_file_list = OrderedDict()
    for year_month in sorted_year_months:
        sorted_file_list[year_month] =  sort_filenames_unixstyle(temp_files_by_month[year_month])

    return sorted_file_list

if __name__=='__main__':
    env_path = Path(__file__).resolve().parent.parent / ".env"
    dotenv.load_dotenv(dotenv_path=env_path)

    POSTGRES_DB= os.getenv("POSTGRES_DB","gis") 
    POSTGRES_USER=os.getenv("POSTGRES_USER","clear") 
    POSTGRES_PASSWORD=os.getenv("POSTGRES_PASSWORD","clear") 
    POSTGRES_PORT=int(os.getenv("POSTGRES_PORT",5432))
    POSTGRES_HOST = os.getenv("POSTGRES_HOST","localhost") 
    database_url = f"postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"

    file_path = 'data/AIS 2023 SFV/989Baltic4071989-20230201-0.csv'
    file_path = "data/test.csv"
    folder_path = 'data/AIS 2023 SFV'

    parser = argparse.ArgumentParser()
    parser.add_argument('--datapath', type=str, default=file_path, help='csv files directory')
    parser.add_argument('--db_url', type=str, default=database_url, help="Postgres database url")
    args = parser.parse_args()

    path = args.datapath
    database_url = args.db_url
    if os.path.exists(path):
        bulk_inserter = ClearAIS_DB(database_url)
        bulk_inserter.create_tables(drop_existing=False)
        bulk_inserter.save_schema()

        # Create queues for inter-process communication
        trajectory_queue = Queue()

        if os.path.isfile(path):
            year_month = None
            process_file(path, year_month, trajectory_queue)
        else:
            csv_files = find_files_in_folder(path, extension=('.csv'))
            sorted_csv_files = sort_file_names_by_year_month(csv_files)
            
            # Start the trajectory insertion process
            p1 = Process(target=insert_complete_trajectories_to_db, 
                        args=(trajectory_queue, database_url))
            p1.start()

            tqdm_logger = TqdmToLogger(logger=logger)

            # Create overall progress bar for all files
            total_files = sum(len(files) for files in sorted_csv_files.values())
            with tqdm(total=total_files, file=tqdm_logger, desc="Overall Progress", unit="file") as pbar:
                for year_month, file_paths in sorted_csv_files.items():
                    for file_path in file_paths:
                        process_file(file_path, year_month, trajectory_queue)
                        pbar.update(1)
                        logger.info("completed: " + str(file_path))
              

            # Send poison pills to stop the processes
            trajectory_queue.put(None)

            # Wait for the processes to finish
            p1.join()