
import sys
import math
import argparse
import traceback
from concurrent.futures import ProcessPoolExecutor, as_completed

from tqdm import tqdm
from db import PostGISConnection
from pathlib import Path
from shapely import points
from utils import *

# Nr of database table rows to process at each chunk.
CHUNK_SIZE = 1000

# Temporary storage for rasterized trajectories.
TMP_IMG_DIR = Path('/mnt/data2/ais_embeddings')

# Path to ml-analysis repo with functions for processing and model inference.
#ML_ANALYSIS_REPO = Path(__file__).resolve().parent.parent.parent / 'ml-analysis'

# Path to externel geometries shapefiles, e.g. coastlines.
EXTERNAL_GEOMETRIES_PATHS = [
    Path('./data/coastlines')
]

# Set to True to fetch all trajectories from scratch.
FETCH_FROM_SCRATCH = False

# Table names to process, if it is empty it will process all of them.
YEARS = [2023, 2024]
MONTHS = [1,2,3,4,5,6,7,8,9,10,11,12]
TRAJECTORY_TABLE_NAMES = [
    f'trajectories_{YEAR}_{month:02d}' for YEAR in YEARS for month in MONTHS
]

# Set to None to skip filtering.
SHIP_TYPE_FILTER = None #18

# Number of parallel workers (each processes one table at a time).
N_WORKERS = 4

# Import embedding model repo.
#sys.path.append(str(ML_ANALYSIS_REPO))

from ml_analysis.analysis.helpers.data_preparation import *


def process_table(table: str, ext_geoms_paths: list[Path], tmp_img_dir: Path,
                  chunk_size: int, ship_type_filter, fetch_from_scratch: bool) -> str:
    """Process a single trajectory table. Intended to run in a worker process."""

    # Re-import inside worker (needed when using ProcessPoolExecutor).
    from ml_analysis.analysis.helpers.data_preparation import time_windowing
    from utils import make_bounding_box
    import math
    import h5py
    import numpy as np
    import pandas as pd
    import geopandas as gpd
    from shapely import points
    from db import PostGISConnection
    from tqdm import tqdm

    print(f'\n\n\nProcessing Table: {table}\n')

    ext_geoms = [gpd.read_file(path) for path in ext_geoms_paths]

    hdf5_path = tmp_img_dir / f'{table}.hdf5'
    if hdf5_path.exists():
        if fetch_from_scratch:
            hdf5_path.unlink()
            trajectories_fetched = []
        else:
            with h5py.File(hdf5_path) as file:
                trajectories_fetched = list(set(
                    [key.split('_')[0] + '_' + key.split('_')[1] for key in file.keys()]
                ))
    else:
        trajectories_fetched = []

    db = PostGISConnection()
    total_rows = db.count_table(table, ship_type=ship_type_filter)
    table_iterator = db.load_dataframe_chunked(table, chunk_size, ship_type=ship_type_filter)

    for i, df in enumerate(tqdm(
        table_iterator,
        total=math.ceil(total_rows / chunk_size),
        unit='chunk',
        dynamic_ncols=True,
        desc=f'[{table}] Chunk',
        position=0,
        leave=True,
    )):
        df_bbox = make_bounding_box(df)
        ext_geoms_cut = [df_tmp.overlay(df_bbox, how='intersection') for df_tmp in ext_geoms]

        for idx, row in tqdm(
            df.iterrows(),
            total=len(df),
            desc=f'[{table}] Row',
            position=1,
            leave=False,
        ):
            timestamps = pd.DatetimeIndex(row['timestamps'])
            positions = row['coordinates'].coords

            if f'{row["mmsi"]}_{row["id"]}' not in trajectories_fetched:

                df_row = gpd.GeoDataFrame(
                    data={
                        'id': row['id'],
                        'mmsi': row['mmsi']
                    },
                    index=timestamps,
                    geometry=points(positions).tolist(),
                    crs=df.crs,
                )

                time_windowing(
                    dataf=df_row,
                    coastlines=ext_geoms_cut,
                    zipfile=hdf5_path,
                    prefix=f'{row["mmsi"]}_{row["id"]}',
                    export_dir=None,
                    value_cols=None,
                    dtype=np.uint8
                )

    return table


if __name__ == '__main__':

    TMP_IMG_DIR.mkdir(parents=True, exist_ok=True)

    # Resolve table list using a temporary DB connection.
    db = PostGISConnection()
    tables = db.list_trajectory_tables()
    if len(TRAJECTORY_TABLE_NAMES) > 0:
        tables = [t for t in TRAJECTORY_TABLE_NAMES if t in tables]

    print(f'Processing {len(tables)} table(s) with up to {N_WORKERS} parallel worker(s).\n')

    with ProcessPoolExecutor(max_workers=N_WORKERS) as executor:
        futures = {
            executor.submit(
                process_table,
                table,
                EXTERNAL_GEOMETRIES_PATHS,
                TMP_IMG_DIR,
                CHUNK_SIZE,
                SHIP_TYPE_FILTER,
                FETCH_FROM_SCRATCH,
            ): table
            for table in tables
        }

        for future in as_completed(futures):
            table = futures[future]
            try:
                result = future.result()
                print(f'\n✓ Finished: {result}\n{"=" * 51}\n')
            except Exception:
                print(f'\n✗ Table {table} raised an exception:\n{traceback.format_exc()}')