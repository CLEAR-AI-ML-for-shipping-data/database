
import sys
import torch
import importlib
import traceback
from datetime import datetime

import numpy as np

import matplotlib.pyplot as plt

from tqdm import tqdm
from db import PostGISConnection
from pathlib import Path
from utils import *


# Inference batch size.
BATCH_SIZE = 1024

# Temporary storage for rasterized trajectories.
TMP_IMG_DIR = Path('/mnt/data2/ais_embeddings')

# Path to ml-analysis repo with functions for processing and model inference.
#ML_ANALYSIS_REPO = Path(__file__).resolve().parent.parent.parent / 'ml-analysis'

# Path to PyTorch model.
MODEL_FILE_PATH = Path('./data/251015/trained_model_20250516_1555.pt')
MODEL_DEVICE = 'cuda'

# Debug samples
DEBUG = False

# If entries in embeddings table should be updated if they already exists.
UPDATE_ENTRIES = False

# Import embedding model repo.
#sys.path.append(str(ML_ANALYSIS_REPO))

from ml_analysis.analysis.datasets.voyage_hdf5_dataset import VoyageHDF5Dataset

sys.modules["astromorph.astromorph"] = importlib.import_module("astromorph")
sys.modules["astromorph.astromorph.src"] = importlib.import_module("astromorph")
sys.modules["models"] = importlib.import_module("ml_analysis.analysis.models")
sys.modules["models.coastal_voyage_model"] = importlib.import_module("ml_analysis.analysis.models.coastal_voyage_model")

if __name__ == '__main__':
    try:
        # List available raster files.
        raster_files = list(TMP_IMG_DIR.glob('*.hdf5'))
        
        # Load model.
        print(f'Loading Model: {MODEL_FILE_PATH.stem}')
        model = torch.load(
            MODEL_FILE_PATH,
            weights_only=False,
            map_location=MODEL_DEVICE
        )
        model.eval()

        # Connecting to db
        db = PostGISConnection()
        

        # Create dummy input and generate dimensions and plot.
        print('Computing Embedding Dimensions.')
        dataset = VoyageHDF5Dataset(raster_files[0], train=False)
        with torch.no_grad():
            embed_dummy = model(dataset[0].to(MODEL_DEVICE))
            embed_len = embed_dummy.shape[-1]

            for f in raster_files:

                print(f'\n\n\nProcessing Raster File: {f.stem}\n')

                # Create table if it does not exist.
                print('Creating embedding table if it does not exist.')

                embed_table_name = f.stem.replace('trajectories', 'embeddings')
                ref_table_name = f.stem
                db.create_embedding_table(
                    table_name=embed_table_name,
                    embedding_length=embed_len,
                    referenced_table=ref_table_name
                )

                dataset = VoyageHDF5Dataset(f, train=False)


                indices = list(range(len(dataset)))
                batches = [indices[b:b + BATCH_SIZE] for b in range(0, len(indices), BATCH_SIZE)]

                for batch_indices in tqdm(batches, desc='Model Inference'):

                    # Parse metadata for every index in the batch upfront.
                    batch_meta = []
                    for i in batch_indices:
                        _id = dataset.filenames[i].split('_')
                        batch_meta.append({
                            'idx':      i,
                            'mmsi':     _id[0],
                            'row_id':   int(_id[1]),
                            'start_dt': datetime.strptime(f"{_id[2]} {_id[3]}", "%Y%m%d %H%M%S"),
                            'end_dt':   datetime.strptime(f"{_id[4]} {_id[5]}", "%Y%m%d %H%M%S"),
                        })

                    if not UPDATE_ENTRIES:
                        # Single batch query to find which entries already exist.
                        keys = [(m['mmsi'], m['row_id'], m['start_dt'], m['end_dt']) for m in batch_meta]
                        existing_keys = db.get_existing_keys(embed_table_name, keys)
                        batch_meta = [
                            m for m in batch_meta
                            if (m['mmsi'], m['row_id'], m['start_dt'], m['end_dt']) not in existing_keys
                        ]

                    if not batch_meta:
                        continue

                    if DEBUG:

                        # Plot only the first image in the batch (using original index).
                        first_meta = batch_meta[0]
                        img = dataset[first_meta['idx']]
                        first_img = np.moveaxis(img.numpy().squeeze(), 0, -1)
                        print(f"Debug batch — first sample: mmsi={first_meta['mmsi']}, "
                              f"id={first_meta['row_id']}, "
                              f"start={first_meta['start_dt']}, end={first_meta['end_dt']}")
                        array_to_rgb_imshow(first_img, expand=False, show=True)

                    else:

                        # Load images only for the (filtered) entries that need processing.
                        imgs = torch.cat([dataset[m['idx']] for m in batch_meta], dim=0)
                        print((imgs.element_size() * imgs.numel()) / (1024**2))

                        # Run a single forward pass for the filtered batch.
                        embeds = model(imgs.to(MODEL_DEVICE))           # (B, embed_len)
                        embeds = embeds.detach().cpu().numpy()           # numpy (B, embed_len)

                        for j, m in tqdm(enumerate(batch_meta), total=len(batch_meta), desc='Inserting Embeddings'):

                            # When UPDATE_ENTRIES is True we still need to know if the row
                            # exists so insert_embedding_data can choose INSERT vs UPDATE.
                            already_exists = UPDATE_ENTRIES and db.entry_exists(
                                table_name=embed_table_name,
                                mmsi=m['mmsi'],
                                trajectory_id=m['row_id'],
                                start_dt=m['start_dt'],
                                end_dt=m['end_dt'],
                            )

                            embed = embeds[j].tolist()

                            # Insert or update the embedding data.
                            db.insert_embedding_data(
                                table_name=embed_table_name,
                                mmsi=m['mmsi'],
                                trajectory_id=m['row_id'],
                                start_dt=m['start_dt'],
                                end_dt=m['end_dt'],
                                embedding=embed,
                                update_entry=already_exists
                            )
    except Exception:
        print(f'\n✗ An error occurred:\n{traceback.format_exc()}')
