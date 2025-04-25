#!/usr/bin/env python3
import os
import argparse
from pathlib import Path
import math
import subprocess
from concurrent.futures import ProcessPoolExecutor
from tqdm import tqdm

def split_files(folder_path, num_splits):
    """Split files in the folder into approximately equal groups"""
    # Get all CSV files
    files = list(Path(folder_path).glob('**/*.csv'))
    
    # Calculate files per split
    files_per_split = math.ceil(len(files) / num_splits)
    
    # Create splits
    splits = []
    for i in range(0, len(files), files_per_split):
        split = files[i:i + files_per_split]
        splits.append([str(f) for f in split])
    
    return splits

def process_split(args):
    """Process a single split of files"""
    split_files, split_num, total_splits, db_url, max_workers = args
    
    # Create temporary folder for this split
    temp_folder = f"temp_split_{split_num}"
    os.makedirs(temp_folder, exist_ok=True)
    
    # Create symbolic links to files in temp folder
    for file_path in split_files:
        link_path = os.path.join(temp_folder, os.path.basename(file_path))
        if os.path.exists(link_path):
            os.remove(link_path)
        os.symlink(os.path.abspath(file_path), link_path)
    
    # Run the AIS data processor with this split
    cmd = [
        "python3", 
        "src/ais_data_processor.py",
        "--datapath", temp_folder,
        "--db_url", db_url,
        "--max_workers", str(max_workers)
    ]
    
    # Create progress bar description
    desc = f"Split {split_num}/{total_splits}"
    
    # Run the process and capture output
    process = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        universal_newlines=True
    )
    
    # Monitor the process
    while True:
        return_code = process.poll()
        if return_code is not None:
            break
    
    # Clean up temp folder
    for file_path in os.listdir(temp_folder):
        os.remove(os.path.join(temp_folder, file_path))
    os.rmdir(temp_folder)
    
    return split_num, return_code

def main():
    parser = argparse.ArgumentParser(description='Split and process AIS data files')
    parser.add_argument('--folder', type=str, required=True, help='Input folder containing CSV files')
    parser.add_argument('--splits', type=int, default=4, help='Number of splits to create')
    parser.add_argument('--db_url', type=str, required=True, help='Database URL')
    parser.add_argument('--workers_per_split', type=int, default=2, 
                      help='Number of workers for each split')
    
    args = parser.parse_args()
    
    # Split the files
    splits = split_files(args.folder, args.splits)
    
    # Prepare arguments for each split
    process_args = [
        (split, i+1, len(splits), args.db_url, args.workers_per_split)
        for i, split in enumerate(splits)
    ]
    
    # Create progress bars for overall progress
    # with tqdm(total=len(splits), desc="Overall Progress") as pbar:
    #     # Process splits in parallel
    with ProcessPoolExecutor(max_workers=len(splits)) as executor:
        # Submit all processes
        futures = [executor.submit(process_split, args) for args in process_args]
        
        # Monitor completion
        for future in futures:
            split_num, return_code = future.result()
            if return_code == 0:
                # pbar.update(1)
                print(f"Split {split_num} completed with return code {return_code}")
            else:
                print(f"Split {split_num} failed with return code {return_code}")

if __name__ == "__main__":
    main()