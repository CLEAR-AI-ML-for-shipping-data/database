#!/usr/bin/env python3
import os, re
import argparse
from pathlib import Path
import math
import subprocess
from concurrent.futures import ProcessPoolExecutor
from tqdm import tqdm
from itertools import chain
from dateutil.parser import parse
from collections import defaultdict, OrderedDict

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

def split_files(folder_path, num_splits):
    """Split files in the folder into approximately equal groups"""
    # Get all CSV files
    csv_files = [str(p) for p in Path(folder_path).glob('**/*.csv')]

    sorted_csv_files = sort_file_names_by_year_month(csv_files)
    
    # Calculate files per split
    files_per_split = math.ceil(len(sorted_csv_files) / num_splits)
    
    ss = list(sorted_csv_files.values())
    # Create splits
    splits = []
    for i in range(0, len(sorted_csv_files), files_per_split):
        split = ss[i:i + files_per_split]
        
        splits.append(list(chain.from_iterable(split)))
    
    return splits

def process_split(args):
    """Process a single split of files"""
    split_files, split_num, total_splits, db_url = args
    
    # Create temporary folder for this split
    temp_folder = f"temp/temp_split_{split_num}"
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
        "--db_url", db_url
    ]
    
    # Create progress bar description
    desc = f"Split {split_num}/{total_splits}"
    
    # Run the process and capture output
    process = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        universal_newlines=True,
        shell=True
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
    folder_path = 'data/AIS 2023 SFV'
    parser = argparse.ArgumentParser(description='Split and process AIS data files')
    parser.add_argument('--folder', type=str,  default=folder_path, help='Input folder containing CSV files')
    parser.add_argument('--splits', type=int, default=4, help='Number of splits to create')
    parser.add_argument('--db_url', type=str, help='Database URL')
    
    args = parser.parse_args()
    
    # Split the files
    splits = split_files(args.folder, args.splits)
    
    # Prepare arguments for each split
    process_args = [
        (split, i+1, len(splits), args.db_url)
        for i, split in enumerate(splits)
    ]

    with ProcessPoolExecutor(max_workers=len(splits)) as executor:
    
        futures = [executor.submit(process_split, args) for args in process_args]
  
        for future in futures:
            split_num, return_code = future.result()
            if return_code == 0:
            
                print(f"Split {split_num} completed with return code {return_code}")
            else:
                print(f"Split {split_num} failed with return code {return_code}")

if __name__ == "__main__":
    main()