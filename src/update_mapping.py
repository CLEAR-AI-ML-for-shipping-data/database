import pandas as pd
import json
import re
import argparse

def to_snake_case(text):
    # Replace spaces or hyphens with underscores
    text = re.sub(r'[\s-]+', '_', text)
    
    # Convert camelCase or PascalCase to snake_case
    text = re.sub(r'([a-z0-9])([A-Z])', r'\1_\2', text)
    
    # Make everything lowercase
    text = text.lower()
    
    # Remove any characters that aren't alphanumeric or underscores
    text = re.sub(r'[^a-z0-9_]', '', text)

    if text == "base_station_time_stamp":
        text = "timestamp"
    
    return text

def main(file_path, mapping_path = 'src/csv_to_db_mapping.json'):
    mapping_path = 'src/csv_to_db_mapping.json'

    for chunk in pd.read_csv(file_path, chunksize=1000):

        cols = { col:to_snake_case(col) for col in chunk.columns if 'unnamed' not in str.lower(col)}

        mapping_set = json.load(open(mapping_path,'r'))
        
        mapping_set.update(cols)

        json.dump(mapping_set, open(mapping_path,'w'), indent=4)

        break

if __name__=='__main__':
    default_file_path = 'data/test.csv'
    json_mapping_path = 'src/csv_to_db_mapping.json'
    parser = argparse.ArgumentParser()
    parser.add_argument('--csvpath', type=str, help='path/to/csv')
    parser.add_argument('--jsonmapping', type=str, default=json_mapping_path, help="Postgres database url")
    args = parser.parse_args()

    main(file_path=args.csvpath, mapping_path=args.jsonmapping)
