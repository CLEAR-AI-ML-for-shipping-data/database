import pandas as pd
import json
import re

def to_snake_case(text):
    # Replace spaces or hyphens with underscores
    text = re.sub(r'[\s-]+', '_', text)
    
    # Convert camelCase or PascalCase to snake_case
    text = re.sub(r'([a-z0-9])([A-Z])', r'\1_\2', text)
    
    # Make everything lowercase
    text = text.lower()
    
    # Remove any characters that aren't alphanumeric or underscores
    text = re.sub(r'[^a-z0-9_]', '', text)
    
    return text

file_path = "/home/sid/workspace/clear_ais/database/data/test.csv"
# file_path = "/home/sid/workspace/clear_ais/database/data/AIS 2023 SFV/1016Baltic39311016-20231230-44.csv"

mapping_path = 'src/source_to_db_mapping.json'

for chunk in pd.read_csv(file_path, chunksize=1000):

    cols = { col:to_snake_case(col) for col in chunk.columns}

    mapping_set = json.load(open(mapping_path,'r'))
    
    mapping_set.update(cols)

    json.dump(mapping_set, open(mapping_path,'w'), indent=4)

    break