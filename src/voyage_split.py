import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random
from shapely.geometry import Point

# Constants
SAMPLE_SIZE = 1000  # Number of AIS records
TIME_INTERVAL = 1  # Minutes between records
SPEED_THRESHOLD = 0.1  # Speed threshold to consider the ship as moving
LONG_SEGMENT_PROBABILITY = 0.8  # Probability of continuing the same status

# Generate random AIS data with longer continuous segments
def generate_ais_data(sample_size, time_interval, long_segment_probability):
    start_date = datetime.now() - timedelta(days=1)
    nav_statuses = ['Underway Using Engine', 'At Anchor', 'Moored', 'Sailing', 'Not Under Command', 'Unknown']
    speeds = []
    timestamps = [start_date + timedelta(minutes=i * time_interval) for i in range(sample_size)]

    # Initialize the first status randomly
    current_status = random.choice(nav_statuses)
    current_speed = random.uniform(0, 20) if current_status in ['Underway Using Engine', 'Sailing'] else 0

    nav_status_series = []
    for i in range(sample_size):
        if random.random() > long_segment_probability:
            # Randomly switch to a different status
            current_status = random.choice(nav_statuses)
            current_speed = random.uniform(0, 20) if current_status in ['Underway Using Engine', 'Sailing'] else 0

        nav_status_series.append(current_status)
        speeds.append(current_speed)

    data = {
        'ship_id': [1] * sample_size,
        'timestamp': timestamps,
        'lat': np.random.uniform(-90, 90, sample_size),
        'lon': np.random.uniform(-180, 180, sample_size),
        'nav_status': nav_status_series,
        'speed': speeds
    }
    return pd.DataFrame(data)

# Process the AIS data to identify voyages
def identify_voyages(df):
    voyages = []
    prev_status = None
    prev_speed = 0
    start_dt = None
    start_point = None

    for _, row in df.iterrows():
        # Check if the voyage is starting
        if (row['nav_status'] in ['Underway Using Engine', 'Sailing'] and prev_status not in ['Underway Using Engine', 'Sailing']) \
                or (row['speed'] > SPEED_THRESHOLD and prev_speed <= SPEED_THRESHOLD):
            if start_dt is None:
                start_dt = row['timestamp']
                start_point = Point(row['lon'], row['lat'])
        
        # Check if the voyage is ending
        if start_dt and ((row['nav_status'] not in ['Underway Using Engine', 'Sailing']) or row['speed'] <= SPEED_THRESHOLD):
            end_dt = row['timestamp']
            end_point = Point(row['lon'], row['lat'])
            voyages.append({
                'start_dt': start_dt,
                'end_dt': end_dt,
                'origin': start_point,
                'destination': end_point
            })
            start_dt = None

        # Update previous status and speed
        prev_status = row['nav_status']
        prev_speed = row['speed']
    
    return voyages

# Main function
def main():
    # Generate sample AIS data with longer continuous segments
    df = generate_ais_data(SAMPLE_SIZE, TIME_INTERVAL, LONG_SEGMENT_PROBABILITY)
    print("Generated AIS Data:")
    print(df.head())

    df.to_csv('ais_sample_data.csv')

    # Identify voyages
    voyages = identify_voyages(df)
    print("\nIdentified Voyages:")
    for voyage in voyages:
        print(f"Voyage from {voyage['start_dt']} to {voyage['end_dt']}")
        print(f"  Origin: {voyage['origin']}")
        print(f"  Destination: {voyage['destination']}\n")

if __name__ == "__main__":
    main()
