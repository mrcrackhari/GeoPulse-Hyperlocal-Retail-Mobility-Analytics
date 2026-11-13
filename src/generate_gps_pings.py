import pandas as pd
import numpy as np
from faker import Faker
from datetime import datetime, timedelta
import os
import time

# Initialize Faker for unique device IDs
fake = Faker()

def generate_gps_data(num_rows=100000): # Starting with 100k for a quick test!
    print(f"Starting generation of {num_rows:,} rows...")
    start_time = time.time()

    # 1. Generate Device IDs (Simulating ~10,000 unique shoppers)
    print("Generating Device IDs...")
    device_pool = [f"DEV-{fake.uuid4()[:8]}" for _ in range(10000)]
    device_ids = np.random.choice(device_pool, size=num_rows)

    # 2. Generate Coordinates (Simulating a city center, e.g., New Delhi)
    print("Generating Coordinates...")
    city_center_lat = 28.6139
    city_center_lon = 77.2090
    # Normal distribution creates a realistic cluster around the center
    latitudes = np.random.normal(loc=city_center_lat, scale=0.05, size=num_rows)
    longitudes = np.random.normal(loc=city_center_lon, scale=0.05, size=num_rows)

    # 3. Generate Timestamps (Over the last 30 days)
    print("Generating Timestamps...")
    end_date = datetime.now()
    start_date = end_date - timedelta(days=30)
    timestamps = pd.to_datetime(np.random.randint(
        start_date.timestamp(), end_date.timestamp(), num_rows), unit='s')

    # 4. Create the DataFrame
    print("Building DataFrame...")
    df = pd.DataFrame({
        'DeviceID': device_ids,
        'Latitude': latitudes,
        'Longitude': longitudes,
        'Timestamp': timestamps
    })

    # 5. Export to Parquet
    output_path = os.path.join('data', 'raw', 'gps_pings.parquet')
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    
    print(f"Saving to {output_path}...")
    df.to_parquet(output_path, index=False, engine='pyarrow')

    end_time = time.time()
    print(f"✅ Success! Generated {num_rows:,} rows in {round(end_time - start_time, 2)} seconds.")

if __name__ == "__main__":
    # We will change this to 10,000,000 later. For now, let's test with 100,000.
    generate_gps_data(10000000)