import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import os
import time
from city_movement_model import generate_planted_devices

def generate_gps_data(target_rows=10_000_000):
    print(f"Starting generation of {target_rows:,} rows...")
    
    # 1. Generate the planted data
    print("Generating planted cohorts (30% cannibalization rule)...")
    planted_pings = generate_planted_devices(num_days=30)
    df_planted = pd.DataFrame(planted_pings)
    print(f"✅ Generated {len(df_planted):,} planted rows.")
    
    # 2. Generate background noise (the rest of the 10M rows)
    remaining_rows = target_rows - len(df_planted)
    print(f"Generating {remaining_rows:,} background noise rows...")
    
    city_center_lat, city_center_lon = 28.6139, 77.2090
    device_ids = [f"DEV-NOISE-{i}" for i in range(25000)]
    
    df_noise = pd.DataFrame({
        'device_id': np.random.choice(device_ids, size=remaining_rows),
        'lat': np.random.normal(loc=city_center_lat, scale=0.05, size=remaining_rows),
        'lon': np.random.normal(loc=city_center_lon, scale=0.05, size=remaining_rows),
        'day': np.random.randint(0, 30, size=remaining_rows)
    })

    # 3. Combine them
    print("Combining and adding timestamps...")
    df = pd.concat([df_planted, df_noise], ignore_index=True)
    
    # Add timestamps
    end_date = datetime.now()
    start_date = end_date - timedelta(days=30)
    
    timestamps = []
    for day in df['day']:
        base_date = start_date + timedelta(days=int(day))
        random_seconds = np.random.randint(0, 86400)
        timestamps.append(base_date + timedelta(seconds=random_seconds))
    
    df['timestamp'] = timestamps
    df = df[['device_id', 'lat', 'lon', 'timestamp']]

    # 4. Save to Parquet
    # Path goes up two levels: from simulator -> src -> root, then into data/raw
    output_path = os.path.join('..', '..', 'data', 'raw', 'gps_pings.parquet')
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    
    print(f"Saving to {output_path}...")
    df.to_parquet(output_path, index=False)

    print(f"✅ Success! Generated {len(df):,} rows.")

if __name__ == "__main__":
    generate_gps_data()