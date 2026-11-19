import pandas as pd
import numpy as np
import os

def create_mock_matches():
    print("Generating mock spatial matches...")
    # Create 100,000 fake matches
    data = {
        'device_id': [f'DEV-{np.random.randint(1000, 2000)}' for _ in range(100000)],
        'store_id': [f'S{np.random.randint(1, 10):04d}' for _ in range(100000)],
        'timestamp': pd.date_range(start='2026-11-01', periods=100000, freq='min'),
        'lat': np.random.uniform(28.6, 28.7, 100000),
        'lon': np.random.uniform(77.2, 77.3, 100000)
    }
    df = pd.DataFrame(data)
    
    # Save to the processed folder
    output_dir = os.path.join('..', '..', 'data', 'processed', 'ping_store_matches')
    os.makedirs(output_dir, exist_ok=True)
    output_path = os.path.join(output_dir, 'part-00000.parquet')
    
    df.to_parquet(output_path, index=False)
    print(f"✅ Mock data saved to {output_path}")

if __name__ == "__main__":
    create_mock_matches()