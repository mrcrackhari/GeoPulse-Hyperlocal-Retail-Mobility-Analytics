import snowflake.connector
import pandas as pd
import os
from snowflake.connector.pandas_tools import write_pandas
import time

# ==========================================
# ⚠️ FILL IN YOUR DETAILS HERE
# ==========================================
USER = 'HARIMAHAJAN108@GMAIL.COM'
PASSWORD = 'Harimahajan@1212' 
ACCOUNT = 'DAGCDID-WV73577'

WAREHOUSE = 'COMPUTE_WH'
DATABASE = 'GEOPULSE_DB'
SCHEMA = 'RAW'

def upload_to_snowflake():
    print("1. Connecting to Snowflake...")
    conn = snowflake.connector.connect(
        user=USER, password=PASSWORD, account=ACCOUNT,
        warehouse=WAREHOUSE, database=DATABASE, schema=SCHEMA
    )
    
    # Explicitly set the schema
    cursor = conn.cursor()
    cursor.execute(f"USE SCHEMA {DATABASE}.{SCHEMA}")
    print("✅ Connected and schema set successfully!")

    # 2. Read the Parquet file
    # Note: Because we are in src/ingestion, we need to go up two levels to find data/
    file_path = os.path.join('..', '..', 'data', 'raw', 'gps_pings.parquet')
    print(f"2. Reading massive Parquet file from {file_path}...")
    
    start_time = time.time()
    df = pd.read_parquet(file_path)
    print(f"✅ Loaded {len(df):,} rows into memory in {round(time.time() - start_time, 2)} secs.")

    # 3. Drop old table to avoid duplicates
    print("3. Dropping old GPS_PINGS table (if it exists)...")
    cursor.execute("DROP TABLE IF EXISTS GPS_PINGS;")

    # 4. Upload to Snowflake
    print("4. Uploading 10 Million rows to Snowflake (This will take 1-3 minutes)...")
    start_time = time.time()
    
    success, nchunks, nrows, _ = write_pandas(
        conn, df, table_name='GPS_PINGS', auto_create_table=True
    )
    
    if success:
        print(f"✅ Successfully uploaded {nrows:,} rows in {nchunks} chunks!")
        print(f"⏱️ Upload time: {round(time.time() - start_time, 2)} seconds.")
    else:
        print("❌ Upload failed.")
    
    cursor.close()
    conn.close()

if __name__ == "__main__":
    upload_to_snowflake()