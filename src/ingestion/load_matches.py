import snowflake.connector
import pandas as pd
import os
from snowflake.connector.pandas_tools import write_pandas
import time

# ==========================================
# ⚠️ FILL IN YOUR DETAILS HERE
# ==========================================
USER = 'HARIMAHAJAN108@GMAIL.COM'
PASSWORD = 'Harimahajan@1212'  # <-- Put your real password here
ACCOUNT = 'DAGCDID-WV73577'
WAREHOUSE = 'COMPUTE_WH'
DATABASE = 'GEOPULSE_DB'
SCHEMA = 'RAW'

def upload_matches():
    print("1. Connecting to Snowflake...")
    conn = snowflake.connector.connect(
        user=USER, password=PASSWORD, account=ACCOUNT,
        warehouse=WAREHOUSE, database=DATABASE, schema=SCHEMA
    )
    cursor = conn.cursor()
    cursor.execute(f"USE SCHEMA {DATABASE}.{SCHEMA}")
    
    # 2. Read the mock file
    file_path = os.path.join('..', '..', 'data', 'processed', 'ping_store_matches', 'part-00000.parquet')
    print(f"2. Reading mock matches file from {file_path}...")
    
    if not os.path.exists(file_path):
        print(f"❌ Error: File not found at {file_path}.")
        return

    df = pd.read_parquet(file_path)
    print(f"✅ Loaded {len(df):,} match rows into memory.")

    # 3. Drop old table and upload
    print("3. Dropping old PING_STORE_MATCHES table (if it exists)...")
    cursor.execute("DROP TABLE IF EXISTS PING_STORE_MATCHES;")

    print("4. Uploading matches to Snowflake...")
    success, nchunks, nrows, _ = write_pandas(
        conn, df, table_name='PING_STORE_MATCHES', auto_create_table=True
    )
    
    if success:
        print(f"✅ Successfully uploaded {nrows:,} rows!")
    else:
        print("❌ Upload failed.")
    
    cursor.close()
    conn.close()

if __name__ == "__main__":
    upload_matches()