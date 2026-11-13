import snowflake.connector

# ⚠️ UPDATE YOUR CREDENTIALS HERE
USER = 'HARIMAHAJAN108@GMAIL.COM'
PASSWORD = 'Harimahajan@1212' # <-- Put your real password here
ACCOUNT = 'DAGCDID-WV73577'
WAREHOUSE = 'COMPUTE_WH'

print("Connecting to Snowflake to set up environment...")
try:
    conn = snowflake.connector.connect(
        user=USER, 
        password=PASSWORD, 
        account=ACCOUNT, 
        warehouse=WAREHOUSE
    )
    cursor = conn.cursor()

    # 1. Create and use Database
    cursor.execute("CREATE DATABASE IF NOT EXISTS GEOPULSE_DB;")
    cursor.execute("USE DATABASE GEOPULSE_DB;")
    
    # 2. Create and use Schema
    cursor.execute("CREATE SCHEMA IF NOT EXISTS RAW;")
    cursor.execute("USE SCHEMA RAW;")

    print("✅ Database 'GEOPULSE_DB' and Schema 'RAW' created and verified successfully!")
    
    cursor.close()
    conn.close()

except Exception as e:
    print(f"❌ Setup failed: {e}")