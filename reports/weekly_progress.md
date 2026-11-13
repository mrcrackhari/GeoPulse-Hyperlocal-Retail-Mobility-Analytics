# GeoPulse Weekly Progress - Member A (Data Engineer)

## Day 1 (Nov 10, 2026)
- Set up local virtual environment and installed engineering libraries.
- Built `src/generate_gps_pings.py`.
- Successfully generated 10,000,000 rows and exported to `data/raw/gps_pings.parquet`.
- Added `.gitignore` to prevent pushing large data files.

## Day 2 (Nov 11, 2026)
- Created Snowflake free trial account and set up database (`GEOPULSE_DB`).
- Built `src/setup_database.py` to initialize the `RAW` schema via Python.
- Built `src/load_raw_data.py` using `snowflake-connector-python`.
- **Successfully uploaded all 10,000,000 GPS pings to Snowflake** (Took ~5.5 minutes).
- Pushed all code to GitHub (credentials removed for security).