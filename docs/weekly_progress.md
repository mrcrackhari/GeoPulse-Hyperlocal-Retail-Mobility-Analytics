# GeoPulse Weekly Progress - Member 1 (Data Engineer + Team Lead)

## Day 1 (Nov 10, 2026)
- Set up local virtual environment and installed engineering libraries (pandas, pyarrow, faker).
- Built `src/simulator/generate_gps_pings.py`.
- Successfully generated 10,000,000 rows and exported to `data/raw/gps_pings.parquet`.
- Added `.gitignore` to prevent pushing large data files.

## Day 2 (Nov 11, 2026)
- Created Snowflake free trial account and set up database (`GEOPULSE_DB`).
- Built `src/ingestion/setup_database.py` to initialize the `RAW` schema via Python.
- Built `src/ingestion/load_raw_data.py` using `snowflake-connector-python`.
- **Successfully uploaded all 10,000,000 GPS pings to Snowflake** (Took ~5.5 minutes).

## Day 3 (Nov 12, 2026)
- Restructured repo folders to match the 20-day team plan (`src/simulator`, `src/ingestion`, etc.).
- Renamed branch to `member-1`.
- Built `src/simulator/city_movement_model.py` to generate planted device cohorts (30% cannibalization rule).
- Updated `generate_gps_pings.py` to combine planted cohorts with background noise.
- Successfully regenerated the 10M `gps_pings.parquet` file.

## Day 4-5 (Nov 13-14, 2026) - CHECKPOINT 1
- Re-uploaded the new 10M planted pings to Snowflake (dropped old table first).
- Fixed Git history to remove large parquet files from previous commits.
- Cleaned up broken branch (`member-1-Data-Collection-and-Cleaning`) and locked in `member-1`.
- Created `docs/work_distribution.md`.

## Day 6 (Nov 15, 2026)
- Installed `dbt-snowflake` and configured `dbt_project.yml` and `profiles.yml`.
- Created `sources.yml` pointing to the `GPS_PINGS` raw table in Snowflake.
- Built staging model `stg_gps_pings.sql`.
- Successfully ran `dbt run` (created view in Snowflake).

## Day 7 (Nov 16, 2026)
- Created custom SQL test `assert_no_nulls_in_gps.sql` to bypass Snowflake case-sensitivity errors.
- Ran `dbt test` - **PASS=1**. Data quality confirmed.
- Pushed Day 6 & 7 work to GitHub.

## Day 8 (Nov 18, 2026)
- Generated mock spatial matches (`ping_store_matches.parquet`) to unblock the pipeline (since Member 2 was delayed).
- Built `src/ingestion/load_matches.py` and uploaded **100,000 mock rows** to Snowflake (`PING_STORE_MATCHES` table).

## Day 9 (Nov 19, 2026)
- Created `stg_ping_store_matches.sql` staging model.
- Ran `dbt run` and `dbt test` end-to-end. **All tests passed.**
- **Note:** Member 2 & 3 have not pushed any code to GitHub yet. Monitoring team status as Team Lead.

## Day 10 (Nov 20, 2026)
- **Mid-Review Preparation:** Consolidated all progress notes and prepared the repository for the Mid-Project Review.
- Reviewed team status (Member 2 & 3 still pending).
- Pushed final documentation to GitHub to satisfy the 20-day commit discipline.