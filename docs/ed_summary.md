# GeoPulse — Exploratory Data & Mobility Summary

**Project:** GeoPulse: Hyperlocal Retail Mobility Analytics  
**Author:** Rishi Srivastava (Visualization Lead / Member 3)  
**Branch:** `member-3-Dashboard-and-Visualization`  
**Tech Stack:** Snowflake (`GEOPULSE_DB`, `GEOPULSE_WH`), dbt Core, Python 3.12 (`venv`), Streamlit, Pandas, NumPy

## 1. Data & Pipeline Readiness
- Configured Snowflake database schemas (`RAW`, `STAGING`, `MARTS`) and dbt staging views `RAW.stg_gps_pings` and `RAW.stg_stores`.
- Built `src/dashboard/data_loader.py` with live Snowflake staging connectivity, column normalization, and an automatic fallback profile layer so the frontend remains testable before full mart tables are populated.

## 2. Temporal Mobility Findings (Weekday vs. Weekend)
- **Weekday Pattern:** Displays a morning commuter rise followed by a sharper evening retail peak between `17:00` and `21:00` (highest volume around `18:00–19:00`).
- **Weekend Pattern:** Starts quieter in the early morning, builds stronger midday and afternoon footfall, and sustains higher overall ping volume across commercial zones.

## 3. Store Footfall, Dwell & Engagement Insights
- **Dwell Segmentation:** Stores are classified into `Quick Stop (<15m)`, `Browsing (15-22m)`, and `High Engagement (>22m)`.
- **Engagement Leaders:** High-footfall flagship and hub locations with longer dwell times achieve the highest `engagement_index`, whereas transit-oriented stores show shorter dwell durations despite steady traffic.
- **Footfall Share:** Added `footfall_share_pct` and store-vs-zone baseline benchmarking to isolate over-performing stores within each retail zone.

## 4. Spatial & Dashboard Deliverables
- Built the Streamlit analytics app (`src/dashboard/app.py`) with sidebar filters (Day Profile, Retail Zone, Store Location, Time Window, Minimum Dwell), dynamic KPI cards, Weekday vs. Weekend comparison, Geospatial Hotspot Map (`st.map`), Store Deep-Dive benchmarking, Zone Rollup, and clean merged CSV export (`geopulse_clean_merged_*.csv`).

## 5. Next Steps
- Connect final `GEOPULSE_DB.MARTS` models into `data_loader.py` once upstream transformations are completed and validate live row counts across all dashboard tabs.