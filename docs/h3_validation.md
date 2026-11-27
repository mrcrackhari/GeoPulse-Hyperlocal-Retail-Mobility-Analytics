# H3 Hexbin Validation — Member 2 (Big Data / Spatial Engineer)

Owner: Member 2 (`member-2-Data-Analysis-and-SQL`)  
Output contract: `data/processed/h3_hexbins/` → `RAW.H3_HEXBINS` (Contract 9.4)

---

## 1. H3 Design & Configuration

- **Resolution:** `8` (avg hex area ≈ `0.74 km²`, edge ≈ `0.46 km` — closely matches the 500 m store catchment area of `0.78 km²`)
- **Coordinate Order Guard:** `h3.geo_to_h3(lat, lon, 8)` takes **latitude first** (guarded by `tests/test_h3.py::test_known_latlon_to_h3_index_and_lat_first_guard`)
- **Schema (Contract 9.4):** `hex_id string, resolution int, hour_of_day int, day_type string, ping_count bigint, unique_devices bigint, center_lat double, center_lon double`
- **Grain:** `(hex_id, hour_of_day, day_type)` where `day_type ∈ {'weekday', 'weekend'}`

---

## 2. Full 10M-Ping Run Results (Day 12)

Command:
```bash
python -m src.spatial.hexagon_aggregation --in data/raw/gps_pings.parquet --out data/processed/h3_hexbins --res 8 --driver-memory 4g --audit