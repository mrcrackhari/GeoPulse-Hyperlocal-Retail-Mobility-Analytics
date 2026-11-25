# Spatial Join Notes — Member 2 (Big Data / Spatial Engineer)

Mid-Review technical documentation for GeoPulse spatial engineering.
Owner: Member 2 (`member-2-Data-Analysis-and-SQL`).

---

## 1. Environment & Spark/Sedona Config (Day 1)

### Locked Stack
| Component | Version | Notes |
|---|---|---|
| Python | 3.10.11 | Required for PySpark 3.4.1 |
| Java | OpenJDK 17 (`jdk-17.0.21.9-hotspot`) | Auto-selected by `sedona_config.py` over Java 21+ |
| PySpark / Sedona | `pyspark==3.4.1`, `apache-sedona==1.5.1` | Matched JARs: `sedona-spark-shaded-3.4_2.12:1.5.1`, `geotools-wrapper:1.5.1-28.2` |
| Geo & Data libs | `shapely>=2.0`, `pyproj>=3.4`, `scipy`, `h3==3.7.7`, `numpy<2`, `pandas`, `pyarrow`, `pytest`, `psutil`, `ipython` | `ipython` needed by Sedona 1.5.1; `psutil` needed for RSS memory audit |
| Windows Hadoop Helper | `HADOOP_HOME=C:\hadoop`, `SPARK_LOCAL_IP=127.0.0.1` | Provides `winutils.exe` for Spark Parquet writes on Windows |

### Session Settings (`src/spatial/sedona_config.py`)
- `master`: `local[*]`
- `spark.driver.memory`: `4g` (default, overridable via `--driver-memory` or `SPARK_DRIVER_MEMORY`)
- `spark.serializer`: `org.apache.spark.serializer.KryoSerializer`
- `spark.kryo.registrator`: `org.apache.sedona.core.serde.SedonaKryoRegistrator`
- `spark.sql.adaptive.enabled`: `true`
- `spark.sql.shuffle.partitions`: `64`
- `spark.sql.session.timeZone`: `UTC` (prevents shifting naive city-local timestamps)
- Day 1 smoke test (`python -m src.spatial.sedona_config`): **PASS** (`ST_Point(lon, lat)` matches inside points; swapped `ST_Point(lat, lon)` returns `False`).

---

## 2. Store Seed Data (Day 2)

Generated deterministically (`SEED = 42`) via `python -m src.spatial.generate_stores`:
- `data/seed/stores.csv` — 5,000 stores (`store_id, store_name, center_lat, center_lon, radius_m, zone_id, zone_type, catchment_wkt`)
- `data/seed/store_polygons.geojson` — 5,000 catchment polygons (500 m radius, 32-gon buffered in EPSG:32643 and reprojected to EPSG:4326)
- `data/seed/zoning.geojson` — 10 × 10 zoning grid (`Z00`–`Z99`)

| Check | Result | Rule |
|---|---|---|
| Total / unique stores | 5,000 / 5,000 | 5,000 |
| Catchment area | 0.7804 km² | ≈ 0.78 km² ± 1% |
| Min distance between stores | 600.01 m | ≥ 600 m |
| Nearest other store to S0001 / S0002 | 1,502.5 m / 1,516.0 m | ≥ 1.5 km |
| Store A (S0001) ↔ Store B (S0002) | 705.6 m (~294 m overlap lens) | ≈ 700 m |
| Determinism | Byte-identical across runs | Verified |

---

## 3. Input Validation & Lon/Lat Order Guard (Day 3)

- `src/spatial/validate_inputs.py` validates stores (18 checks: schema, bbox, WKT validity, closed CCW rings, centre inside polygon) and pings (9 streaming checks: nulls, bbox, swap detection, naive timestamps in `2026-01-01..2026-01-31`, `D######` IDs).
- **Coordinate order rule:** `ST_Point(lon, lat)` takes **longitude first** in Sedona/Snowflake; `h3.geo_to_h3(lat, lon, res)` takes **latitude first** in H3.
- 100k-ping smoke join against all 5,000 stores matched NumPy brute force and confirmed swapped `ST_Point(lat, lon)` is caught (`False`).

---

## 4. Spatial Join Design, Contract & Shapely Verification (Days 4–5)

### Output Contract Confirmed (Contract 9.3 — `PING_STORE_MATCHES`)
`src/spatial/spatial_join.py` writes `data/processed/ping_store_matches/` with exact columns:
`device_id string, store_id string, ts timestamp, lat double, lon double`

- **Join strategy:** `ST_Intersects(s.geom, p.geom)` with `/*+ BROADCAST(s) */` on the 5,000 store polygons so the 10M pings are never shuffled.
- **Memory safety:** Pings are never `collect()`ed to the driver; output is written with `coalesce(16)`; `--by-day` joins one daily Parquet file at a time as an OOM fallback.
- **1M sample run (`demo_pings_1m.parquet`):** 1,000,006 pings × 5,000 stores → 399,998 matched rows in 5.8 s (`SPATIAL JOIN: PASS`).
- **Day 5 Sedona vs. Shapely fixture (`tests/test_spatial_join.py`):** 3 stores (`S0001`, `S0002`, `S0003`) × 200 pings (including lens points and exact polygon vertices). Sedona output matched Shapely brute-force `intersects` with zero differences.

---

## 5. Full-Scale Run (10M × 5,000) & Memory Audit (Days 6–7)

### Full-Scale Run (Day 6)
- **Input:** 9,999,996 pings across 30 daily Parquet files (stand-in dataset via `make_demo_pings`; ready to re-run on Member 1's simulator data)
- **Output:** 3,997,656 matched rows across all 5,000 stores (0 nulls, Contract 9.3 **PASS**)
- **Baseline performance (`4g` driver, 64 shuffle partitions, broadcast enabled):** **21.6 s** end-to-end (`16.7 s` join stage, **462,216 pings/s**), **2,583 MB** peak RSS, **no OOM**.

### 5-Run Memory Audit Table (Day 7)
Executed via `python -m src.spatial.memory_audit --pings data/raw/gps_pings.parquet --driver-memory 4g` (fresh Spark JVM per run):

| Run | Shuffle Partitions | `--by-day` | Broadcast Hint | Broadcast in Plan | Driver Memory | Runtime | Peak Memory (RSS) | OOM? |
|---:|---:|:---:|:---:|:---:|:---:|---|---:|:---:|
| 1 | 64 | no | yes | yes | 4g | 22 s (0.4 min) | 2,583 MB | no |
| 2 | 16 | no | yes | yes | 4g | 17 s (0.3 min) | 3,734 MB | no |
| 3 | 200 | no | yes | yes | 4g | 34 s (0.6 min) | 3,350 MB | no |
| 4 | 64 | yes | yes | yes | 4g | 94 s (1.6 min) | 2,411 MB | no |
| 5 | 64 | no | no | no | 4g | 54 s (0.9 min) | 4,799 MB | no |

**Conclusions & Locked Defaults:**
- All 5 runs produced the exact same **3,997,656** rows with zero OOMs.
- Dropping the `BROADCAST` hint (Run 5) made the job **2.5× slower** (54 s vs 22 s) and nearly doubled peak RAM (**4,799 MB** vs **2,583 MB**). Keeping `BROADCAST(s)` is mandatory.
- `--by-day` (Run 4) had the lowest peak memory (**2,411 MB**) and serves as the documented OOM fallback.
- **Locked defaults:** `DEFAULT_SHUFFLE_PARTITIONS = 64`, `by_day = False`, `BROADCAST(s)` enabled, `driver_memory = 4g`.

---

## 6. Catchment Analysis (Day 8)

- `src/spatial/catchment_analysis.py` streams `data/processed/ping_store_matches/` via PyArrow, writes `data/processed/catchment_summary.csv` (`store_id, unique_devices, ping_count, peak_hour`), and asserts the Section 3.4 planted ground truth (`S0001 = 4,000`, `S0002 = 2,000`, `shared = 600`, `S0002→S0001 = 30.0% ± 3%`, `S0001→S0002 = 15.0% ± 3%`).
- Verified **8/8 PASS** on the planted contract mock (`sample_planted_matches.parquet`) and generated the 5,000-store `catchment_summary.csv` on the full stand-in join output.

---

## 7. Passing Test Output & Spark UI Placeholder (Day 9)

Command: `pytest tests -q`

| Test File | What It Tests | Status |
|---|---|---|
| `tests/test_sedona_config.py` | Spark/Sedona config, Java 17 auto-select, UTC timezone, live `ST_Contains` smoke test | PASS (45) |
| `tests/test_stores.py` | 5,000 stores, EPSG:32643 accuracy, 0.78 km² area, 600 m / 1.5 km rules, determinism | PASS (31) |
| `tests/test_spatial_join.py` | Input validation, lon/lat guard, 100k smoke join, Sedona vs. Shapely brute force | PASS (26) |
| `tests/test_join_contract.py` | Contract 9.3 schema, broadcast plan, `--by-day` idempotency, fail-fast CLI, audit runner | PASS (27) |
| `tests/test_catchment_analysis.py` | Catchment CSV schema, peak-hour tie-breaking, 4,000/2,000/600 ground-truth checks | PASS (7) |
| **Total** | **Full Member 2 Test Suite** | **136 passed** |

### Spark UI Screenshot Placeholder
![alt text](image.png)