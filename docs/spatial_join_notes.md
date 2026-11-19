# Spatial join notes — Member 2 (Big Data / Spatial Engineer)

Living document. Owner: Member 2 (`member-2`). Sections marked **TODO (Day n)** are filled in on that day.

---

## 1. Environment setup (Day 1)

### 1.1 Pinned versions (do not change — they must match each other)

| Component | Version | Why |
|---|---|---|
| Python | 3.10 (3.8 – 3.11 work for PySpark 3.4) | PySpark 3.4.1 does not support Python 3.12+ |
| Java | 17 | Spark 3.4 runs on Java 8 / 11 / 17; the plan standardises on 17 |
| `pyspark` | 3.4.1 | Sedona 1.5.1 is built for Spark 3.4 |
| `apache-sedona` | 1.5.1 | Python API; the jars are pulled by Spark at start-up |
| Spark package | `org.apache.sedona:sedona-spark-shaded-3.4_2.12:1.5.1` | Sedona SQL functions (`ST_*`) |
| Spark package | `org.datasyslab:geotools-wrapper:1.5.1-28.2` | GeoTools classes Sedona needs |
| `numpy` | `<2` | PySpark 3.4 / pandas 1.5–2.0 era wheels are built against NumPy 1.x |
| `h3` | 3.7.7 | v3 API: `h3.geo_to_h3(lat, lng, res)` — **latitude first** |
| `shapely`, `pyproj`, `scipy`, `pandas`, `pyarrow`, `pytest` | see `requirements.txt` (Member 1) | |

### 1.2 Install (once, from the repo root)

```bash
java -version                      # must say 17.x   (macOS: brew install openjdk@17 ; Ubuntu: sudo apt install openjdk-17-jdk)
python3.10 -m venv .venv
source .venv/bin/activate          # Windows: .venv\Scripts\activate
python -m pip install --upgrade pip
pip install "pyspark==3.4.1" "apache-sedona==1.5.1" "numpy<2" "shapely>=2.0" "pyproj>=3.4" \
            scipy "pandas>=1.5,<2.1" "pyarrow>=12,<15" "h3==3.7.7" pytest python-dotenv
```

If Member 1's `requirements.txt` already exists on `main`, use `pip install -r requirements.txt` instead and
tell Member 1 if any pin does not install.

### 1.3 Smoke test — the Day 1 "Done when"

```bash
python -m src.spatial.sedona_config
```

Expected output (the first run downloads the two jars from Maven Central, so it needs internet and takes
a minute or two; later runs take a few seconds):

```
python            : 3.10.x
pyspark (pip)     : 3.4.1  (expected 3.4.1)
apache-sedona(pip): 1.5.1  (expected 1.5.1)
Spark (running)   : 3.4.1
Java (running)    : 17.x.x
JAVA_HOME         : <your Java 17 folder>
driver memory     : 4g
----------------------------------------------------------------------
ST_Contains(polygon, ST_Point(lon, lat)):
  P1_STORE_A_CENTER        lat=12.9716 lon=77.5946  expected=True  got=True  ok
  P2_INSIDE_BOX            lat=12.9725 lon=77.5960  expected=True  got=True  ok
  P3_OUTSIDE_NORTH_EAST    lat=13.0358 lon=77.6408  expected=False got=False ok
  Store A centre as ST_Point(lat, lon) (WRONG order) -> False  (must be False)
----------------------------------------------------------------------
SMOKE TEST: PASS
```

The smoke test uses real GeoPulse coordinates on purpose: if anyone writes `ST_Point(lat, lon)` the points that
should be inside come back `False`, so a lon/lat mix-up fails here on Day 1 instead of silently corrupting the
join later. The last line shows that wrong-order result deliberately (it is the viva answer to "how do you guard lon/lat order?").

Then run the tests: `pytest tests/test_sedona_config.py -q` (the real-Spark test is skipped automatically
if PySpark / Java are missing; the configuration tests always run).

### 1.4 Configuration summary (`src/spatial/sedona_config.py`)

`get_sedona(app_name="geopulse", driver_memory=None, shuffle_partitions=None, extra_conf=None)` returns a
`SedonaContext` session with:

| Setting | Value |
|---|---|
| master | `local[*]` |
| `spark.driver.memory` | argument > `SPARK_DRIVER_MEMORY` env var > `4g` |
| `spark.serializer` | `org.apache.spark.serializer.KryoSerializer` |
| `spark.kryo.registrator` | `org.apache.sedona.core.serde.SedonaKryoRegistrator` |
| `spark.sql.adaptive.enabled` | `true` |
| `spark.sql.shuffle.partitions` | `64` (tuned on Day 7) |
| `spark.sql.session.timeZone` | `UTC` — our pings are city-local wall-clock time with no timezone (rule G2), so Spark must never shift them |
| `spark.jars.packages` | the two packages in 1.1 |

Before the JVM starts, `configure_environment()` also:

* **picks Java 17** if it is installed but not the active one (Windows `Program Files\...\jdk-17*`, Linux `/usr/lib/jvm`,
  macOS / Homebrew). It judges the JDK folder name only, so `jdk-11.0.17`, `jdk1.8.0_171` or a user folder called `dev17`
  are not mistaken for Java 17. Only the running Python process is changed, never your system;
* sets `PYSPARK_PYTHON` / `PYSPARK_DRIVER_PYTHON` to the running interpreter (fixes "python3 not found" worker errors on Windows);
* prints a warning on Windows if `HADOOP_HOME` is missing.

The session is created the way Sedona 1.5.1 documents it: `SedonaContext.builder()...getOrCreate()` first, then
`SedonaContext.create(spark)`. `create` needs a real SparkSession; passing the un-built builder fails.

Driver memory only takes effect on the **first** `get_sedona()` call in a Python process (the JVM starts once).

### 1.5 Known issues & fixes

| Symptom | Fix |
|---|---|
| `JAVA_HOME is not set` / `Unsupported class file major version` | Spark 3.4 does not run on Java 21+. Install Java 17 (the config finds it automatically in the usual folders) or point `JAVA_HOME` at it; check `java -version` |
| `'Builder' object has no attribute 'sql'` | `SedonaContext.create()` was given the builder instead of a session. Use `get_sedona()` (calls `getOrCreate()` first) |
| `ModuleNotFoundError: sedona` | You are not in the venv, or `apache-sedona` is not installed |
| `ClassNotFoundException … SedonaKryoRegistrator` or `ST_Point not found` | The jars were not downloaded. Check internet / proxy and re-run; keep Spark 3.4.x with Sedona 1.5.1 |
| `A module that was compiled using NumPy 1.x cannot be run in NumPy 2` | `pip install "numpy<2"` |
| Windows: `winutils.exe` / `HADOOP_HOME` warnings or write errors | Install a matching winutils build and set `HADOOP_HOME`; or run the project in WSL2 (recommended) |
| Port 4040 busy | Harmless: Spark uses 4041. The Spark UI is where the Day 9 screenshot comes from |

**TODO (Day 1, your machine):** write the combination that actually worked for you here (OS, Python, Java,
`pip freeze | grep -i -E "spark|sedona|numpy|h3|shapely"`), and message Member 1 if `requirements.txt` needs a fix.

| Field | Your value |
|---|---|
| OS / RAM / cores | |
| Python | |
| Java | |
| pyspark / apache-sedona | |
| Smoke test result | |

---

## 2. Store seed data (Day 2)

### 2.1 What was generated

`python -m src.spatial.generate_stores` writes three small, deterministic files into `data/seed/` (committed to git):

| File | Contents |
|---|---|
| `stores.csv` | 5,000 rows: `store_id, store_name, center_lat, center_lon, radius_m, zone_id, zone_type, catchment_wkt` (contract 9.2) |
| `store_polygons.geojson` | the same 5,000 catchment polygons; all other columns as properties |
| `zoning.geojson` | 10 × 10 zoning grid `Z00`…`Z99`; properties `zone_id, zone_type, zone_wkt` (contract 9.5) |

### 2.2 Rules implemented

* **Store A / Store B.** `S0001` = "Store A (existing)" at (12.9716, 77.5946); `S0002` = "Store B (proposed)" at
  (12.9716, 77.6011). Both exactly as in the plan. Every other store is named `Store S0003`, `Store S0004`, …
* **Other 4,998 stores.** 60 % from Gaussians (σ = 0.06°) around the three hubs, 40 % uniform in the bounding box;
  rejected if outside the box, closer than 600 m to any other store, or closer than 1.5 km to S0001 / S0002.
  A 1.5 km exclusion means no other 500 m catchment can touch the planted pair — the 4,000 / 2,000 / 600 device
  ground truth stays clean.
* **Catchments.** 500 m radius circle, built as a 32-sided polygon (`quad_segs = 8`) in EPSG:32643 (UTM 43N), then
  reprojected to EPSG:4326; exterior ring counter-clockwise and closed; coordinates written to 7 decimals (≈ 1 cm).
* **Zoning.** 10 × 10 equal cells over the city box; zone type by distance from the cell centre to the nearest hub
  (< 8 km: 70 % commercial / 30 % mixed; 8–20 km: 50 % mixed / 50 % residential; > 20 km: 70 % residential /
  30 % industrial). Because the city box is ~100 km wide, most cells are > 20 km from a hub:
  70 residential, 21 industrial, 6 mixed, 3 commercial (that is what the rule gives; both stores sit in `Z56`, `commercial`).
* **Determinism.** Fixed `SEED = 42`, PCG64 generator, normals built from uniform draws (Box–Muller) so the output does
  not depend on numpy's `normal()` implementation. Two runs are byte-identical (tested).
* **No GIS libraries needed to regenerate.** The EPSG:4326 ↔ EPSG:32643 transform is implemented in the module
  (Krüger series). It is cross-checked in `tests/test_stores.py` against an independent Snyder (USGS) formula
  to 2 cm, round-trips to < 0.1 mm, and against `pyproj` when installed.

### 2.3 Results (seed 42)

| Check | Result | Rule |
|---|---|---|
| Rows / unique ids | 5,000 / 5,000 | 5,000 |
| Catchment area | 0.7804 km² (every polygon within 1 % of 0.78 km²) | ≈ 0.78 km² ± 1 % |
| Vertices from centre | 500 m ± 0.05 m | 500 m |
| Min distance between two stores | 600.01 m | ≥ 600 m |
| Nearest other store to S0001 / S0002 | 1,502.5 m / 1,516.0 m | ≥ 1.5 km |
| Store A ↔ Store B | **705.6 m** (circles overlap in a ~294 m lens) | "≈ 700 m" |
| Centres inside the city box | all | all |

### 2.4 Assumptions / deviations to know about

1. **A ↔ B is 705.6 m, not 700 m.** The plan states "≈ 700 m" but also fixes both coordinates; those coordinates are
   0.0065° of longitude apart, which is 705.2 m on the ground (705.6 m in UTM grid metres). I kept the coordinates
   exactly as specified (other members' docs quote them) and test `700 ± 10 m`. The overlap lens is ~294 m wide instead of 300 m — no effect on the plan.
2. **"Metres" = EPSG:32643 grid metres** for the 600 m / 1.5 km / 500 m rules. Inside the city, grid metres are 0.02 – 0.09 %
   longer than ground metres (tested over the whole city box), so a "600 m" gap is ≥ 599.4 m on the ground and a 500 m circle has a ground radius of ≥ 499.5 m. Irrelevant for the analysis; recorded so nobody is surprised.
3. **Zone ids are `Z{row}{col}`** with row 0 = southernmost band and col 0 = westernmost band (so `Z00` is the south-west cell, `Z99` the north-east).
4. **The catchment is a 32-gon, not a perfect circle** (plan decision G3): area 0.7804 km² vs 0.7854 km² for a true circle (0.6 % less).
5. These notes are the place for assumptions until Member 1's `docs/assumptions.md` exists; copy items 1–2 there (append-only) when it does.

### 2.5 How to verify (Day 2 "Done when")

```bash
python -m src.spatial.generate_stores        # regenerate (about 2 s); git diff data/seed must be empty
pytest tests/test_stores.py -q               # 31 tests (shapely / pyproj ones skip if not installed)
```

Contract consumers: Member 1's simulator reads `data/seed/stores.csv` (planted cohorts around S0001 / S0002 and background store visits);
Member 3's frontend reads the polygons through the exports.

---

## 3. Spatial join design — TODO (Day 4 / Day 5)

Output contract confirmed with Members 1 & 3, partition / broadcast plan, Sedona-vs-shapely fixture result.

## 4. Full-scale run (10 M × 5,000) — TODO (Day 6)

Runtime, driver memory, rows in / out.

## 5. Memory audit — TODO (Day 7)

| Run | Shuffle partitions | `--by-day` | Broadcast hint | Driver memory | Runtime | Peak memory | OOM? |
|---|---|---|---|---|---|---|---|
| 1 | | | | | | | |
| 2 | | | | | | | |
| 3 | | | | | | | |

## 6. Test results — TODO (Day 9)

Spark UI screenshot: `docs/screenshots/` (placeholder).

---

## Day 3 - Input validation and lon/lat-order smoke join

**Files:** `src/spatial/validate_inputs.py`, `tests/test_spatial_join.py`

**Run**
```
python -m src.spatial.validate_inputs --stores data\seed\stores.csv
python -m src.spatial.validate_inputs --stores data\seed\stores.csv --pings <pings file> --smoke-join
pytest tests\test_spatial_join.py -q
```
Exit code 0 = all PASS, 1 = at least one FAIL.

**What is validated**
- Stores (18 checks): columns, blanks, unique `S####` ids, centres numeric / in box / not swapped, radius 500, zone fields, WKT parses, ring closed, WKT is (lon lat), vertices within box + 0.006 deg, area > 0, CCW, no self-crossing, centre inside own polygon.
- Pings (9 checks, chunked): columns, nulls, numeric, inside city box, not swapped, timestamps parse / naive / in window, `D######` ids.
- Smoke join: 100,000 pings x 5,000 polygons, broadcast `ST_Intersects`, compared with a numpy brute force; six planted points with known answers (A centre -> S0001, B centre -> S0002, lens -> both, 420 m west of A -> S0001, 420 m east of B -> S0002, 600 m north of A -> none).

**Rule:** `ST_Point(lon, lat)` - longitude first. `h3.geo_to_h3(lat, lon, res)` - latitude first. Store A's centre is inside S0001 only with the first order.

**Deviation:** edge-store circles overhang the city box by up to ~500 m, so the polygon-in-box check allows a 0.006 deg margin (centres must still be strictly inside the box).

**Results (fill in after running on your machine):** validation ___/18 PASS; smoke join ___ s; tests ___ passed.
