"""Input validation for GeoPulse spatial work (Member 2, Day 3).

Checks the two inputs of the spatial join BEFORE any Spark job runs, and prints PASS / FAIL per
check. Exit code 0 = everything passed, 1 = at least one FAIL (so Airflow / CI can stop).

Usage (from the repository root)
--------------------------------
    python -m src.spatial.validate_inputs                                   # stores only
    python -m src.spatial.validate_inputs --pings data/raw/gps_pings_sample.parquet
    python -m src.spatial.validate_inputs --pings data/raw/gps_pings.parquet --max-rows 1000000
    python -m src.spatial.validate_inputs --pings data/raw/gps_pings_sample.parquet --smoke-join

What is checked
---------------
Pings (parquet file, folder of parquet files, or csv; read in chunks, never all in memory)
  required columns, no nulls, numeric lat/lon, inside the city box, NOT swapped (lat holding a
  longitude), timestamps parse / fall in the data window / carry no timezone, device_id format.
Stores (data/seed/stores.csv, contract 9.2)
  required columns, no empty values, unique ids, centres numeric and inside the box and not
  swapped, radius 500, zone fields, WKT parses as one POLYGON ring with lon-first coordinates,
  ring closed, counter-clockwise, simple (no self-crossing; plus shapely.is_valid when installed),
  vertices inside the box, centre inside its own polygon.
Smoke join (--smoke-join, needs Spark / Sedona / Java 17)
  joins 100,000 pings x 5,000 store polygons with a broadcast ST_Intersects and checks the answer
  against a plain numpy brute force for S0001 / S0002. Store A's centre must land inside S0001
  with ST_Point(lon, lat), and must NOT with the swapped ST_Point(lat, lon).

The most common bug in this project is a latitude / longitude mix-up:
    ST_Point(lon, lat)   <- Sedona / Snowflake take LONGITUDE first
    h3.geo_to_h3(lat, lon, res)   <- H3 takes LATITUDE first
"""
from __future__ import annotations

import argparse
import logging
import re
import shutil
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterator, List, Optional, Sequence, Set, Tuple

import numpy as np
import pandas as pd

LOG = logging.getLogger("geopulse.validate_inputs")

# --- Constants (kept local so the script also runs stand-alone; a test pins them to generate_stores) ---
MIN_LAT, MAX_LAT = 12.50, 13.40
MIN_LON, MAX_LON = 77.05, 77.95
TS_START = pd.Timestamp("2026-01-01 00:00:00")
TS_END = pd.Timestamp("2026-01-31 23:59:59.999999")      # same window as the dbt range test
PING_COLUMNS: Tuple[str, ...] = ("device_id", "lat", "lon", "ts")
STORE_COLUMNS: Tuple[str, ...] = ("store_id", "store_name", "center_lat", "center_lon",
                                  "radius_m", "zone_id", "zone_type", "catchment_wkt")
ZONE_TYPES = frozenset({"commercial", "mixed", "residential", "industrial"})
DEVICE_ID_RE = r"D\d{6}"
STORE_ID_RE = r"S\d{4}"
ZONE_ID_RE = r"Z\d\d"
DEFAULT_STORES = "data/seed/stores.csv"
SMOKE_ROWS = 100_000
POLY_MARGIN_DEG = 0.006        # 500 m radius ~ 0.0045 deg: edge stores overhang the box a little


@dataclass
class CheckResult:
    """One validation result; ``passed`` False means FAIL."""
    name: str
    passed: bool
    detail: str = ""

    def __str__(self) -> str:
        tag = "PASS" if self.passed else "FAIL"
        return f"[{tag}] {self.name}" + (f" - {self.detail}" if self.detail else "")


def _res(name: str, ok: bool, ok_detail: str = "", fail_detail: str = "") -> CheckResult:
    return CheckResult(name, bool(ok), ok_detail if ok else fail_detail)


# =========================================================================================
# Geometry helpers (pure numpy)
# =========================================================================================
_WKT_RE = re.compile(r"^\s*POLYGON\s*\(\(\s*([^()]+?)\s*\)\)\s*$", re.IGNORECASE)


def parse_wkt_polygon(wkt: str) -> Optional[np.ndarray]:
    """Parse ``POLYGON((x y, x y, ...))`` (one ring, no holes) into an (n, 2) float array, else None."""
    if not isinstance(wkt, str):
        return None
    m = _WKT_RE.match(wkt)
    if not m:
        return None
    try:
        pts = [[float(v) for v in pair.split()] for pair in m.group(1).split(",")]
        arr = np.array(pts, dtype=float)
    except ValueError:
        return None
    if arr.ndim != 2 or arr.shape[1] != 2:
        return None
    return arr


def signed_area(ring: np.ndarray) -> float:
    """Shoelace area; > 0 means counter-clockwise."""
    x, y = ring[:-1, 0], ring[:-1, 1]
    return float(0.5 * np.sum(x * np.roll(y, -1) - np.roll(x, -1) * y))


def ring_is_simple(ring: np.ndarray) -> bool:
    """True when no two edges properly cross (detects bow-ties / self-intersections)."""
    p, q = ring[:-1], ring[1:]

    def orient(a, b, c):
        return (b[..., 0] - a[..., 0]) * (c[..., 1] - a[..., 1]) - (b[..., 1] - a[..., 1]) * (c[..., 0] - a[..., 0])

    pi, qi, pj, qj = p[:, None], q[:, None], p[None, :], q[None, :]
    o1, o2 = orient(pi, qi, pj), orient(pi, qi, qj)
    o3, o4 = orient(pj, qj, pi), orient(pj, qj, qi)
    return not bool(np.any((o1 * o2 < 0) & (o3 * o4 < 0)))


def point_in_ring(x: float, y: float, ring: np.ndarray) -> bool:
    """Even-odd ray casting; x = longitude, y = latitude."""
    x1, y1, x2, y2 = ring[:-1, 0], ring[:-1, 1], ring[1:, 0], ring[1:, 1]
    crosses = (y1 > y) != (y2 > y)
    denom = np.where(crosses, y2 - y1, 1.0)
    x_at_y = (x2 - x1) * (y - y1) / denom + x1
    return int(np.sum(crosses & (x < x_at_y))) % 2 == 1


def points_in_convex_ccw(points_xy: np.ndarray, ring: np.ndarray) -> np.ndarray:
    """Boolean mask: which points (N, 2) = (lon, lat) are strictly inside a convex counter-clockwise ring."""
    v = ring[:-1]
    e = np.roll(v, -1, axis=0) - v
    dx = points_xy[:, 0:1] - v[None, :, 0]
    dy = points_xy[:, 1:2] - v[None, :, 1]
    cross = e[None, :, 0] * dy - e[None, :, 1] * dx
    return np.all(cross > 0, axis=1)


def _shapely_is_valid(wkt: str) -> Optional[bool]:
    """shapely's verdict when shapely is installed, else None."""
    try:
        from shapely import wkt as shapely_wkt  # type: ignore
    except ImportError:
        return None
    try:
        return bool(shapely_wkt.loads(wkt).is_valid)
    except Exception:  # noqa: BLE001 - unparseable counts as invalid
        return False


def _first(ids: Sequence, n: int = 5) -> str:
    ids = list(ids)
    shown = ", ".join(str(i) for i in ids[:n])
    return f"{shown}{' ...' if len(ids) > n else ''} ({len(ids)} total)"


# =========================================================================================
# Pings
# =========================================================================================
def iter_ping_chunks(path, batch_rows: int = 500_000, max_rows: Optional[int] = None) -> Iterator[pd.DataFrame]:
    """Yield pandas chunks from a parquet file, a folder of *.parquet files, or a .csv file.

    ``max_rows`` stops after that many rows in total (used for "validate the 1M sample").
    """
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"{p} does not exist")
    if p.is_dir():
        files = sorted(p.glob("*.parquet")) or sorted(p.glob("*.csv"))
        if not files:
            raise FileNotFoundError(f"no *.parquet or *.csv files in {p}")
    else:
        files = [p]

    remaining = max_rows
    for f in files:
        if remaining is not None and remaining <= 0:
            return
        if f.suffix.lower() == ".csv":
            chunks = pd.read_csv(f, chunksize=batch_rows)
        elif f.suffix.lower() == ".parquet":
            try:
                import pyarrow.parquet as pq  # type: ignore
            except ImportError as exc:
                raise ImportError("reading parquet needs pyarrow:  pip install \"pyarrow>=12,<15\"") from exc
            pf = pq.ParquetFile(f)
            cols = [c for c in PING_COLUMNS if c in pf.schema_arrow.names] or None
            chunks = (b.to_pandas() for b in pf.iter_batches(batch_size=batch_rows, columns=cols))
        else:
            raise ValueError(f"unsupported file type: {f.suffix} (use .parquet or .csv)")
        for chunk in chunks:
            if remaining is not None:
                if remaining <= 0:
                    return
                chunk = chunk.iloc[:remaining]
                remaining -= len(chunk)
            if len(chunk):
                yield chunk


class PingStats:
    """Accumulates validation counters over chunks, so 10 M rows never sit in memory at once."""

    def __init__(self) -> None:
        self.rows = 0
        self.missing_cols: Set[str] = set()
        self.nulls: Dict[str, int] = {c: 0 for c in PING_COLUMNS}
        self.non_numeric = 0
        self.out_of_bbox = 0
        self.swapped = 0
        self.ts_unparseable = 0
        self.ts_out_of_window = 0
        self.ts_tz_aware = False
        self.ts_min: Optional[pd.Timestamp] = None
        self.ts_max: Optional[pd.Timestamp] = None
        self.bad_ids = 0
        self.lat_min = np.inf
        self.lat_max = -np.inf
        self.lon_min = np.inf
        self.lon_max = -np.inf

    def update(self, df: pd.DataFrame) -> None:
        self.rows += len(df)
        missing = [c for c in PING_COLUMNS if c not in df.columns]
        if missing:
            self.missing_cols.update(missing)
            return
        for c in PING_COLUMNS:
            self.nulls[c] += int(df[c].isna().sum())

        lat = pd.to_numeric(df["lat"], errors="coerce")
        lon = pd.to_numeric(df["lon"], errors="coerce")
        self.non_numeric += int((lat.isna() & df["lat"].notna()).sum() + (lon.isna() & df["lon"].notna()).sum())
        ok = lat.notna() & lon.notna()
        if ok.any():
            self.lat_min, self.lat_max = min(self.lat_min, float(lat[ok].min())), max(self.lat_max, float(lat[ok].max()))
            self.lon_min, self.lon_max = min(self.lon_min, float(lon[ok].min())), max(self.lon_max, float(lon[ok].max()))
        inside = lat.between(MIN_LAT, MAX_LAT) & lon.between(MIN_LON, MAX_LON)
        self.out_of_bbox += int((ok & ~inside).sum())
        swapped = lat.between(MIN_LON, MAX_LON) & lon.between(MIN_LAT, MAX_LAT)
        self.swapped += int((ok & swapped).sum())

        raw = df["ts"]
        try:
            ts = pd.to_datetime(raw, errors="coerce")
        except (ValueError, TypeError):
            ts = pd.Series(pd.NaT, index=raw.index)
        if getattr(ts.dtype, "tz", None) is not None:
            self.ts_tz_aware = True
            ts = ts.dt.tz_localize(None)
        self.ts_unparseable += int((ts.isna() & raw.notna()).sum())
        valid = ts.dropna()
        if len(valid):
            lo, hi = valid.min(), valid.max()
            self.ts_min = lo if self.ts_min is None else min(self.ts_min, lo)
            self.ts_max = hi if self.ts_max is None else max(self.ts_max, hi)
            self.ts_out_of_window += int(((valid < TS_START) | (valid > TS_END)).sum())

        ids = df["device_id"].dropna().astype(str)
        self.bad_ids += int((~ids.str.fullmatch(DEVICE_ID_RE)).sum())


def ping_checks(stats: PingStats) -> List[CheckResult]:
    """Turn the accumulated counters into PASS / FAIL results."""
    if stats.missing_cols:
        return [CheckResult("pings: required columns", False,
                            f"missing {sorted(stats.missing_cols)}; need {list(PING_COLUMNS)}")]
    out = [_res("pings: required columns", True, f"{list(PING_COLUMNS)} present"),
           _res("pings: has rows", stats.rows > 0, f"{stats.rows:,} rows checked", "file has 0 rows")]
    total_nulls = sum(stats.nulls.values())
    out.append(_res("pings: no null values", total_nulls == 0, "0 nulls",
                    f"nulls per column: { {c: n for c, n in stats.nulls.items() if n} }"))
    out.append(_res("pings: lat/lon are numbers", stats.non_numeric == 0, "all numeric",
                    f"{stats.non_numeric:,} non-numeric lat/lon values"))
    out.append(_res("pings: inside the city box", stats.out_of_bbox == 0,
                    f"lat {stats.lat_min:.4f}..{stats.lat_max:.4f}, lon {stats.lon_min:.4f}..{stats.lon_max:.4f}",
                    f"{stats.out_of_bbox:,} rows outside lat {MIN_LAT}-{MAX_LAT} / lon {MIN_LON}-{MAX_LON}"))
    out.append(_res("pings: lat/lon NOT swapped", stats.swapped == 0, "no row has a longitude in the lat column",
                    f"{stats.swapped:,} rows have lat~{MIN_LON}-{MAX_LON} and lon~{MIN_LAT}-{MAX_LAT}: "
                    "latitude and longitude are swapped"))
    out.append(_res("pings: timestamps parse", stats.ts_unparseable == 0, "all parse",
                    f"{stats.ts_unparseable:,} unparseable timestamps"))
    out.append(_res("pings: timestamps have no timezone", not stats.ts_tz_aware, "naive (city-local) time",
                    "ts carries a timezone; the plan stores naive city-local time (rule G2)"))
    window = f"{TS_START:%Y-%m-%d}..{TS_END:%Y-%m-%d}"
    span = f"{stats.ts_min} .. {stats.ts_max}" if stats.ts_min is not None else "no valid ts"
    out.append(_res("pings: timestamps in data window", stats.ts_out_of_window == 0, f"{span} (window {window})",
                    f"{stats.ts_out_of_window:,} timestamps outside {window}"))
    out.append(_res("pings: device_id format D######", stats.bad_ids == 0, "all match", f"{stats.bad_ids:,} bad ids"))
    return out


def validate_pings(path, max_rows: Optional[int] = None, batch_rows: int = 500_000) -> List[CheckResult]:
    """Run every ping check on a file or folder; returns the list of results."""
    stats = PingStats()
    t0 = time.time()
    for chunk in iter_ping_chunks(path, batch_rows=batch_rows, max_rows=max_rows):
        stats.update(chunk)
    LOG.info("checked %s ping rows in %.1fs", f"{stats.rows:,}", time.time() - t0)
    return ping_checks(stats)


# =========================================================================================
# Stores
# =========================================================================================
def load_stores(path) -> pd.DataFrame:
    """Read stores.csv with every column as text (numbers are parsed inside the checks)."""
    return pd.read_csv(path, dtype=str, keep_default_na=False, na_values=[""])


def check_stores(df: pd.DataFrame) -> List[CheckResult]:
    """All store checks (contract 9.2). Safe to call on broken data: it reports, it does not crash."""
    missing = [c for c in STORE_COLUMNS if c not in df.columns]
    if missing:
        return [CheckResult("stores: required columns", False, f"missing {missing}; need {list(STORE_COLUMNS)}")]
    out = [_res("stores: required columns", True, f"{len(STORE_COLUMNS)} columns present"),
           _res("stores: has rows", len(df) > 0, f"{len(df):,} stores", "file has 0 rows")]
    if len(df) == 0:
        return out

    empty = {c: int((df[c].isna() | (df[c].astype(str).str.strip() == "")).sum()) for c in STORE_COLUMNS}
    bad_cols = {c: n for c, n in empty.items() if n}
    out.append(_res("stores: no empty values", not bad_cols, "no nulls or blanks", f"empty values per column: {bad_cols}"))

    ids = df["store_id"].astype(str)
    dup = ids[ids.duplicated()].tolist()
    bad_fmt = ids[~ids.str.fullmatch(STORE_ID_RE)].tolist()
    out.append(_res("stores: store_id unique", not dup, f"{ids.nunique():,} unique ids", f"duplicates: {_first(dup)}"))
    out.append(_res("stores: store_id format S####", not bad_fmt, "all match", f"bad ids: {_first(bad_fmt)}"))

    lat = pd.to_numeric(df["center_lat"], errors="coerce")
    lon = pd.to_numeric(df["center_lon"], errors="coerce")
    nonnum = ids[lat.isna() | lon.isna()].tolist()
    out.append(_res("stores: centre lat/lon are numbers", not nonnum, "all numeric", f"non-numeric: {_first(nonnum)}"))
    inside = lat.between(MIN_LAT, MAX_LAT) & lon.between(MIN_LON, MAX_LON)
    outside = ids[~inside & lat.notna() & lon.notna()].tolist()
    out.append(_res("stores: centres inside the city box", not outside, "all inside", f"outside: {_first(outside)}"))
    swapped = ids[lat.between(MIN_LON, MAX_LON) & lon.between(MIN_LAT, MAX_LAT)].tolist()
    out.append(_res("stores: centre lat/lon NOT swapped", not swapped, "no swapped centres",
                    f"latitude/longitude swapped for: {_first(swapped)}"))

    radius = pd.to_numeric(df["radius_m"], errors="coerce")
    bad_r = ids[radius != 500].tolist()
    out.append(_res("stores: radius_m = 500", not bad_r, "all 500", f"not 500: {_first(bad_r)}"))
    bad_zone = ids[~df["zone_id"].astype(str).str.fullmatch(ZONE_ID_RE) | ~df["zone_type"].isin(ZONE_TYPES)].tolist()
    out.append(_res("stores: zone_id / zone_type valid", not bad_zone, "Z## ids and known types",
                    f"bad zone fields: {_first(bad_zone)}"))

    rings: Dict[str, np.ndarray] = {}
    unparsed: List[str] = []
    for sid, w in zip(ids, df["catchment_wkt"]):
        arr = parse_wkt_polygon(w)
        if arr is None:
            unparsed.append(sid)
        else:
            rings[sid] = arr
    out.append(_res("stores: catchment WKT parses (one POLYGON ring)", not unparsed, f"{len(rings):,} polygons parsed",
                    f"unparseable: {_first(unparsed)}"))

    open_ring = [s for s, r in rings.items() if len(r) < 4 or not np.array_equal(r[0], r[-1])]
    out.append(_res("stores: polygon rings closed (>= 4 points)", not open_ring, "all closed", f"open: {_first(open_ring)}"))
    good = {s: r for s, r in rings.items() if s not in set(open_ring)}

    lonlat_swapped = [s for s, r in good.items() if r[:, 0].max() < MIN_LON and r[:, 1].min() > 60]
    out.append(_res("stores: WKT is (lon lat), not (lat lon)", not lonlat_swapped, "x values are longitudes",
                    f"WKT coordinates look swapped for: {_first(lonlat_swapped)}"))
    m = POLY_MARGIN_DEG
    out_box = [s for s, r in good.items()
               if not (r[:, 0].min() >= MIN_LON - m and r[:, 0].max() <= MAX_LON + m
                       and r[:, 1].min() >= MIN_LAT - m and r[:, 1].max() <= MAX_LAT + m)]
    out.append(_res("stores: polygon vertices near the city box", not out_box,
                    f"all within box + {m} deg (a 500 m circle may overhang the edge slightly)",
                    f"far outside: {_first(out_box)}"))

    zero_area = [s for s, r in good.items() if abs(signed_area(r)) == 0.0]
    cw = [s for s, r in good.items() if signed_area(r) < 0]
    out.append(_res("stores: polygon area > 0", not zero_area, "all non-degenerate", f"zero area: {_first(zero_area)}"))
    out.append(_res("stores: exterior ring counter-clockwise", not cw, "all CCW", f"clockwise: {_first(cw)}"))

    not_simple = [s for s, r in good.items() if not ring_is_simple(r)]
    wkt_by_id = dict(zip(ids, df["catchment_wkt"]))
    shapely_bad = [s for s in good if _shapely_is_valid(wkt_by_id[s]) is False]
    invalid = sorted(set(not_simple) | set(shapely_bad))
    engine = "own check + shapely" if _shapely_is_valid("POINT(0 0)") is not None else "own check (shapely not installed)"
    out.append(_res("stores: polygons valid (no self-crossing)", not invalid, f"all valid [{engine}]",
                    f"invalid polygons: {_first(invalid)}"))

    centre_out: List[str] = []
    for sid, la, lo in zip(ids, lat, lon):
        r = good.get(sid)
        if r is not None and not np.isnan(la) and not np.isnan(lo) and not point_in_ring(float(lo), float(la), r):
            centre_out.append(sid)
    out.append(_res("stores: centre lies inside its own polygon", not centre_out, "all centres inside",
                    f"centre outside polygon: {_first(centre_out)}"))
    return out


def validate_stores(path=DEFAULT_STORES) -> List[CheckResult]:
    return check_stores(load_stores(path))


# =========================================================================================
# Smoke join: does ST_Point(lon, lat) put points in the right store? (needs Spark)
# =========================================================================================
PLANTED_EXPECTED: Dict[str, Set[str]] = {
    "PLANT_A_CENTER": {"S0001"},
    "PLANT_B_CENTER": {"S0002"},
    "PLANT_LENS_MID": {"S0001", "S0002"},      # the overlap lens: both catchments
    "PLANT_A_WEST_420": {"S0001"},             # inside A only
    "PLANT_B_EAST_420": {"S0002"},             # inside B only
    "PLANT_A_NORTH_600": set(),                # outside both
}


def _gs():
    try:
        from src.spatial import generate_stores as gs  # type: ignore
    except ImportError:  # running as a plain script from src/spatial
        import generate_stores as gs  # type: ignore
    return gs


def planted_points() -> Dict[str, Tuple[float, float]]:
    """name -> (lat, lon) of test points with known answers around Store A (S0001) and Store B (S0002)."""
    gs = _gs()
    _, _, a_lat, a_lon = gs.STORE_A
    _, _, b_lat, b_lon = gs.STORE_B
    ae, an = gs.lonlat_to_utm43n(a_lon, a_lat)
    be, bn = gs.lonlat_to_utm43n(b_lon, b_lat)

    def ll(e: float, n: float) -> Tuple[float, float]:
        lon, lat = gs.utm43n_to_lonlat(e, n)
        return round(float(lat), 6), round(float(lon), 6)

    return {
        "PLANT_A_CENTER": (a_lat, a_lon),
        "PLANT_B_CENTER": (b_lat, b_lon),
        "PLANT_LENS_MID": ll((ae + be) / 2, (an + bn) / 2),
        "PLANT_A_WEST_420": ll(ae - 420.0, an),
        "PLANT_B_EAST_420": ll(be + 420.0, bn),
        "PLANT_A_NORTH_600": ll(ae, an + 600.0),
    }


def expected_matches(points_lat_lon: np.ndarray, rings: Dict[str, np.ndarray]) -> Set[Tuple[int, str]]:
    """Brute force (numpy only): {(row_index, store_id)} for convex CCW rings. Points are (lat, lon) rows."""
    xy = np.column_stack([points_lat_lon[:, 1], points_lat_lon[:, 0]])      # x = lon, y = lat
    pairs: Set[Tuple[int, str]] = set()
    for sid, ring in rings.items():
        for i in np.flatnonzero(points_in_convex_ccw(xy, ring)):
            pairs.add((int(i), sid))
    return pairs


def build_smoke_points(n_rows: int = SMOKE_ROWS, pings_path=None, seed: int = 42):
    """Return (points (N, 2) as lat/lon, planted_index {name: row}, source description).

    Uses the first ``n_rows`` pings of ``pings_path`` when given, else uniform random points in the city
    box. The six planted points are always appended, so the answer is known whatever the data is.
    """
    if pings_path:
        parts = [c[["lat", "lon"]].to_numpy(dtype=float) for c in iter_ping_chunks(pings_path, max_rows=n_rows)]
        base = np.vstack(parts) if parts else np.empty((0, 2))
        source = f"first {len(base):,} rows of {pings_path}"
    else:
        rng = np.random.default_rng(seed)
        base = np.column_stack([MIN_LAT + (MAX_LAT - MIN_LAT) * rng.random(n_rows),
                                MIN_LON + (MAX_LON - MIN_LON) * rng.random(n_rows)])
        source = f"{n_rows:,} synthetic uniform points"
    plant = planted_points()
    index = {name: len(base) + k for k, name in enumerate(plant)}
    pts = np.vstack([base, np.array(list(plant.values()), dtype=float)])
    return pts, index, source


def run_smoke_join(sedona, stores_df: pd.DataFrame, n_rows: int = SMOKE_ROWS, pings_path=None) -> List[CheckResult]:
    """Broadcast ST_Intersects join of ~100k pings x all stores in Sedona, verified against numpy."""
    points, plant_idx, source = build_smoke_points(n_rows, pings_path)
    rings = {sid: parse_wkt_polygon(w) for sid, w in zip(stores_df["store_id"], stores_df["catchment_wkt"])
             if sid in ("S0001", "S0002")}
    wkt_s1 = stores_df.loc[stores_df["store_id"] == "S0001", "catchment_wkt"].iloc[0]
    a_lat, a_lon = planted_points()["PLANT_A_CENTER"]

    # Feed Spark through temporary CSV files read by the JVM. createDataFrame(python_list) needs Python
    # worker processes, which fail on some Windows setups ("Python worker failed to connect back").
    import tempfile
    tmp = Path(tempfile.mkdtemp(prefix="geopulse_smoke_"))
    pd.DataFrame({"pid": np.arange(len(points)), "lat": points[:, 0], "lon": points[:, 1]}).to_csv(
        tmp / "pings.csv", index=False, float_format="%.7f")
    stores_df[["store_id", "catchment_wkt"]].to_csv(tmp / "stores.csv", index=False)
    (sedona.read.option("header", True).schema("pid INT, lat DOUBLE, lon DOUBLE")
           .csv((tmp / "pings.csv").as_posix()).createOrReplaceTempView("vi_pings"))
    (sedona.read.option("header", True).option("quote", '"').schema("store_id STRING, wkt STRING")
           .csv((tmp / "stores.csv").as_posix())
           .selectExpr("store_id", "ST_GeomFromWKT(wkt) AS geom").createOrReplaceTempView("vi_stores"))
    store_rows = list(stores_df["store_id"])

    t0 = time.time()
    joined = sedona.sql(
        """
        SELECT /*+ BROADCAST(s) */ p.pid, s.store_id
        FROM (SELECT pid, ST_Point(lon, lat) AS geom FROM vi_pings) p      -- longitude FIRST
        JOIN vi_stores s ON ST_Intersects(s.geom, p.geom)
        """
    )
    total = joined.count()
    got = {(int(r["pid"]), str(r["store_id"])) for r in joined.where("store_id IN ('S0001', 'S0002')").collect()}
    secs = time.time() - t0
    shutil.rmtree(tmp, ignore_errors=True)

    expected = expected_matches(points, rings)
    results = [_res("smoke join: Sedona == numpy brute force (S0001 / S0002)", got == expected,
                    f"{len(got)} matches agree; {len(points):,} pings x {len(store_rows):,} stores "
                    f"-> {total:,} (ping, store) rows in {secs:.1f}s [{source}]",
                    f"only in Sedona: {sorted(got - expected)[:5]}; only in numpy: {sorted(expected - got)[:5]}")]

    wrong = []
    for name, want in PLANTED_EXPECTED.items():
        have = {s for (pid, s) in got if pid == plant_idx[name]}
        if have != want:
            wrong.append(f"{name}: got {sorted(have)}, expected {sorted(want)}")
    results.append(_res("smoke join: planted points land in the right store", not wrong,
                        "A centre -> S0001, B centre -> S0002, lens -> both, outside -> none", "; ".join(wrong)))

    swapped = sedona.sql(
        f"SELECT ST_Intersects(ST_GeomFromWKT('{wkt_s1}'), ST_Point({a_lat}, {a_lon})) AS inside"   # WRONG order
    ).collect()[0]["inside"]
    results.append(_res("smoke join: swapped ST_Point(lat, lon) is caught", swapped is False,
                        "Store A centre with the wrong coordinate order falls OUTSIDE S0001 (as it must)",
                        "swapped order still matched - the lon/lat guard is broken"))
    return results


# =========================================================================================
# CLI
# =========================================================================================
def print_results(results: Sequence[CheckResult]) -> bool:
    """Print every result and a summary; returns True if all passed."""
    for r in results:
        print(r)
    failed = [r for r in results if not r.passed]
    print("-" * 70)
    print(f"{len(results) - len(failed)}/{len(results)} checks passed" + ("" if not failed else f", {len(failed)} FAILED"))
    print("VALIDATION: PASS" if not failed else "VALIDATION: FAIL")
    return not failed


def main(argv: Optional[list] = None) -> int:
    parser = argparse.ArgumentParser(description="Validate GeoPulse spatial inputs (stores, pings, lon/lat order).")
    parser.add_argument("--stores", default=DEFAULT_STORES, help=f"stores csv (default {DEFAULT_STORES})")
    parser.add_argument("--pings", default=None, help="pings parquet file / folder / csv (skipped if omitted)")
    parser.add_argument("--max-rows", type=int, default=None, help="only check the first N ping rows")
    parser.add_argument("--smoke-join", action="store_true",
                        help="also run the 100k-ping Sedona smoke join (needs Spark, Sedona and Java 17)")
    parser.add_argument("--smoke-rows", type=int, default=SMOKE_ROWS, help="pings used by --smoke-join (default 100000)")
    parser.add_argument("--driver-memory", default=None, help="Spark driver memory for --smoke-join, e.g. 4g")
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")

    results: List[CheckResult] = []
    stores_df: Optional[pd.DataFrame] = None
    try:
        stores_df = load_stores(args.stores)
        results += check_stores(stores_df)
    except FileNotFoundError:
        results.append(CheckResult("stores: file readable", False,
                                   f"{args.stores} not found - run `python -m src.spatial.generate_stores`"))
    except Exception as exc:  # noqa: BLE001 - report, never crash
        results.append(CheckResult("stores: file readable", False, str(exc)))

    if args.pings:
        try:
            results += validate_pings(args.pings, max_rows=args.max_rows)
        except (FileNotFoundError, ImportError, ValueError) as exc:
            results.append(CheckResult("pings: file readable", False, str(exc)))
        except Exception as exc:  # noqa: BLE001
            results.append(CheckResult("pings: file readable", False, f"{type(exc).__name__}: {exc}"))
    else:
        print("(no --pings given: ping checks skipped)")

    if args.smoke_join:
        if stores_df is None or not all(c in stores_df.columns for c in STORE_COLUMNS):
            results.append(CheckResult("smoke join: needs valid stores", False, "fix the store checks first"))
        else:
            sedona = None
            try:
                try:
                    from src.spatial.sedona_config import get_sedona  # type: ignore
                except ImportError:
                    from sedona_config import get_sedona  # type: ignore
                sedona = get_sedona(app_name="geopulse-validate", driver_memory=args.driver_memory)
                results += run_smoke_join(sedona, stores_df, n_rows=args.smoke_rows,
                                          pings_path=args.pings if args.pings else None)
            except Exception as exc:  # noqa: BLE001
                results.append(CheckResult("smoke join: Spark session / join", False,
                                           f"{type(exc).__name__}: {str(exc).splitlines()[0] if str(exc) else exc}"))
            finally:
                if sedona is not None:
                    sedona.stop()

    return 0 if print_results(results) else 1


if __name__ == "__main__":
    sys.exit(main())