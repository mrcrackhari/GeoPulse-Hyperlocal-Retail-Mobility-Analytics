"""Generate the GeoPulse store seed data (Member 2, Day 2).

Writes three small, deterministic files (committed to git):

    data/seed/stores.csv            5,000 stores: id, name, centre, radius, zone, catchment WKT
    data/seed/store_polygons.geojson  the same catchment polygons + all other columns as properties
    data/seed/zoning.geojson        10 x 10 zoning grid (Z00..Z99) over the city bounding box

Run from the repository root:

    python -m src.spatial.generate_stores                 # writes data/seed/*
    python -m src.spatial.generate_stores --n-stores 300  # quick dev sample

Rules implemented (plan section 3.2 / 3.4 / 3.5)
------------------------------------------------
* S0001 = "Store A (existing)" at (12.9716, 77.5946); S0002 = "Store B (proposed)" at
  (12.9716, 77.6011), about 705 m east of A, so the two 500 m circles overlap in a lens
  roughly 295 m wide. Both coordinates are used exactly as given in the plan.
* The other 4,998 stores: 60% from Gaussians (sigma 0.06 deg) around the three hubs and
  40% uniform over the bounding box. Rejected when outside the box, closer than 600 m to any
  existing store, or closer than 1.5 km to S0001 / S0002.
* Catchment = 500 m radius circle built as a 32-sided polygon (quad_segs = 8) in
  EPSG:32643 (UTM 43N), reprojected to EPSG:4326, exterior ring counter-clockwise, closed.
* Zoning grid: 10 x 10 equal cells; zone type by distance to the nearest hub.

Implementation notes
--------------------
* Needs only numpy + scipy. The EPSG:32643 <-> EPSG:4326 transform is implemented here
  (Krueger series, WGS84, sub-millimetre accurate) so the output is bit-for-bit the same on
  every machine whether or not pyproj / shapely are installed. tests/test_stores.py
  cross-checks it against two independent formulas and, when installed, against pyproj.
* "Metres" in this module are EPSG:32643 grid metres: within ~0.09% of ground metres in this
  city. All 600 m / 1.5 km / 500 m rules use the same unit that the tests check.
* Fully deterministic: seeded numpy PCG64 stream, and normals are built from uniform draws
  (Box-Muller) so results do not depend on numpy's normal() implementation.
"""
from __future__ import annotations

import argparse
import csv
import json
import logging
import math
import time
from pathlib import Path
from typing import Dict, List, Sequence, Tuple

import numpy as np
from scipy.spatial import cKDTree

LOG = logging.getLogger("geopulse.generate_stores")

# --- Locked constants (plan section 3.2 - 3.5) -------------------------------------------
SEED = 42
MIN_LAT, MAX_LAT = 12.50, 13.40
MIN_LON, MAX_LON = 77.05, 77.95
HUBS: Tuple[Tuple[float, float], ...] = (  # (lat, lon)
    (12.9716, 77.5946),
    (13.0358, 77.5970),
    (12.9784, 77.6408),
)
N_STORES = 5000
RADIUS_M = 500.0
QUAD_SEGS = 8
N_SEGMENTS = 4 * QUAD_SEGS          # 32-gon
MIN_SEP_M = 600.0                   # min distance between any two store centres
ANCHOR_EXCL_M = 1500.0              # other stores keep this far from S0001 / S0002
HUB_SIGMA_DEG = 0.06
HUB_SHARE = 0.60
CENTRE_DECIMALS = 6                 # ~0.11 m
VERTEX_DECIMALS = 7                 # ~1 cm
STORE_A = ("S0001", "Store A (existing)", 12.9716, 77.5946)
STORE_B = ("S0002", "Store B (proposed)", 12.9716, 77.6011)
GRID_N = 10
ZONE_TYPES = ("commercial", "mixed", "residential", "industrial")
CSV_COLUMNS = ("store_id", "store_name", "center_lat", "center_lon",
               "radius_m", "zone_id", "zone_type", "catchment_wkt")

# --- EPSG:32643 (WGS84 / UTM zone 43N) -------------------------------------------------
_A = 6378137.0
_F = 1.0 / 298.257223563
_N = _F / (2.0 - _F)
_K0 = 0.9996
_E0 = 500000.0
_LON0 = math.radians(75.0)
_AA = _A / (1 + _N) * (1 + _N**2 / 4 + _N**4 / 64 + _N**6 / 256)
_ALPHA = (
    _N / 2 - 2 * _N**2 / 3 + 5 * _N**3 / 16 + 41 * _N**4 / 180,
    13 * _N**2 / 48 - 3 * _N**3 / 5 + 557 * _N**4 / 1440,
    61 * _N**3 / 240 - 103 * _N**4 / 140,
    49561 * _N**4 / 161280,
)
_BETA = (
    _N / 2 - 2 * _N**2 / 3 + 37 * _N**3 / 96 - _N**4 / 360,
    _N**2 / 48 + _N**3 / 15 - 437 * _N**4 / 1440,
    17 * _N**3 / 480 - 37 * _N**4 / 840,
    4397 * _N**4 / 161280,
)
_DELTA = (
    2 * _N - 2 * _N**2 / 3 - 2 * _N**3 + 116 * _N**4 / 45,
    7 * _N**2 / 3 - 8 * _N**3 / 5 - 227 * _N**4 / 45,
    56 * _N**3 / 15 - 136 * _N**4 / 35,
    4279 * _N**4 / 630,
)


def lonlat_to_utm43n(lon, lat):
    """(lon, lat) in degrees -> (easting, northing) in metres, EPSG:4326 -> EPSG:32643."""
    lon = np.asarray(lon, dtype=float)
    lat = np.asarray(lat, dtype=float)
    phi = np.radians(lat)
    dlam = np.radians(lon) - _LON0
    s = np.sin(phi)
    c = 2.0 * math.sqrt(_N) / (1.0 + _N)
    t = np.sinh(np.arctanh(s) - c * np.arctanh(c * s))
    xi_p = np.arctan2(t, np.cos(dlam))
    eta_p = np.arctanh(np.sin(dlam) / np.sqrt(1.0 + t * t))
    xi, eta = xi_p.copy(), eta_p.copy()
    for j, a in enumerate(_ALPHA, start=1):
        xi = xi + a * np.sin(2 * j * xi_p) * np.cosh(2 * j * eta_p)
        eta = eta + a * np.cos(2 * j * xi_p) * np.sinh(2 * j * eta_p)
    return _E0 + _K0 * _AA * eta, _K0 * _AA * xi


def utm43n_to_lonlat(easting, northing):
    """(easting, northing) in metres -> (lon, lat) in degrees, EPSG:32643 -> EPSG:4326."""
    xi = np.asarray(northing, dtype=float) / (_K0 * _AA)
    eta = (np.asarray(easting, dtype=float) - _E0) / (_K0 * _AA)
    xi_p, eta_p = xi.copy(), eta.copy()
    for j, b in enumerate(_BETA, start=1):
        xi_p = xi_p - b * np.sin(2 * j * xi) * np.cosh(2 * j * eta)
        eta_p = eta_p - b * np.cos(2 * j * xi) * np.sinh(2 * j * eta)
    chi = np.arcsin(np.sin(xi_p) / np.cosh(eta_p))
    phi = chi.copy()
    for j, d in enumerate(_DELTA, start=1):
        phi = phi + d * np.sin(2 * j * chi)
    lam = _LON0 + np.arctan2(np.sinh(eta_p), np.cos(xi_p))
    return np.degrees(lam), np.degrees(phi)


# --- Geometry helpers ------------------------------------------------------------------
def circle_ring_lonlat(lon: float, lat: float, radius_m: float = RADIUS_M,
                       n_segments: int = N_SEGMENTS) -> np.ndarray:
    """Closed, counter-clockwise ring (n_segments + 1, 2) of (lon, lat) around a centre.

    The circle is a regular polygon built in EPSG:32643 metres, then reprojected.
    """
    e0, n0 = lonlat_to_utm43n(lon, lat)
    ang = 2.0 * math.pi * np.arange(n_segments) / n_segments
    lo, la = utm43n_to_lonlat(e0 + radius_m * np.cos(ang), n0 + radius_m * np.sin(ang))
    ring = np.column_stack([lo, la])
    return np.vstack([ring, ring[:1]])


def _fmt(v: float, decimals: int) -> str:
    return f"{v:.{decimals}f}"


def ring_to_wkt(ring: np.ndarray) -> str:
    """WKT ``POLYGON((lon lat, ...))`` with 7 decimals (the same text goes into the GeoJSON)."""
    pairs = ", ".join(f"{_fmt(x, VERTEX_DECIMALS)} {_fmt(y, VERTEX_DECIMALS)}" for x, y in ring)
    return f"POLYGON(({pairs}))"


def _ring_to_geojson_coords(ring: np.ndarray) -> List[List[float]]:
    return [[float(_fmt(x, VERTEX_DECIMALS)), float(_fmt(y, VERTEX_DECIMALS))] for x, y in ring]


# --- Random helpers (stable across numpy versions) ---------------------------------------
def _normal(rng: np.random.Generator, n: int) -> np.ndarray:
    """n standard normals via Box-Muller from uniform draws."""
    m = (n + 1) // 2
    u1 = 1.0 - rng.random(m)            # (0, 1]
    u2 = rng.random(m)
    r = np.sqrt(-2.0 * np.log(u1))
    theta = 2.0 * math.pi * u2
    return np.concatenate([r * np.cos(theta), r * np.sin(theta)])[:n]


def _draw_hub(rng: np.random.Generator, m: int) -> Tuple[np.ndarray, np.ndarray]:
    hubs = np.asarray(HUBS, dtype=float)
    idx = np.minimum((rng.random(m) * len(HUBS)).astype(np.int64), len(HUBS) - 1)
    lat = hubs[idx, 0] + HUB_SIGMA_DEG * _normal(rng, m)
    lon = hubs[idx, 1] + HUB_SIGMA_DEG * _normal(rng, m)
    return lat, lon


def _draw_uniform(rng: np.random.Generator, m: int) -> Tuple[np.ndarray, np.ndarray]:
    lat = MIN_LAT + (MAX_LAT - MIN_LAT) * rng.random(m)
    lon = MIN_LON + (MAX_LON - MIN_LON) * rng.random(m)
    return lat, lon


def _sample_phase(rng, need, draw, existing_xy, anchors_xy, label, batch=20000, max_rounds=5000):
    """Rejection-sample ``need`` store centres; returns (lat, lon, xy) arrays.

    Each round draws ``batch`` candidates, drops those outside the box / too close to the
    anchors / too close to already accepted stores (vectorised, C speed), then accepts the
    survivors one by one, checking them against each other.
    """
    lats: List[np.ndarray] = []
    lons: List[np.ndarray] = []
    xys: List[np.ndarray] = []
    got, attempts = 0, 0
    for rnd in range(1, max_rounds + 1):
        if got >= need:
            break
        lat, lon = draw(rng, batch)
        attempts += batch
        lat = np.round(lat, CENTRE_DECIMALS)
        lon = np.round(lon, CENTRE_DECIMALS)
        inside = (lat >= MIN_LAT) & (lat <= MAX_LAT) & (lon >= MIN_LON) & (lon <= MAX_LON)
        lat, lon = lat[inside], lon[inside]
        e, n = lonlat_to_utm43n(lon, lat)
        xy = np.column_stack([e, n])

        keep = np.ones(len(xy), dtype=bool)
        for a in anchors_xy:
            keep &= np.hypot(xy[:, 0] - a[0], xy[:, 1] - a[1]) >= ANCHOR_EXCL_M
        placed = [existing_xy] + xys
        placed_xy = np.vstack([p for p in placed if len(p)])
        dist, _ = cKDTree(placed_xy).query(xy, k=1, distance_upper_bound=MIN_SEP_M)
        keep &= np.isinf(dist)

        buf = np.empty((need - got, 2))
        picked: List[int] = []
        for i in np.flatnonzero(keep):
            p = xy[i]
            if picked and np.min(np.hypot(buf[: len(picked), 0] - p[0], buf[: len(picked), 1] - p[1])) < MIN_SEP_M:
                continue
            buf[len(picked)] = p
            picked.append(int(i))
            if len(picked) == need - got:
                break
        if picked:
            sel = np.asarray(picked)
            lats.append(lat[sel])
            lons.append(lon[sel])
            xys.append(xy[sel])
            got += len(sel)
        LOG.debug("%s round %d: %d/%d accepted", label, rnd, got, need)
    if got < need:
        raise RuntimeError(
            f"Could only place {got} of {need} {label} stores after {attempts:,} candidates; "
            "the 600 m separation rule leaves no room - lower N_STORES or raise HUB_SIGMA_DEG."
        )
    LOG.info("%s stores: %d placed from %s candidates", label, need, f"{attempts:,}")
    return np.concatenate(lats), np.concatenate(lons), np.vstack(xys)


# --- Zoning --------------------------------------------------------------------------------
def _grid_edges(lo: float, hi: float) -> np.ndarray:
    return np.round(lo + (hi - lo) * np.arange(GRID_N + 1) / GRID_N, 6)


def zone_index(lat: float, lon: float) -> Tuple[int, int]:
    """(row, col) of the zoning cell holding a point; row 0 = southernmost, col 0 = westernmost."""
    i = int((lat - MIN_LAT) / ((MAX_LAT - MIN_LAT) / GRID_N))
    j = int((lon - MIN_LON) / ((MAX_LON - MIN_LON) / GRID_N))
    return min(max(i, 0), GRID_N - 1), min(max(j, 0), GRID_N - 1)


def zone_id_of(lat: float, lon: float) -> str:
    i, j = zone_index(lat, lon)
    return f"Z{i}{j}"


def build_zones(seed: int = SEED) -> List[Dict]:
    """The 10 x 10 zoning grid with a reproducible zone type per cell (own RNG stream)."""
    rng = np.random.default_rng(seed + 1)
    lat_edges, lon_edges = _grid_edges(MIN_LAT, MAX_LAT), _grid_edges(MIN_LON, MAX_LON)
    hub_e, hub_n = lonlat_to_utm43n([h[1] for h in HUBS], [h[0] for h in HUBS])
    zones: List[Dict] = []
    for i in range(GRID_N):
        for j in range(GRID_N):
            lat0, lat1 = float(lat_edges[i]), float(lat_edges[i + 1])
            lon0, lon1 = float(lon_edges[j]), float(lon_edges[j + 1])
            ce, cn = lonlat_to_utm43n((lon0 + lon1) / 2, (lat0 + lat1) / 2)
            dist_km = float(np.min(np.hypot(hub_e - ce, hub_n - cn))) / 1000.0
            u = float(rng.random())
            if dist_km < 8:
                ztype = "commercial" if u < 0.7 else "mixed"
            elif dist_km <= 20:
                ztype = "mixed" if u < 0.5 else "residential"
            else:
                ztype = "residential" if u < 0.7 else "industrial"
            ring = np.array([[lon0, lat0], [lon1, lat0], [lon1, lat1], [lon0, lat1], [lon0, lat0]])
            zones.append({
                "zone_id": f"Z{i}{j}", "zone_type": ztype, "hub_distance_km": round(dist_km, 3),
                "lat_min": lat0, "lat_max": lat1, "lon_min": lon0, "lon_max": lon1,
                "zone_wkt": ring_to_wkt(ring), "ring": ring,
            })
    return zones


# --- Store generation ------------------------------------------------------------------------
def generate(n_stores: int = N_STORES, seed: int = SEED) -> Tuple[List[Dict], List[Dict]]:
    """Return (stores, zones). Deterministic for a given (n_stores, seed)."""
    if n_stores < 2:
        raise ValueError("n_stores must be at least 2 (Store A and Store B)")
    t0 = time.time()
    rng = np.random.default_rng(seed)

    anchors = [STORE_A, STORE_B]
    a_lat = np.array([a[2] for a in anchors])
    a_lon = np.array([a[3] for a in anchors])
    ae, an = lonlat_to_utm43n(a_lon, a_lat)
    anchors_xy = np.column_stack([ae, an])

    n_other = n_stores - 2
    n_hub = int(round(HUB_SHARE * n_other))
    n_uni = n_other - n_hub

    lat_parts, lon_parts, xy_parts = [], [], []
    existing = anchors_xy
    for need, draw, label in ((n_hub, _draw_hub, "hub-cluster"), (n_uni, _draw_uniform, "uniform")):
        if need == 0:
            continue
        la, lo, xy = _sample_phase(rng, need, draw, existing, anchors_xy, label)
        lat_parts.append(la)
        lon_parts.append(lo)
        xy_parts.append(xy)
        existing = np.vstack([existing, xy])

    if n_other:
        lat_o, lon_o = np.concatenate(lat_parts), np.concatenate(lon_parts)
        order = np.argsort(rng.random(n_other), kind="stable")   # mix hub / uniform ids
        lat_o, lon_o = lat_o[order], lon_o[order]
    else:
        lat_o = lon_o = np.empty(0)

    # Safety net: no two centres closer than MIN_SEP_M.
    all_e, all_n = lonlat_to_utm43n(np.concatenate([a_lon, lon_o]), np.concatenate([a_lat, lat_o]))
    if cKDTree(np.column_stack([all_e, all_n])).query_pairs(MIN_SEP_M * (1 - 1e-12)):
        raise RuntimeError("internal error: two stores are closer than the minimum separation")

    centres = [(a[0], a[1], a[2], a[3]) for a in anchors]
    centres += [(f"S{k:04d}", f"Store S{k:04d}", float(lat_o[k - 3]), float(lon_o[k - 3]))
                for k in range(3, n_stores + 1)]

    stores: List[Dict] = []
    for store_id, name, lat, lon in centres:
        lat, lon = float(_fmt(lat, CENTRE_DECIMALS)), float(_fmt(lon, CENTRE_DECIMALS))
        ring = circle_ring_lonlat(lon, lat)
        zid = zone_id_of(lat, lon)
        stores.append({
            "store_id": store_id, "store_name": name, "center_lat": lat, "center_lon": lon,
            "radius_m": RADIUS_M, "zone_id": zid, "zone_type": "",
            "catchment_wkt": ring_to_wkt(ring), "ring": ring,
        })
    zones = build_zones(seed)
    ztype = {z["zone_id"]: z["zone_type"] for z in zones}
    for s in stores:
        s["zone_type"] = ztype[s["zone_id"]]
    LOG.info("Generated %d stores and %d zones in %.1fs", len(stores), len(zones), time.time() - t0)
    return stores, zones


# --- Writers ---------------------------------------------------------------------------------
def write_seed_files(stores: Sequence[Dict], zones: Sequence[Dict], out_dir) -> Dict[str, Path]:
    """Write stores.csv, store_polygons.geojson and zoning.geojson; return their paths."""
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    paths = {
        "stores": out / "stores.csv",
        "polygons": out / "store_polygons.geojson",
        "zoning": out / "zoning.geojson",
    }

    with open(paths["stores"], "w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh, lineterminator="\n")
        writer.writerow(CSV_COLUMNS)
        for s in stores:
            writer.writerow([
                s["store_id"], s["store_name"],
                _fmt(s["center_lat"], CENTRE_DECIMALS), _fmt(s["center_lon"], CENTRE_DECIMALS),
                f"{s['radius_m']:.0f}", s["zone_id"], s["zone_type"], s["catchment_wkt"],
            ])

    features = []
    for s in stores:
        features.append({
            "type": "Feature",
            "geometry": {"type": "Polygon", "coordinates": [_ring_to_geojson_coords(s["ring"])]},
            "properties": {
                "store_id": s["store_id"], "store_name": s["store_name"],
                "center_lat": s["center_lat"], "center_lon": s["center_lon"],
                "radius_m": int(s["radius_m"]), "zone_id": s["zone_id"], "zone_type": s["zone_type"],
            },
        })
    _write_json(paths["polygons"], {"type": "FeatureCollection", "name": "geopulse_store_catchments",
                                    "features": features})

    zfeatures = [{
        "type": "Feature",
        "geometry": {"type": "Polygon", "coordinates": [_ring_to_geojson_coords(z["ring"])]},
        "properties": {"zone_id": z["zone_id"], "zone_type": z["zone_type"], "zone_wkt": z["zone_wkt"]},
    } for z in zones]
    _write_json(paths["zoning"], {"type": "FeatureCollection", "name": "geopulse_zoning",
                                  "features": zfeatures})
    return paths


def _write_json(path: Path, obj) -> None:
    with open(path, "w", encoding="utf-8", newline="\n") as fh:
        json.dump(obj, fh, separators=(",", ":"))
        fh.write("\n")


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="Generate the GeoPulse store + zoning seed files.")
    parser.add_argument("--out-dir", default="data/seed", help="output folder (default data/seed)")
    parser.add_argument("--n-stores", type=int, default=N_STORES)
    parser.add_argument("--seed", type=int, default=SEED)
    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO,
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    try:
        stores, zones = generate(args.n_stores, args.seed)
        paths = write_seed_files(stores, zones, args.out_dir)
    except Exception as exc:  # noqa: BLE001 - CLI boundary
        LOG.error("store generation failed: %s", exc)
        return 1

    xy = np.column_stack(lonlat_to_utm43n([s["center_lon"] for s in stores], [s["center_lat"] for s in stores]))
    nn = cKDTree(xy).query(xy, k=2)[0][:, 1]
    ab = float(np.hypot(*(xy[0] - xy[1]))) if len(xy) > 1 else float("nan")
    zone_counts = {t: sum(1 for s in stores if s["zone_type"] == t) for t in ZONE_TYPES}
    print(f"stores written        : {len(stores)} -> {paths['stores']}")
    print(f"polygons / zoning     : {paths['polygons']}, {paths['zoning']}")
    print(f"Store A - Store B     : {ab:.1f} m (circle overlap lens {2 * RADIUS_M - ab:.0f} m wide)")
    print(f"nearest-neighbour min : {nn.min():.1f} m (rule: >= {MIN_SEP_M:.0f} m)")
    print(f"stores per zone type  : {zone_counts}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())