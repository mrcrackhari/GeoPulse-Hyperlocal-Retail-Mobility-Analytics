
from __future__ import annotations

import csv
import json
import math
import sys
from pathlib import Path

import numpy as np
import pytest
from scipy.spatial import cKDTree

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from src.spatial import generate_stores as gs  # noqa: E402

SEED_DIR = ROOT / "data" / "seed"
REGEN_HINT = "Run `python -m src.spatial.generate_stores` from the repo root and commit data/seed/*."

# WGS84 ellipsoid (used by the independent cross-checks below)
A_ELL = 6378137.0
F_ELL = 1.0 / 298.257223563
E2 = F_ELL * (2.0 - F_ELL)
EP2 = E2 / (1.0 - E2)
K0 = 0.9996


# --------------------------------------------------------------------------- helpers ---
def parse_wkt_ring(wkt: str) -> np.ndarray:
    inner = wkt[wkt.index("((") + 2: wkt.rindex("))")]
    return np.array([[float(v) for v in pt.split()] for pt in inner.split(",")])


def inside_convex_ccw(ring: np.ndarray, lon: float, lat: float) -> bool:
    """True if the point is strictly inside a convex counter-clockwise closed ring."""
    v = ring[:-1]
    e = np.roll(v, -1, axis=0) - v
    d = np.array([lon, lat]) - v
    return bool(np.all(e[:, 0] * d[:, 1] - e[:, 1] * d[:, 0] > 0))


def snyder_utm43n(lon_deg: float, lat_deg: float):
    """Independent forward Transverse Mercator (Snyder, USGS PP 1395, eq. 8-9..8-13) for zone 43N."""
    phi, lam, lam0 = math.radians(lat_deg), math.radians(lon_deg), math.radians(75.0)
    sin_p, cos_p, tan_p = math.sin(phi), math.cos(phi), math.tan(phi)
    n_rad = A_ELL / math.sqrt(1.0 - E2 * sin_p**2)
    t, c, a = tan_p**2, EP2 * cos_p**2, (lam - lam0) * cos_p
    m = A_ELL * ((1 - E2 / 4 - 3 * E2**2 / 64 - 5 * E2**3 / 256) * phi
                 - (3 * E2 / 8 + 3 * E2**2 / 32 + 45 * E2**3 / 1024) * math.sin(2 * phi)
                 + (15 * E2**2 / 256 + 45 * E2**3 / 1024) * math.sin(4 * phi)
                 - (35 * E2**3 / 3072) * math.sin(6 * phi))
    x = 500000.0 + K0 * n_rad * (a + (1 - t + c) * a**3 / 6
                                 + (5 - 18 * t + t**2 + 72 * c - 58 * EP2) * a**5 / 120)
    y = K0 * (m + n_rad * tan_p * (a**2 / 2 + (5 - t + 9 * c + 4 * c**2) * a**4 / 24
                                   + (61 - 58 * t + t**2 + 600 * c - 330 * EP2) * a**6 / 720))
    return x, y


def ground_ew_m(lat_deg: float, dlon_deg: float) -> float:
    phi = math.radians(lat_deg)
    n_rad = A_ELL / math.sqrt(1.0 - E2 * math.sin(phi) ** 2)
    return n_rad * math.cos(phi) * math.radians(dlon_deg)


def ground_ns_m(lat_deg: float, dlat_deg: float) -> float:
    phi = math.radians(lat_deg + dlat_deg / 2.0)
    m_rad = A_ELL * (1.0 - E2) / (1.0 - E2 * math.sin(phi) ** 2) ** 1.5
    return m_rad * math.radians(dlat_deg)


# -------------------------------------------------------------------------- fixtures ---
@pytest.fixture(scope="module")
def rows():
    path = SEED_DIR / "stores.csv"
    if not path.exists():
        pytest.fail(f"{path} is missing. {REGEN_HINT}")
    with open(path, newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        out = list(reader)
        header = reader.fieldnames
    return header, out


@pytest.fixture(scope="module")
def stores(rows):
    return rows[1]


@pytest.fixture(scope="module")
def centres_utm(stores):
    lon = np.array([float(s["center_lon"]) for s in stores])
    lat = np.array([float(s["center_lat"]) for s in stores])
    e, n = gs.lonlat_to_utm43n(lon, lat)
    return np.column_stack([e, n])


@pytest.fixture(scope="module")
def rings(stores):
    return np.array([parse_wkt_ring(s["catchment_wkt"]) for s in stores])  # (N, 33, 2) lon/lat


@pytest.fixture(scope="module")
def zoning():
    path = SEED_DIR / "zoning.geojson"
    if not path.exists():
        pytest.fail(f"{path} is missing. {REGEN_HINT}")
    return json.loads(path.read_text(encoding="utf-8"))


@pytest.fixture(scope="module")
def polygons_geojson():
    path = SEED_DIR / "store_polygons.geojson"
    if not path.exists():
        pytest.fail(f"{path} is missing. {REGEN_HINT}")
    return json.loads(path.read_text(encoding="utf-8"))


# ----------------------------------------------------------------- contract: stores.csv ---
def test_row_count_and_columns(rows):
    header, data = rows
    assert header == list(gs.CSV_COLUMNS)              # exact contract 9.2, in order
    assert len(data) == 5000


def test_ids_unique_and_sequential(stores):
    ids = [s["store_id"] for s in stores]
    assert ids == [f"S{k:04d}" for k in range(1, 5001)]
    assert len(set(ids)) == 5000


def test_anchor_stores_exact(stores):
    a, b = stores[0], stores[1]
    assert (a["store_id"], a["store_name"]) == ("S0001", "Store A (existing)")
    assert (b["store_id"], b["store_name"]) == ("S0002", "Store B (proposed)")
    assert (float(a["center_lat"]), float(a["center_lon"])) == (12.9716, 77.5946)
    assert (float(b["center_lat"]), float(b["center_lon"])) == (12.9716, 77.6011)


def test_names_and_radius(stores):
    for s in stores[2:]:
        assert s["store_name"] == f"Store {s['store_id']}"
    assert {s["radius_m"] for s in stores} == {"500"}


def test_no_empty_values(stores):
    for s in stores:
        assert all(v not in ("", None) for v in s.values()), s["store_id"]


def test_centres_inside_bbox(stores):
    lat = np.array([float(s["center_lat"]) for s in stores])
    lon = np.array([float(s["center_lon"]) for s in stores])
    assert lat.min() >= gs.MIN_LAT and lat.max() <= gs.MAX_LAT
    assert lon.min() >= gs.MIN_LON and lon.max() <= gs.MAX_LON


def test_centres_have_at_most_6_decimals(stores):
    for s in stores:
        for col in ("center_lat", "center_lon"):
            assert len(s[col].split(".")[1]) <= 6


def test_store_mix_is_roughly_hub_clustered(stores):
    """60% hub-cluster + 40% uniform: most stores sit within ~27 km (0.25 deg) of a hub."""
    hubs = np.asarray(gs.HUBS)
    near = 0
    for s in stores[2:]:
        d = np.hypot(hubs[:, 0] - float(s["center_lat"]), hubs[:, 1] - float(s["center_lon"]))
        near += bool(d.min() <= 0.25)
    share = near / (len(stores) - 2)
    assert 0.60 <= share <= 0.90, share


# --------------------------------------------------------- distances & the planted pair ---
def test_store_a_store_b_distance_and_overlap(centres_utm):
    d = float(np.hypot(*(centres_utm[0] - centres_utm[1])))
    # Plan text says "~700 m"; the plan's own coordinates give 705.6 grid metres (about 705.2 m
    # on the ground), so the check is 700 +/- 10 m. Documented in docs/spatial_join_notes.md.
    assert 690.0 <= d <= 710.0, d
    lens_width = 2 * gs.RADIUS_M - d
    assert 280.0 <= lens_width <= 310.0, lens_width     # the circles overlap in a ~295 m lens


def test_anchor_exclusion_zone(centres_utm):
    for k in (0, 1):
        d = np.hypot(centres_utm[2:, 0] - centres_utm[k, 0], centres_utm[2:, 1] - centres_utm[k, 1])
        assert d.min() >= gs.ANCHOR_EXCL_M - 1e-6, (k, d.min())


def test_min_store_separation(centres_utm):
    nn = cKDTree(centres_utm).query(centres_utm, k=2)[0][:, 1]
    assert nn.min() >= gs.MIN_SEP_M - 1e-6, nn.min()


def test_grid_to_ground_scale_is_tight_and_conformal():
    """Grid metres vs ellipsoidal ground metres over the whole bbox (independent of the generator)."""
    dlon, dlat = 0.0065, 0.0055
    for lon in (77.05, 77.5, 77.95):
        for lat in (12.5, 12.95, 13.4):
            e0, n0 = gs.lonlat_to_utm43n(lon, lat)
            e1, n1 = gs.lonlat_to_utm43n(lon + dlon, lat)
            e2, n2 = gs.lonlat_to_utm43n(lon, lat + dlat)
            k_ew = float(np.hypot(e1 - e0, n1 - n0)) / ground_ew_m(lat, dlon)
            k_ns = float(np.hypot(e2 - e0, n2 - n0)) / ground_ns_m(lat, dlat)
            assert 1.0002 <= k_ew <= 1.0010, (lon, lat, k_ew)
            assert abs(k_ew - k_ns) < 2e-5, (lon, lat, k_ew, k_ns)   # conformal: same scale both ways


# --------------------------------------------------------------------- catchment polygons ---
def test_polygons_shape_closed_ccw_convex(rings):
    assert rings.shape == (5000, 33, 2)
    assert np.array_equal(rings[:, 0, :], rings[:, -1, :])             # closed
    v = rings[:, :-1, :]
    nxt = np.roll(v, -1, axis=1)
    signed_area = 0.5 * np.sum(v[..., 0] * nxt[..., 1] - nxt[..., 0] * v[..., 1], axis=1)
    assert (signed_area > 0).all()                                      # counter-clockwise exterior ring
    e = nxt - v
    e2 = np.roll(e, -1, axis=1)
    cross = e[..., 0] * e2[..., 1] - e[..., 1] * e2[..., 0]
    assert (cross > 0).all()                                            # strictly convex => simple/valid
    for ring in rings[:50]:
        assert len({tuple(p) for p in ring[:-1]}) == 32                 # 32 distinct vertices


def test_polygon_area_is_078_km2(rings):
    areas = np.empty(len(rings))
    for i, ring in enumerate(rings):
        e, n = gs.lonlat_to_utm43n(ring[:, 0], ring[:, 1])
        areas[i] = 0.5 * np.sum(e[:-1] * n[1:] - e[1:] * n[:-1])
    expected = 0.5 * gs.N_SEGMENTS * gs.RADIUS_M**2 * math.sin(2 * math.pi / gs.N_SEGMENTS)  # 780,361 m2
    assert abs(expected - 780_361) < 5
    assert np.all(np.abs(areas / expected - 1.0) < 0.01), (areas.min(), areas.max())
    assert np.all(np.abs(areas / 1e6 - 0.78) / 0.78 < 0.01)


def test_vertices_are_500m_from_centre(rings, centres_utm):
    e, n = gs.lonlat_to_utm43n(rings[:, :-1, 0], rings[:, :-1, 1])
    r = np.hypot(e - centres_utm[:, 0:1], n - centres_utm[:, 1:2])
    assert np.abs(r - gs.RADIUS_M).max() < 0.05, np.abs(r - gs.RADIUS_M).max()


def test_each_polygon_contains_its_centre(stores, rings):
    for k in range(0, 5000, 25):
        assert inside_convex_ccw(rings[k], float(stores[k]["center_lon"]), float(stores[k]["center_lat"]))


def test_planted_pair_geometry(stores, rings, centres_utm):
    """Overlap lens exists; A-only and B-only zones exist (needed by the simulator's planted cohorts)."""
    a_ring, b_ring = rings[0], rings[1]
    mid = (centres_utm[0] + centres_utm[1]) / 2
    lon, lat = gs.utm43n_to_lonlat(mid[0], mid[1])
    assert inside_convex_ccw(a_ring, float(lon), float(lat)) and inside_convex_ccw(b_ring, float(lon), float(lat))

    # a point 420 m west of A is inside A, outside B (>= 550 m from B)
    lon_w, lat_w = gs.utm43n_to_lonlat(centres_utm[0, 0] - 420.0, centres_utm[0, 1])
    assert inside_convex_ccw(a_ring, float(lon_w), float(lat_w))
    assert not inside_convex_ccw(b_ring, float(lon_w), float(lat_w))
    # a point 420 m east of B is inside B, outside A
    lon_e, lat_e = gs.utm43n_to_lonlat(centres_utm[1, 0] + 420.0, centres_utm[1, 1])
    assert inside_convex_ccw(b_ring, float(lon_e), float(lat_e))
    assert not inside_convex_ccw(a_ring, float(lon_e), float(lat_e))


def test_no_other_catchment_overlaps_the_planted_pair(centres_utm):
    """1.5 km exclusion => no other 500 m catchment can touch S0001 / S0002's circles."""
    for k in (0, 1):
        d = np.hypot(centres_utm[2:, 0] - centres_utm[k, 0], centres_utm[2:, 1] - centres_utm[k, 1])
        assert d.min() > 2 * gs.RADIUS_M


def test_shapely_validity_if_installed(stores):
    shapely = pytest.importorskip("shapely")
    from shapely import wkt as shapely_wkt  # noqa: F401
    for s in stores:
        geom = shapely_wkt.loads(s["catchment_wkt"])
        assert geom.is_valid and geom.geom_type == "Polygon", s["store_id"]


# -------------------------------------------------------------- projection implementation ---
def test_utm_matches_independent_snyder_formulas():
    pts = [(75.0, 0.0), (75.0, 12.9716), (77.5946, 12.9716), (77.6011, 12.9716), (77.05, 12.5),
           (77.95, 13.4), (77.05, 13.4), (77.95, 12.5), (77.5, 13.0358)]
    for lon, lat in pts:
        e, n = gs.lonlat_to_utm43n(lon, lat)
        ex, ny = snyder_utm43n(lon, lat)
        assert abs(float(e) - ex) < 0.02 and abs(float(n) - ny) < 0.02, (lon, lat, float(e) - ex, float(n) - ny)
    e0, n0 = gs.lonlat_to_utm43n(75.0, 0.0)
    assert (round(float(e0), 6), round(float(n0), 6)) == (500000.0, 0.0)   # definition of the CRS


def test_utm_round_trip():
    rng = np.random.default_rng(0)
    lon = gs.MIN_LON + (gs.MAX_LON - gs.MIN_LON) * rng.random(1000)
    lat = gs.MIN_LAT + (gs.MAX_LAT - gs.MIN_LAT) * rng.random(1000)
    e, n = gs.lonlat_to_utm43n(lon, lat)
    lon2, lat2 = gs.utm43n_to_lonlat(e, n)
    assert np.abs(lon2 - lon).max() < 1e-9 and np.abs(lat2 - lat).max() < 1e-9


def test_utm_matches_pyproj_if_installed():
    pyproj = pytest.importorskip("pyproj")
    tr = pyproj.Transformer.from_crs("EPSG:4326", "EPSG:32643", always_xy=True)
    for lon, lat in [(77.5946, 12.9716), (77.05, 12.5), (77.95, 13.4)]:
        ex, ny = tr.transform(lon, lat)
        e, n = gs.lonlat_to_utm43n(lon, lat)
        assert abs(float(e) - ex) < 0.005 and abs(float(n) - ny) < 0.005


# ---------------------------------------------------------------------------- zoning ---
def test_zoning_grid_cells(zoning):
    feats = zoning["features"]
    assert len(feats) == 100
    ids = [f["properties"]["zone_id"] for f in feats]
    assert sorted(ids) == sorted(f"Z{i}{j}" for i in range(10) for j in range(10))
    lons, lats = [], []
    for f in feats:
        ring = np.array(f["geometry"]["coordinates"][0])
        assert len(ring) == 5 and np.array_equal(ring[0], ring[-1])
        lons += [ring[:, 0].min(), ring[:, 0].max()]
        lats += [ring[:, 1].min(), ring[:, 1].max()]
        assert abs((ring[:, 0].max() - ring[:, 0].min()) - 0.09) < 1e-6
        assert abs((ring[:, 1].max() - ring[:, 1].min()) - 0.09) < 1e-6
    assert (min(lons), max(lons)) == (gs.MIN_LON, gs.MAX_LON)
    assert (min(lats), max(lats)) == (gs.MIN_LAT, gs.MAX_LAT)


def test_zone_types_follow_distance_rule(zoning):
    built = {z["zone_id"]: z for z in gs.build_zones()}
    seen = set()
    for f in zoning["features"]:
        zid, ztype = f["properties"]["zone_id"], f["properties"]["zone_type"]
        assert ztype == built[zid]["zone_type"]                    # file == generator output
        d = built[zid]["hub_distance_km"]
        allowed = ({"commercial", "mixed"} if d < 8 else {"mixed", "residential"} if d <= 20
                   else {"residential", "industrial"})
        assert ztype in allowed, (zid, d, ztype)
        assert f["properties"]["zone_wkt"].startswith("POLYGON((")
        seen.add(ztype)
    assert seen == set(gs.ZONE_TYPES)


def test_store_zone_assignment(stores, zoning):
    zone_type = {f["properties"]["zone_id"]: f["properties"]["zone_type"] for f in zoning["features"]}
    for s in stores:
        assert s["zone_id"] == gs.zone_id_of(float(s["center_lat"]), float(s["center_lon"]))
        assert s["zone_type"] == zone_type[s["zone_id"]]


# --------------------------------------------------------------------- store_polygons.geojson ---
def test_geojson_matches_csv(stores, rings, polygons_geojson):
    feats = polygons_geojson["features"]
    assert polygons_geojson["type"] == "FeatureCollection" and len(feats) == 5000
    for k in range(0, 5000, 7):
        f, s = feats[k], stores[k]
        assert f["geometry"]["type"] == "Polygon"
        assert np.array_equal(np.array(f["geometry"]["coordinates"][0]), rings[k])
        p = f["properties"]
        assert p["store_id"] == s["store_id"] and p["store_name"] == s["store_name"]
        assert p["zone_id"] == s["zone_id"] and p["zone_type"] == s["zone_type"]
        assert p["radius_m"] == 500
        assert (p["center_lat"], p["center_lon"]) == (float(s["center_lat"]), float(s["center_lon"]))
    assert [f["properties"]["store_id"] for f in feats] == [s["store_id"] for s in stores]


# ----------------------------------------------------------------- generator behaviour ---
def test_two_runs_are_byte_identical(tmp_path):
    names = ("stores.csv", "store_polygons.geojson", "zoning.geojson")
    for sub in ("a", "b"):
        st, zn = gs.generate()
        gs.write_seed_files(st, zn, tmp_path / sub)
    for name in names:
        assert (tmp_path / "a" / name).read_bytes() == (tmp_path / "b" / name).read_bytes(), name


def test_committed_seed_matches_generator(tmp_path):
    """Catches 'edited the generator but forgot to regenerate / commit data/seed'."""
    st, zn = gs.generate()
    gs.write_seed_files(st, zn, tmp_path)
    for name in ("stores.csv", "store_polygons.geojson", "zoning.geojson"):
        committed = (SEED_DIR / name).read_bytes()
        assert (tmp_path / name).read_bytes() == committed, f"{name} is stale. {REGEN_HINT}"


def test_different_seed_changes_stores_but_not_the_planted_pair():
    a, _ = gs.generate(300, seed=1)
    b, _ = gs.generate(300, seed=2)
    assert [s["center_lat"] for s in a[2:]] != [s["center_lat"] for s in b[2:]]
    for st in (a, b):
        assert (st[0]["store_id"], st[0]["center_lat"], st[0]["center_lon"]) == ("S0001", 12.9716, 77.5946)
        assert (st[1]["store_id"], st[1]["center_lat"], st[1]["center_lon"]) == ("S0002", 12.9716, 77.6011)


def test_small_runs_and_bad_input():
    st, zn = gs.generate(10)
    assert len(st) == 10 and len(zn) == 100
    assert [s["store_id"] for s in st] == [f"S{k:04d}" for k in range(1, 11)]
    two, _ = gs.generate(2)
    assert [s["store_id"] for s in two] == ["S0001", "S0002"]
    with pytest.raises(ValueError):
        gs.generate(1)


def test_cli_exit_codes(tmp_path):
    assert gs.main(["--out-dir", str(tmp_path), "--n-stores", "50"]) == 0
    assert (tmp_path / "stores.csv").exists()
    assert gs.main(["--out-dir", str(tmp_path), "--n-stores", "1"]) == 1     # generator error -> exit 1
