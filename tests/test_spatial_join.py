"""Tests for src/spatial/validate_inputs.py and the lon/lat-order smoke join (Member 2, Day 3).

Most tests are pure Python (no Spark). The live Sedona tests at the bottom skip automatically when
pyspark / apache-sedona / Java 17 are missing. The key guard: Store A's centre must be INSIDE S0001
when the point is built ST_Point(lon, lat), and OUTSIDE when built ST_Point(lat, lon).
"""
from __future__ import annotations

import os
import shutil
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from src.spatial import generate_stores as gs  # noqa: E402
from src.spatial import validate_inputs as vi  # noqa: E402

STORES_CSV = ROOT / "data" / "seed" / "stores.csv"


@pytest.fixture(scope="module")
def stores() -> pd.DataFrame:
    if not STORES_CSV.exists():
        pytest.skip("run `python -m src.spatial.generate_stores` first")
    return vi.load_stores(STORES_CSV)


def _failed(results):
    return [r.name for r in results if not r.passed]


def _good_pings(n=50) -> pd.DataFrame:
    rng = np.random.default_rng(1)
    return pd.DataFrame({
        "device_id": [f"D{i:06d}" for i in range(n)],
        "lat": 12.6 + 0.5 * rng.random(n),
        "lon": 77.2 + 0.5 * rng.random(n),
        "ts": pd.date_range("2026-01-05", periods=n, freq="h"),
    })


def _ping_failures(df):
    st = vi.PingStats()
    st.update(df)
    return _failed(vi.ping_checks(st))


# ------------------------------------------------------------------ constants stay in sync ---
def test_constants_match_generate_stores():
    assert (vi.MIN_LAT, vi.MAX_LAT, vi.MIN_LON, vi.MAX_LON) == (gs.MIN_LAT, gs.MAX_LAT, gs.MIN_LON, gs.MAX_LON)
    assert vi.STORE_COLUMNS == tuple(gs.CSV_COLUMNS)
    assert set(vi.ZONE_TYPES) == set(gs.ZONE_TYPES)


# ------------------------------------------------------------------------- store checks ---
def test_seed_stores_pass_every_check(stores):
    assert _failed(vi.check_stores(stores)) == []


def test_store_centre_swap_detected(stores):
    bad = stores.copy()
    bad.loc[0, ["center_lat", "center_lon"]] = bad.loc[0, ["center_lon", "center_lat"]].values
    failed = _failed(vi.check_stores(bad))
    assert "stores: centre lat/lon NOT swapped" in failed


def test_store_wkt_swapped_detected(stores):
    bad = stores.copy()
    ring = vi.parse_wkt_polygon(bad.loc[0, "catchment_wkt"])
    bad.loc[0, "catchment_wkt"] = "POLYGON ((" + ", ".join(f"{y:.7f} {x:.7f}" for x, y in ring) + "))"
    assert "stores: WKT is (lon lat), not (lat lon)" in _failed(vi.check_stores(bad))


def test_store_duplicate_and_missing_detected(stores):
    bad = stores.copy()
    bad.loc[1, "store_id"] = bad.loc[0, "store_id"]
    bad.loc[2, "zone_id"] = None
    failed = _failed(vi.check_stores(bad))
    assert "stores: store_id unique" in failed
    assert "stores: no empty values" in failed


def test_store_bad_polygons_detected(stores):
    bad = stores.copy()
    bad.loc[0, "catchment_wkt"] = "POLYGON ((77.5 12.9, 77.6 12.9, 77.6 13.0, 77.5 12.9))"     
    bad.loc[1, "catchment_wkt"] = "POLYGON ((77.5 12.9, 77.6 13.0, 77.6 12.9, 77.5 13.0, 77.5 12.9))"  # bow-tie
    bad.loc[2, "catchment_wkt"] = "NOT A POLYGON"
    failed = _failed(vi.check_stores(bad))
    assert "stores: polygons valid (no self-crossing)" in failed
    assert "stores: catchment WKT parses (one POLYGON ring)" in failed


def test_clockwise_ring_detected(stores):
    bad = stores.copy()
    ring = vi.parse_wkt_polygon(bad.loc[0, "catchment_wkt"])[::-1]
    bad.loc[0, "catchment_wkt"] = "POLYGON ((" + ", ".join(f"{x:.7f} {y:.7f}" for x, y in ring) + "))"
    assert "stores: exterior ring counter-clockwise" in _failed(vi.check_stores(bad))


def test_missing_columns_reported():
    res = vi.check_stores(pd.DataFrame({"store_id": ["S0001"]}))
    assert len(res) == 1 and not res[0].passed


# ------------------------------------------------------------------------- ping checks ---
def test_good_pings_pass():
    assert _ping_failures(_good_pings()) == []


def test_swapped_pings_detected():
    df = _good_pings()
    df[["lat", "lon"]] = df[["lon", "lat"]].values
    failed = _ping_failures(df)
    assert "pings: lat/lon NOT swapped" in failed
    assert "pings: inside the city box" in failed


def test_other_ping_problems_detected():
    df = _good_pings()
    df.loc[0, "lat"] = 40.0                                   # outside city
    df.loc[1, "ts"] = pd.Timestamp("2025-06-01")              # outside window
    df.loc[2, "device_id"] = "bogus"
    df.loc[3, "lon"] = np.nan
    failed = _ping_failures(df)
    for name in ("pings: inside the city box", "pings: timestamps in data window",
                 "pings: device_id format D######", "pings: no null values"):
        assert name in failed


def test_tz_aware_timestamps_detected():
    df = _good_pings()
    df["ts"] = df["ts"].dt.tz_localize("UTC")
    assert "pings: timestamps have no timezone" in _ping_failures(df)


def test_missing_ping_columns():
    st = vi.PingStats()
    st.update(_good_pings().drop(columns=["ts"]))
    assert not vi.ping_checks(st)[0].passed


def test_chunked_validation_from_csv(tmp_path):
    p = tmp_path / "pings.csv"
    _good_pings(100).to_csv(p, index=False)
    assert all(r.passed for r in vi.validate_pings(p, batch_rows=30))
    assert vi.PingStats().rows == 0
    assert sum(len(c) for c in vi.iter_ping_chunks(p, batch_rows=30, max_rows=45)) == 45


# ----------------------------------------------------------------------- geometry helpers ---
def test_planted_points_geometry(stores):
    a = stores[stores.store_id == "S0001"].iloc[0]
    b = stores[stores.store_id == "S0002"].iloc[0]
    rings = {"S0001": vi.parse_wkt_polygon(a.catchment_wkt), "S0002": vi.parse_wkt_polygon(b.catchment_wkt)}
    pts = vi.planted_points()
    assert pts["PLANT_A_CENTER"] == (float(a.center_lat), float(a.center_lon))
    for name, want in vi.PLANTED_EXPECTED.items():
        lat, lon = pts[name]
        got = {s for s, r in rings.items() if vi.point_in_ring(lon, lat, r)}
        assert got == want, name


def test_lon_lat_order_guard_pure_python(stores):
    """Store A centre is inside S0001 as (lon, lat) and outside as (lat, lon) - the Day 3 acceptance check."""
    a = stores[stores.store_id == "S0001"].iloc[0]
    ring = vi.parse_wkt_polygon(a.catchment_wkt)
    lat, lon = float(a.center_lat), float(a.center_lon)
    assert vi.point_in_ring(lon, lat, ring) is True
    assert vi.point_in_ring(lat, lon, ring) is False


def test_vectorised_matches_ray_casting(stores):
    ring = vi.parse_wkt_polygon(stores.iloc[0].catchment_wkt)
    cx, cy = ring[:-1].mean(axis=0)
    rng = np.random.default_rng(3)
    xy = np.column_stack([cx + (rng.random(500) - 0.5) * 0.02, cy + (rng.random(500) - 0.5) * 0.02])
    fast = vi.points_in_convex_ccw(xy, ring)
    slow = np.array([vi.point_in_ring(x, y, ring) for x, y in xy])
    assert (fast == slow).all() and fast.any() and not fast.all()


def test_ring_is_simple():
    square = np.array([[0, 0], [1, 0], [1, 1], [0, 1], [0, 0]], float)
    bowtie = np.array([[0, 0], [1, 1], [1, 0], [0, 1], [0, 0]], float)
    assert vi.ring_is_simple(square) and not vi.ring_is_simple(bowtie)
    assert vi.signed_area(square) > 0 and vi.signed_area(square[::-1]) < 0


def test_smoke_points_has_100k_plus_planted():
    pts, idx, _ = vi.build_smoke_points()
    assert len(pts) == vi.SMOKE_ROWS + len(vi.PLANTED_EXPECTED)
    assert set(idx) == set(vi.PLANTED_EXPECTED)
    assert (pts[:, 0] >= vi.MIN_LAT).all() and (pts[:, 1] <= vi.MAX_LON).all()


def test_cli_exit_codes(tmp_path, capsys):
    assert vi.main(["--stores", str(STORES_CSV)]) == 0 if STORES_CSV.exists() else True
    assert vi.main(["--stores", str(tmp_path / "missing.csv")]) == 1


# ------------------------------------------------------------------ live Spark smoke join ---
@pytest.fixture(scope="module")
def sedona_session():
    pytest.importorskip("pyspark")
    pytest.importorskip("sedona.spark")
    from src.spatial import sedona_config as sc
    sc.configure_environment()
    if shutil.which("java") is None and not os.environ.get("JAVA_HOME"):
        pytest.skip("Java 17 is not available")
    session = sc.get_sedona(app_name="geopulse-join-tests", driver_memory="2g", shuffle_partitions=4)
    yield session
    session.stop()


def test_sedona_lon_lat_order(sedona_session, stores):
    a = stores[stores.store_id == "S0001"].iloc[0]
    lat, lon = float(a.center_lat), float(a.center_lon)
    q = "SELECT ST_Intersects(ST_GeomFromWKT('{w}'), ST_Point({x}, {y})) AS hit"
    right = sedona_session.sql(q.format(w=a.catchment_wkt, x=lon, y=lat)).collect()[0]["hit"]
    wrong = sedona_session.sql(q.format(w=a.catchment_wkt, x=lat, y=lon)).collect()[0]["hit"]
    assert right is True and wrong is False


def test_sedona_100k_smoke_join(sedona_session, stores):
    results = vi.run_smoke_join(sedona_session, stores, n_rows=vi.SMOKE_ROWS)
    assert all(r.passed for r in results), [str(r) for r in results if not r.passed]