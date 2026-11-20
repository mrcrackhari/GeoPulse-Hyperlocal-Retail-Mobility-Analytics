"""Tests for src/spatial/spatial_join.py (Member 2, Day 4): contract 9.3, helpers and a live Sedona run.

Pure-Python tests run anywhere. The live tests skip automatically without pyspark / apache-sedona /
pyarrow / Java 17.
"""
from __future__ import annotations

import json
import os
import shutil
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from src.spatial import make_demo_pings as demo  # noqa: E402
from src.spatial import spatial_join as sj  # noqa: E402
from src.spatial import validate_inputs as vi  # noqa: E402

STORES_CSV = ROOT / "data" / "seed" / "stores.csv"


# ------------------------------------------------------------------------------ contract ---
def test_contract_columns_and_ddl():
    assert sj.MATCH_COLUMNS == ("device_id", "store_id", "ts", "lat", "lon")
    assert sj.MATCH_DDL == "device_id string, store_id string, ts timestamp, lat double, lon double"


def test_schema_problems_accepts_the_contract():
    fields = [(c, sj.MATCH_TYPES[c]) for c in sj.MATCH_COLUMNS]
    assert sj.schema_problems(fields) == []


def test_schema_problems_reports_each_kind_of_mismatch():
    ok = [(c, sj.MATCH_TYPES[c]) for c in sj.MATCH_COLUMNS]
    wrong_type = [(c, "float" if c == "lat" else t) for c, t in ok]
    assert any("lat is float" in p for p in sj.schema_problems(wrong_type))
    assert sj.schema_problems(ok[:-1])                                   # missing column
    assert sj.schema_problems(list(reversed(ok)))                        # wrong order
    assert sj.schema_problems(ok + [("geom", "binary")])                 # extra column


def test_sql_is_a_broadcast_intersects_join_with_lon_first():
    assert "BROADCAST(s)" in sj.JOIN_SQL and "ST_Intersects(s.geom, p.geom)" in sj.JOIN_SQL
    assert sj.POINT_EXPR.index("lon") < sj.POINT_EXPR.index("lat")       # ST_Point(lon, lat)
    for col in sj.MATCH_COLUMNS:
        assert col in sj.JOIN_SQL


# ------------------------------------------------------------------------------- helpers ---
def test_list_ping_files(tmp_path):
    (tmp_path / "pings_2026-01-02.parquet").write_text("x")
    (tmp_path / "pings_2026-01-01.parquet").write_text("x")
    (tmp_path / "notes.txt").write_text("x")
    assert [f.name for f in sj.list_ping_files(tmp_path)] == ["pings_2026-01-01.parquet", "pings_2026-01-02.parquet"]
    single = tmp_path / "pings_2026-01-01.parquet"
    assert sj.list_ping_files(single) == [single]
    with pytest.raises(FileNotFoundError):
        sj.list_ping_files(tmp_path / "missing")
    empty = tmp_path / "empty"
    empty.mkdir()
    with pytest.raises(FileNotFoundError):
        sj.list_ping_files(empty)


@pytest.mark.parametrize("total,n,expected", [(16, 1, 16), (16, 30, 1), (16, 4, 4), (16, 5, 4), (1, 3, 1)])
def test_coalesce_for(total, n, expected):
    assert sj.coalesce_for(total, n) == expected


def test_coalesce_for_rejects_bad_input():
    with pytest.raises(ValueError):
        sj.coalesce_for(0, 1)


def test_spark_path_is_absolute_with_forward_slashes():
    p = sj.spark_path("data/raw")
    assert "\\" not in p and Path(p).is_absolute()


def test_plan_uses_broadcast():
    assert sj.plan_uses_broadcast("BroadcastIndexJoin ...")
    assert not sj.plan_uses_broadcast("RangeJoin ... SortMergeJoin")


def test_format_and_write_audit(tmp_path):
    audit = {"rows_in": 1_000_000, "rows_out": 400_000, "by_day": False, "runtime_seconds": 12.5}
    text = sj.format_audit(audit)
    assert "1,000,000" in text and "rows_out" in text
    path = sj.write_audit(audit, tmp_path / "sub" / "audit.json")
    assert json.loads(path.read_text())["rows_out"] == 400_000


def test_parser_defaults_and_flags():
    a = sj.build_parser().parse_args([])
    assert (a.pings, a.stores, a.out) == (sj.DEFAULT_PINGS, sj.DEFAULT_STORES, sj.DEFAULT_OUT)
    assert not a.by_day and not a.audit
    b = sj.build_parser().parse_args(["--by-day", "--audit", "--driver-memory", "4g", "--shuffle-partitions", "8"])
    assert b.by_day and b.audit and b.driver_memory == "4g" and b.shuffle_partitions == 8


def test_main_fails_fast_with_exit_code_1(tmp_path):
    assert sj.main(["--pings", str(tmp_path / "nope.parquet")]) == 1


# ---------------------------------------------------------------------------- demo pings ---
def test_demo_pings_pass_the_input_validator():
    df = demo.make_pings(2_000)
    assert list(df.columns) == ["device_id", "lat", "lon", "ts"] and len(df) == 2_000 + len(vi.PLANTED_EXPECTED)
    stats = vi.PingStats()
    stats.update(df)
    assert [r.name for r in vi.ping_checks(stats) if not r.passed] == []


def test_demo_pings_are_deterministic():
    pd.testing.assert_frame_equal(demo.make_pings(500, seed=7), demo.make_pings(500, seed=7))


# ------------------------------------------------------------------------ live Sedona run ---
@pytest.fixture(scope="module")
def sedona_session():
    pytest.importorskip("pyspark")
    pytest.importorskip("sedona.spark")
    pytest.importorskip("pyarrow")
    from src.spatial import sedona_config as sc
    sc.configure_environment()
    if shutil.which("java") is None and not os.environ.get("JAVA_HOME"):
        pytest.skip("Java 17 is not available")
    session = sc.get_sedona(app_name="geopulse-join-contract-tests", driver_memory="2g", shuffle_partitions=4)
    yield session
    session.stop()


@pytest.fixture(scope="module")
def tiny_inputs(tmp_path_factory):
    """3,000 random pings + the 6 planted points, device_id = D<row index>, split into two daily files."""
    if not STORES_CSV.exists():
        pytest.skip("run `python -m src.spatial.generate_stores` first")
    pts, plant_idx, _ = vi.build_smoke_points(n_rows=3_000)
    n = len(pts)
    rng = np.random.default_rng(5)
    ts = (pd.Timestamp("2026-01-05") + pd.to_timedelta(rng.integers(0, 2 * 86_400, n), unit="s")).astype("datetime64[ms]")
    df = pd.DataFrame({"device_id": [f"D{i:06d}" for i in range(n)], "lat": pts[:, 0], "lon": pts[:, 1], "ts": ts})
    root = tmp_path_factory.mktemp("join")
    pings_dir = root / "pings"
    demo.write_pings(df, pings_dir, split_days=True)
    stores = vi.load_stores(STORES_CSV)
    rings = {sid: vi.parse_wkt_polygon(w) for sid, w in zip(stores["store_id"], stores["catchment_wkt"])
             if sid in ("S0001", "S0002")}
    return {"dir": pings_dir, "root": root, "points": pts, "expected": vi.expected_matches(pts, rings),
            "plant_idx": plant_idx, "n": n}


def _a_b_matches(sedona, out):
    rows = (sedona.read.parquet(sj.spark_path(out)).where("store_id IN ('S0001', 'S0002')")
            .select("device_id", "store_id").collect())
    return {(int(r["device_id"][1:]), r["store_id"]) for r in rows}


def test_join_matches_numpy_and_contract(sedona_session, tiny_inputs):
    out = tiny_inputs["root"] / "out_all"
    audit = sj.run_join(sedona_session, tiny_inputs["dir"], STORES_CSV, out, by_day=False)
    assert audit["rows_in"] == tiny_inputs["n"] and audit["rows_out"] > 0 and audit["stores"] == 5000
    assert all(r.passed for r in sj.verify_output(sedona_session, out)), "contract 9.3 check failed"
    assert _a_b_matches(sedona_session, out) == tiny_inputs["expected"]
    lens = tiny_inputs["plant_idx"]["PLANT_LENS_MID"]
    assert {s for (i, s) in _a_b_matches(sedona_session, out) if i == lens} == {"S0001", "S0002"}   # twice, once per store


def test_by_day_equals_single_run_and_is_idempotent(sedona_session, tiny_inputs):
    out_all, out_day = tiny_inputs["root"] / "out_all2", tiny_inputs["root"] / "out_day"
    a = sj.run_join(sedona_session, tiny_inputs["dir"], STORES_CSV, out_all, by_day=False)
    b = sj.run_join(sedona_session, tiny_inputs["dir"], STORES_CSV, out_day, by_day=True)
    assert b["by_day"] and b["ping_files"] == 2 and a["rows_out"] == b["rows_out"]
    again = sj.run_join(sedona_session, tiny_inputs["dir"], STORES_CSV, out_day, by_day=True)
    assert again["rows_out"] == b["rows_out"]                     # overwrite, never doubled
    assert _a_b_matches(sedona_session, out_day) == tiny_inputs["expected"]