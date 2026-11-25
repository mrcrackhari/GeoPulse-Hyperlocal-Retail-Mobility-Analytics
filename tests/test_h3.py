"""Tests for src/spatial/hexagon_aggregation.py (Member 2, Day 11).

Covers:
- Known (lat, lon) -> H3 index and lat-first coordinate order guard
- Hex centers inside the city bounding box
- resolution column equals --res
- Contract 9.4 schema, grain uniqueness, day_type classification, and ping_count conservation
- PyArrow + live Spark pandas_udf aggregation and fail-fast CLI exit codes
"""
from __future__ import annotations

import os
import shutil
import sys
from pathlib import Path

import pandas as pd
import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from src.spatial import hexagon_aggregation as hx  # noqa: E402

h3 = pytest.importorskip("h3")
pytest.importorskip("pyarrow")

STORE_A_LAT, STORE_A_LON = 12.9716, 77.5946
STORE_B_LAT, STORE_B_LON = 12.9716, 77.6011


def test_contract_columns_and_ddl():
    assert hx.HEX_COLUMNS == (
        "hex_id",
        "resolution",
        "hour_of_day",
        "day_type",
        "ping_count",
        "unique_devices",
        "center_lat",
        "center_lon",
    )
    assert hx.HEX_DDL == (
        "hex_id string, resolution int, hour_of_day int, day_type string, "
        "ping_count bigint, unique_devices bigint, center_lat double, center_lon double"
    )


def test_schema_problems_accepts_contract_and_catches_mismatch():
    ok = [(c, hx.HEX_TYPES[c]) for c in hx.HEX_COLUMNS]
    assert hx.schema_problems(ok) == []
    assert hx.schema_problems(ok[:-1])
    wrong_type = [(c, "int" if c == "ping_count" else t) for c, t in ok]
    assert any("ping_count is int" in p for p in hx.schema_problems(wrong_type))


def test_known_latlon_to_h3_index_and_lat_first_guard():
    """Known (lat, lon) maps to exact H3 index at res 8; swapped (lon, lat) lands outside city bbox."""
    expected_fn = h3.geo_to_h3 if hasattr(h3, "geo_to_h3") else h3.latlng_to_cell
    expected = expected_fn(STORE_A_LAT, STORE_A_LON, 8)

    got = hx.latlon_to_h3(STORE_A_LAT, STORE_A_LON, res=8)
    assert got == expected
    assert hx.h3_is_valid(got)
    assert hx.h3_resolution(got) == 8

    clat, clon = hx.h3_to_latlon(got)
    assert hx.MIN_LAT <= clat <= hx.MAX_LAT
    assert hx.MIN_LON <= clon <= hx.MAX_LON
    assert abs(clat - STORE_A_LAT) < 0.01 and abs(clon - STORE_A_LON) < 0.01

    swapped = hx.latlon_to_h3(STORE_A_LON, STORE_A_LAT, res=8)
    assert swapped != got
    slat, slon = hx.h3_to_latlon(swapped)
    assert not (hx.MIN_LAT <= slat <= hx.MAX_LAT and hx.MIN_LON <= slon <= hx.MAX_LON)


def test_classify_day_type_weekday_and_weekend():
    ts = pd.Series(pd.to_datetime([
        "2026-01-01 08:00:00",  # Thu -> weekday
        "2026-01-02 18:00:00",  # Fri -> weekday
        "2026-01-03 10:00:00",  # Sat -> weekend
        "2026-01-04 12:00:00",  # Sun -> weekend
        "2026-01-05 09:00:00",  # Mon -> weekday
    ]))
    assert hx.classify_day_type_series(ts).tolist() == [
        "weekday", "weekday", "weekend", "weekend", "weekday"
    ]


def _sample_pings_parquet(tmp_path: Path) -> Path:
    df = pd.DataFrame({
        "device_id": [
            "D000001", "D000001", "D000002",
            "D000003", "D000003",
            "D000004",
        ],
        "lat": [STORE_A_LAT, STORE_A_LAT, STORE_A_LAT, STORE_A_LAT, STORE_A_LAT, STORE_B_LAT],
        "lon": [STORE_A_LON, STORE_A_LON, STORE_A_LON, STORE_A_LON, STORE_A_LON, STORE_B_LON],
        "ts": pd.to_datetime([
            "2026-01-05 08:05:00",  # Mon (weekday)
            "2026-01-05 08:15:00",  # Mon (weekday)
            "2026-01-06 08:25:00",  # Tue (weekday)
            "2026-01-03 08:10:00",  # Sat (weekend)
            "2026-01-04 08:40:00",  # Sun (weekend)
            "2026-01-05 18:30:00",  # Mon (weekday)
        ]).astype("datetime64[ms]"),
    })
    p = tmp_path / "tiny_pings.parquet"
    df.to_parquet(p, engine="pyarrow", index=False, coerce_timestamps="ms", allow_truncated_timestamps=True)
    return p


def test_pyarrow_aggregation_matches_contract_and_counts(tmp_path):
    pings_pq = _sample_pings_parquet(tmp_path)
    out_dir = tmp_path / "h3_out"
    rc = hx.main(["--in", str(pings_pq), "--out", str(out_dir), "--res", "8", "--engine", "pyarrow"])
    assert rc == 0

    df = pd.read_parquet(out_dir)
    assert list(df.columns) == list(hx.HEX_COLUMNS)
    assert (df["resolution"] == 8).all()
    assert int(df["ping_count"].sum()) == 6
    assert len(df) == 3

    hex_a = hx.latlon_to_h3(STORE_A_LAT, STORE_A_LON, 8)
    wk_a = df[(df["hex_id"] == hex_a) & (df["day_type"] == "weekday") & (df["hour_of_day"] == 8)].iloc[0]
    assert int(wk_a["ping_count"]) == 3 and int(wk_a["unique_devices"]) == 2

    we_a = df[(df["hex_id"] == hex_a) & (df["day_type"] == "weekend") & (df["hour_of_day"] == 8)].iloc[0]
    assert int(we_a["ping_count"]) == 2 and int(we_a["unique_devices"]) == 1


def test_resolution_flag_respected(tmp_path):
    pings_pq = _sample_pings_parquet(tmp_path)
    out_dir = tmp_path / "h3_res7"
    assert hx.main(["--in", str(pings_pq), "--out", str(out_dir), "--res", "7", "--engine", "pyarrow"]) == 0
    df = pd.read_parquet(out_dir)
    assert (df["resolution"] == 7).all()
    assert all(hx.h3_resolution(h) == 7 for h in df["hex_id"])


def test_cli_fails_fast_on_missing_input_and_bad_res(tmp_path):
    assert hx.main(["--in", str(tmp_path / "missing.parquet"), "--out", str(tmp_path / "out")]) == 1
    assert hx.main(["--in", str(tmp_path / "missing.parquet"), "--out", str(tmp_path / "out"), "--res", "99"]) == 1


@pytest.fixture(scope="module")
def sedona_session():
    pytest.importorskip("pyspark")
    pytest.importorskip("sedona.spark")
    from src.spatial import sedona_config as sc
    sc.configure_environment()
    if shutil.which("java") is None and not os.environ.get("JAVA_HOME"):
        pytest.skip("Java 17 is not available")
    session = sc.get_sedona(app_name="geopulse-h3-tests", driver_memory="2g", shuffle_partitions=4)
    yield session
    session.stop()


def test_spark_pandas_udf_matches_pyarrow_reference(sedona_session, tmp_path):
    pings_pq = _sample_pings_parquet(tmp_path)
    out_spark = tmp_path / "h3_spark"
    out_arrow = tmp_path / "h3_arrow"

    audit_spark = hx.run_spark_aggregation(sedona_session, pings_pq, out_spark, res=8, coalesce=1)
    audit_arrow = hx.run_pyarrow_aggregation(pings_pq, out_arrow, res=8)
    assert audit_spark["rows_in"] == audit_arrow["rows_in"] == 6
    assert audit_spark["rows_out"] == audit_arrow["rows_out"] == 3
    assert all(r.passed for r in hx.verify_output(out_spark, expected_res=8, expected_total_pings=6))

    df_s = pd.read_parquet(out_spark).sort_values(["hex_id", "day_type", "hour_of_day"]).reset_index(drop=True)
    df_a = pd.read_parquet(out_arrow).sort_values(["hex_id", "day_type", "hour_of_day"]).reset_index(drop=True)
    pd.testing.assert_frame_equal(df_s, df_a, check_dtype=True)