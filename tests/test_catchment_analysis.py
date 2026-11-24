"""Tests for src/spatial/catchment_analysis.py (Member 2, Day 8)."""
from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from src.spatial import catchment_analysis as ca  # noqa: E402

pytest.importorskip("pyarrow")


def test_contract_and_ground_truth_constants():
    assert ca.SUMMARY_COLUMNS == ("store_id", "unique_devices", "ping_count", "peak_hour")
    assert (ca.EXPECTED_A_DEVICES, ca.EXPECTED_B_DEVICES, ca.EXPECTED_SHARED_DEVICES) == (4000, 2000, 600)


def test_list_match_files(tmp_path):
    (tmp_path / "part-00001.parquet").write_text("x")
    (tmp_path / "part-00000.parquet").write_text("x")
    assert [f.name for f in ca.list_match_files(tmp_path)] == ["part-00000.parquet", "part-00001.parquet"]
    with pytest.raises(FileNotFoundError):
        ca.list_match_files(tmp_path / "missing")


def test_analyze_mock_planted_matches_passes_ground_truth(tmp_path):
    pq = tmp_path / "planted.parquet"
    ca.write_mock_planted_matches(pq, seed=42)
    out = tmp_path / "summary.csv"
    assert ca.main(["--matches", str(pq), "--out", str(out), "--batch-rows", "4000"]) == 0
    s = pd.read_csv(out).set_index("store_id")
    assert int(s.loc["S0001", "unique_devices"]) == 4000
    assert int(s.loc["S0002", "unique_devices"]) == 2000
    assert int(s.loc["S0001", "peak_hour"]) == 8
    assert int(s.loc["S0003", "peak_hour"]) == 18


def test_peak_hour_tie_breaks_to_lowest_hour(tmp_path):
    df = pd.DataFrame({
        "device_id": ["D000001", "D000001", "D000002", "D000002"],
        "store_id": ["S0010"] * 4,
        "ts": pd.to_datetime(["2026-01-05 18:10", "2026-01-05 18:45", "2026-01-05 08:15", "2026-01-05 08:50"]).astype("datetime64[ms]"),
        "lat": [12.95] * 4,
        "lon": [77.59] * 4,
    })
    pq = tmp_path / "tie.parquet"
    df.to_parquet(pq, index=False)
    summary, _ = ca.analyze_matches(pq, batch_rows=2)
    assert int(summary.iloc[0]["peak_hour"]) == 8


def test_planted_ground_truth_failure_detected(tmp_path):
    df = ca.build_mock_planted_matches(seed=42)
    drop_ids = {f"D{i:06d}" for i in range(4_001, 4_501)}
    broken = df[~((df["store_id"] == "S0002") & (df["device_id"].isin(drop_ids)))]
    pq = tmp_path / "broken.parquet"
    broken.to_parquet(pq, index=False)
    assert ca.main(["--matches", str(pq), "--out", str(tmp_path / "out.csv")]) == 1


def test_skip_planted_assert_flag(tmp_path):
    df = pd.DataFrame({
        "device_id": ["D000001", "D000002"],
        "store_id": ["S0001", "S0002"],
        "ts": pd.to_datetime(["2026-01-01 08:00", "2026-01-01 18:00"]).astype("datetime64[ms]"),
        "lat": [12.9716, 12.9716],
        "lon": [77.5946, 77.6011],
    })
    pq = tmp_path / "standin.parquet"
    df.to_parquet(pq, index=False)
    out = tmp_path / "standin.csv"
    assert ca.main(["--matches", str(pq), "--out", str(out)]) == 1
    assert ca.main(["--matches", str(pq), "--out", str(out), "--skip-planted-assert"]) == 0


def test_cli_fails_fast_on_missing_file(tmp_path):
    assert ca.main(["--matches", str(tmp_path / "missing.parquet")]) == 1
    