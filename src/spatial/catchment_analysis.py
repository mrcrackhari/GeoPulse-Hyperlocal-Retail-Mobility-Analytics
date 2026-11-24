"""GeoPulse catchment analysis & planted ground-truth verifier (Member 2, Day 8).

Reads data/processed/ping_store_matches/ (contract 9.3), writes
data/processed/catchment_summary.csv (store_id, unique_devices, ping_count, peak_hour),
and checks the planted ground truth from plan section 3.4:
  S0001 = 4,000 devices | S0002 = 2,000 devices | shared = 600 (30.0% / 15.0%)
"""
from __future__ import annotations

import argparse
import logging
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterator, List, Optional, Sequence, Set, Tuple

import numpy as np
import pandas as pd

LOG = logging.getLogger("geopulse.catchment_analysis")

DEFAULT_MATCHES = "data/processed/ping_store_matches"
DEFAULT_OUT = "data/processed/catchment_summary.csv"
DEFAULT_BATCH_ROWS = 500_000

REQUIRED_COLUMNS: Tuple[str, ...] = ("device_id", "store_id", "ts")
SUMMARY_COLUMNS: Tuple[str, ...] = ("store_id", "unique_devices", "ping_count", "peak_hour")

STORE_A_ID = "S0001"
STORE_B_ID = "S0002"
EXPECTED_A_DEVICES = 4_000
EXPECTED_B_DEVICES = 2_000
EXPECTED_SHARED_DEVICES = 600
EXPECTED_B_TO_A_PCT = 30.0
EXPECTED_A_TO_B_PCT = 15.0
PCT_TOLERANCE = 3.0


@dataclass(frozen=True)
class CheckResult:
    name: str
    passed: bool
    detail: str = ""

    def __str__(self) -> str:
        tag = "PASS" if self.passed else "FAIL"
        return f"[{tag}] {self.name}" + (f" - {self.detail}" if self.detail else "")


def _res(name: str, ok: bool, ok_detail: str = "", fail_detail: str = "") -> CheckResult:
    return CheckResult(name, bool(ok), ok_detail if ok else fail_detail)


def list_match_files(path) -> List[Path]:
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"matches path {p} does not exist")
    if p.is_file():
        if p.suffix.lower() != ".parquet":
            raise ValueError(f"unsupported file type {p.suffix!r} (expected .parquet)")
        return [p]
    files = sorted(f for f in p.glob("*.parquet") if f.is_file())
    if not files:
        raise FileNotFoundError(f"no *.parquet files found inside {p}")
    return files


def iter_match_batches(path, batch_rows: int = DEFAULT_BATCH_ROWS) -> Iterator[pd.DataFrame]:
    import pyarrow.parquet as pq  # type: ignore

    for f in list_match_files(path):
        pf = pq.ParquetFile(f)
        names = set(pf.schema_arrow.names)
        missing = [c for c in REQUIRED_COLUMNS if c not in names]
        if missing:
            raise ValueError(f"{f} is missing required columns {missing}; found {sorted(names)}")
        for batch in pf.iter_batches(batch_size=batch_rows, columns=list(REQUIRED_COLUMNS)):
            df = batch.to_pandas()
            if len(df):
                yield df


def analyze_matches(
    matches_path,
    batch_rows: int = DEFAULT_BATCH_ROWS,
) -> Tuple[pd.DataFrame, Dict[str, object]]:
    t0 = time.time()
    files = list_match_files(matches_path)

    total_rows = 0
    hour_counts_parts: List[pd.DataFrame] = []
    pair_parts: List[pd.DataFrame] = []
    devices_a: Set[str] = set()
    devices_b: Set[str] = set()

    for batch in iter_match_batches(matches_path, batch_rows=batch_rows):
        total_rows += len(batch)
        ts = pd.to_datetime(batch["ts"], errors="coerce")
        if ts.isna().any():
            raise ValueError(f"found {int(ts.isna().sum()):,} unparseable/null timestamps")

        store_col = batch["store_id"].astype(str)
        dev_col = batch["device_id"].astype(str)

        hc = (
            pd.DataFrame({"store_id": store_col, "hour": ts.dt.hour.astype(np.int16)})
            .groupby(["store_id", "hour"], sort=False)
            .size()
            .rename("ping_count")
            .reset_index()
        )
        hour_counts_parts.append(hc)

        pairs = pd.DataFrame({"store_id": store_col, "device_id": dev_col}).drop_duplicates()
        pair_parts.append(pairs)
        if len(pair_parts) >= 8:
            pair_parts = [pd.concat(pair_parts, ignore_index=True).drop_duplicates()]

        a_mask = pairs["store_id"] == STORE_A_ID
        if a_mask.any():
            devices_a.update(pairs.loc[a_mask, "device_id"].tolist())
        b_mask = pairs["store_id"] == STORE_B_ID
        if b_mask.any():
            devices_b.update(pairs.loc[b_mask, "device_id"].tolist())

    if total_rows == 0:
        return pd.DataFrame(columns=list(SUMMARY_COLUMNS)), {
            "match_files": len(files),
            "total_match_rows": 0,
            "stores_hit": 0,
            "s0001_devices": 0,
            "s0002_devices": 0,
            "shared_devices": 0,
            "pct_b_also_a": None,
            "pct_a_also_b": None,
            "runtime_seconds": round(time.time() - t0, 2),
        }

    hour_totals = (
        pd.concat(hour_counts_parts, ignore_index=True)
        .groupby(["store_id", "hour"], as_index=False)["ping_count"]
        .sum()
    )
    ping_totals = hour_totals.groupby("store_id", as_index=False)["ping_count"].sum()
    peak_hours = (
        hour_totals.sort_values(["store_id", "ping_count", "hour"], ascending=[True, False, True])
        .drop_duplicates(subset=["store_id"], keep="first")[["store_id", "hour"]]
        .rename(columns={"hour": "peak_hour"})
    )

    all_pairs = pd.concat(pair_parts, ignore_index=True).drop_duplicates()
    unique_devs = (
        all_pairs.groupby("store_id", as_index=False)["device_id"]
        .count()
        .rename(columns={"device_id": "unique_devices"})
    )

    summary = (
        unique_devs.merge(ping_totals, on="store_id", how="inner")
        .merge(peak_hours, on="store_id", how="inner")
        .sort_values("store_id", ascending=True)
        .reset_index(drop=True)
    )
    for col in ("unique_devices", "ping_count", "peak_hour"):
        summary[col] = summary[col].astype(np.int64)
    summary = summary[list(SUMMARY_COLUMNS)]

    n_a, n_b = len(devices_a), len(devices_b)
    n_shared = len(devices_a & devices_b)
    pct_b_to_a = round(100.0 * n_shared / n_b, 1) if n_b > 0 else None
    pct_a_to_b = round(100.0 * n_shared / n_a, 1) if n_a > 0 else None

    stats: Dict[str, object] = {
        "match_files": len(files),
        "total_match_rows": int(total_rows),
        "stores_hit": int(len(summary)),
        "s0001_devices": int(n_a),
        "s0002_devices": int(n_b),
        "shared_devices": int(n_shared),
        "pct_b_also_a": pct_b_to_a,
        "pct_a_also_b": pct_a_to_b,
        "runtime_seconds": round(time.time() - t0, 2),
    }
    return summary, stats


def check_summary_and_ground_truth(
    summary: pd.DataFrame,
    stats: Dict[str, object],
    strict_exact: bool = True,
) -> List[CheckResult]:
    results: List[CheckResult] = [
        _res(
            "summary: exact columns",
            list(summary.columns) == list(SUMMARY_COLUMNS),
            ", ".join(SUMMARY_COLUMNS),
            f"got {list(summary.columns)}",
        ),
        _res(
            "summary: has rows",
            len(summary) > 0,
            f"{len(summary):,} stores from {int(stats['total_match_rows']):,} matches",
            "0 rows",
        ),
        _res(
            "summary: no nulls and valid peak_hour (0..23)",
            (not summary.isna().any().any())
            and (len(summary) == 0 or bool(summary["peak_hour"].between(0, 23).all())),
            "0 nulls; peak_hour in 0..23",
            "nulls or invalid peak_hour",
        ),
    ]

    n_a, n_b, n_shared = int(stats["s0001_devices"]), int(stats["s0002_devices"]), int(stats["shared_devices"])
    pct_b_to_a, pct_a_to_b = stats["pct_b_also_a"], stats["pct_a_also_b"]

    if strict_exact:
        results.append(_res("planted: S0001 == 4,000", n_a == EXPECTED_A_DEVICES, f"{n_a:,}", f"got {n_a:,}"))
        results.append(_res("planted: S0002 == 2,000", n_b == EXPECTED_B_DEVICES, f"{n_b:,}", f"got {n_b:,}"))
        results.append(_res("planted: S0001 ∩ S0002 == 600", n_shared == EXPECTED_SHARED_DEVICES, f"{n_shared:,}", f"got {n_shared:,}"))

    b_ok = pct_b_to_a is not None and abs(float(pct_b_to_a) - EXPECTED_B_TO_A_PCT) <= PCT_TOLERANCE
    a_ok = pct_a_to_b is not None and abs(float(pct_a_to_b) - EXPECTED_A_TO_B_PCT) <= PCT_TOLERANCE
    results.append(_res("planted: S0002 -> S0001 == 30.0% ± 3%", b_ok, f"{pct_b_to_a}%", f"got {pct_b_to_a}%"))
    results.append(_res("planted: S0001 -> S0002 == 15.0% ± 3%", a_ok, f"{pct_a_to_b}%", f"got {pct_a_to_b}%"))
    return results


def write_summary(summary: pd.DataFrame, out_path) -> Path:
    p = Path(out_path)
    p.parent.mkdir(parents=True, exist_ok=True)
    summary[list(SUMMARY_COLUMNS)].to_csv(p, index=False, lineterminator="\n")
    return p


def build_mock_planted_matches(seed: int = 42) -> pd.DataFrame:
    """Section 9.7 mock satisfying Section 3.4 planted cohorts.
    # TODO(contract): switch to Member 1's real gps_pings.parquet matches
    """
    rng = np.random.default_rng(seed)
    a_only = [f"D{i:06d}" for i in range(1, 3_401)]
    shared = [f"D{i:06d}" for i in range(3_401, 4_001)]
    b_only = [f"D{i:06d}" for i in range(4_001, 5_401)]
    bg_devs = [f"D{i:06d}" for i in range(5_401, 6_401)]

    base_day = pd.Timestamp("2026-01-05 08:00:00")
    rows = []

    def _add(devs: Sequence[str], sid: str, lat: float, lon: float, hour_off: int, n_pings: int = 2):
        total = len(devs) * n_pings
        ts = (base_day + pd.to_timedelta(hour_off, unit="h") + pd.to_timedelta(rng.integers(0, 55, total), unit="m")).astype("datetime64[ms]")
        rows.append(pd.DataFrame({
            "device_id": np.repeat(devs, n_pings),
            "store_id": sid,
            "ts": ts,
            "lat": np.full(total, lat, dtype=np.float64),
            "lon": np.full(total, lon, dtype=np.float64),
        }))

    _add(a_only, STORE_A_ID, 12.9716, 77.5920, 0, 2)
    _add(shared, STORE_A_ID, 12.9716, 77.5920, 0, 2)
    _add(shared, STORE_B_ID, 12.9716, 77.6035, 0, 2)
    _add(b_only, STORE_B_ID, 12.9716, 77.6035, 0, 2)
    _add(bg_devs, "S0003", 13.0839, 77.2609, 10, 3)
    return pd.concat(rows, ignore_index=True)


def write_mock_planted_matches(out_path, seed: int = 42) -> Path:
    df = build_mock_planted_matches(seed=seed)
    p = Path(out_path)
    p.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(p, engine="pyarrow", index=False, coerce_timestamps="ms", allow_truncated_timestamps=True)
    return p


def print_report(results: Sequence[CheckResult], stats: Dict[str, object], out_path: Path) -> bool:
    print("CATCHMENT ANALYSIS SUMMARY")
    print("-" * 60)
    print(f"matches read       : {int(stats['total_match_rows']):,} ({stats['match_files']} file(s))")
    print(f"stores summarized  : {int(stats['stores_hit']):,} -> {out_path}")
    print(f"S0001 / S0002 / ∩  : {int(stats['s0001_devices']):,} / {int(stats['s0002_devices']):,} / {int(stats['shared_devices']):,}")
    print(f"B->A / A->B overlap: {stats['pct_b_also_a']}% / {stats['pct_a_also_b']}%")
    print("-" * 60)
    for r in results:
        print(r)
    failed = [r for r in results if not r.passed]
    print("-" * 60)
    print("CATCHMENT ANALYSIS: PASS" if not failed else "CATCHMENT ANALYSIS: FAIL")
    return not failed


def build_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(description="Aggregate ping-store matches and verify planted ground truth.")
    ap.add_argument("--matches", default=DEFAULT_MATCHES)
    ap.add_argument("--out", default=DEFAULT_OUT)
    ap.add_argument("--batch-rows", type=int, default=DEFAULT_BATCH_ROWS)
    ap.add_argument("--skip-planted-assert", action="store_true")
    ap.add_argument("--make-mock-matches", default=None)
    ap.add_argument("--verbose", action="store_true")
    return ap


def main(argv: Optional[list] = None) -> int:
    args = build_parser().parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    try:
        if args.make_mock_matches:
            out = write_mock_planted_matches(args.make_mock_matches)
            print(f"Wrote mock planted matches -> {out}")
            return 0
        summary, stats = analyze_matches(args.matches, batch_rows=args.batch_rows)
        out_path = write_summary(summary, args.out)
        results = check_summary_and_ground_truth(summary, stats, strict_exact=True)
        passed = print_report(results, stats, out_path)
        if args.skip_planted_assert:
            return 0 if all(r.passed for r in results if r.name.startswith("summary:")) else 1
        return 0 if passed else 1
    except Exception as exc:  # noqa: BLE001
        LOG.error("catchment analysis failed: %s: %s", type(exc).__name__, exc, exc_info=args.verbose)
        return 1


if __name__ == "__main__":
    sys.exit(main())