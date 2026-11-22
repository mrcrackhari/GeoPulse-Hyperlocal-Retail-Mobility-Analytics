"""Stand-in GPS pings for testing the spatial join before Member 1's simulator sample exists (Member 2).

Writes pings in the GPS_PINGS contract (device_id, lat, lon, ts): uniformly random points in the city
box plus six planted points around Store A / Store B whose store membership is known. This is NOT the
real simulator data - it only lets you run and time ``spatial_join.py`` early.

    python -m src.spatial.make_demo_pings                           # 1,000,000 rows -> data/raw/demo_pings_1m.parquet
    python -m src.spatial.make_demo_pings --rows 200000 --out data/raw/demo_days --split-days
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import numpy as np
import pandas as pd

try:
    from src.spatial import validate_inputs as vi
except ImportError:  # run as a plain script
    import validate_inputs as vi  # type: ignore

N_DEVICES = 25_000
DAYS = 30
START = pd.Timestamp("2026-01-01")


def make_pings(rows: int, seed: int = 42) -> pd.DataFrame:
    """``rows`` random pings + 6 planted points (appended last). ts is naive city-local time."""
    pts, _, _ = vi.build_smoke_points(n_rows=rows, seed=seed)
    rng = np.random.default_rng(seed + 1)
    n = len(pts)
    seconds = rng.integers(0, DAYS * 86_400, size=n)
    return pd.DataFrame({
        "device_id": [f"D{(i % N_DEVICES) + 1:06d}" for i in range(n)],
        "lat": pts[:, 0],
        "lon": pts[:, 1],
        "ts": (START + pd.to_timedelta(seconds, unit="s")).astype("datetime64[ms]"),
    })


def iter_demo_days(rows: int, seed: int = 42):
    """Yield (YYYY-MM-DD, DataFrame) for 30 days, ~rows/30 each, so 10 M rows never sit in memory at once.

    Same schema as make_pings; the 6 planted points go into day 1.
    """
    per_day = rows // DAYS
    plant = vi.planted_points()
    for d in range(DAYS):
        rng = np.random.default_rng(seed + d)
        lat = vi.MIN_LAT + (vi.MAX_LAT - vi.MIN_LAT) * rng.random(per_day)
        lon = vi.MIN_LON + (vi.MAX_LON - vi.MIN_LON) * rng.random(per_day)
        if d == 0:
            lat = np.concatenate([lat, [p[0] for p in plant.values()]])
            lon = np.concatenate([lon, [p[1] for p in plant.values()]])
        n = len(lat)
        day = START + pd.Timedelta(days=d)
        ids = pd.Series(rng.integers(1, N_DEVICES + 1, size=n)).astype(str).str.zfill(6)
        yield f"{day:%Y-%m-%d}", pd.DataFrame({
            "device_id": "D" + ids,
            "lat": lat, "lon": lon,
            "ts": (day + pd.to_timedelta(rng.integers(0, 86_400, size=n), unit="s")).astype("datetime64[ms]"),
        })


def write_pings(df: pd.DataFrame, out: Path, split_days: bool) -> int:
    """Write one Parquet file, or (split_days) one file per day pings_YYYY-MM-DD.parquet; returns file count."""
    kw = dict(engine="pyarrow", index=False, coerce_timestamps="ms", allow_truncated_timestamps=True)
    if not split_days:
        out.parent.mkdir(parents=True, exist_ok=True)
        df.to_parquet(out, **kw)
        return 1
    out.mkdir(parents=True, exist_ok=True)
    days = df["ts"].dt.strftime("%Y-%m-%d")
    for day, part in df.groupby(days):
        part.to_parquet(out / f"pings_{day}.parquet", **kw)
    return int(days.nunique())


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="Write stand-in pings for testing spatial_join.py")
    ap.add_argument("--rows", type=int, default=1_000_000)
    ap.add_argument("--out", default="data/raw/demo_pings_1m.parquet")
    ap.add_argument("--split-days", action="store_true", help="--out is a folder of daily files")
    ap.add_argument("--seed", type=int, default=42)
    args = ap.parse_args(argv)
    if args.split_days:                      # chunked path: one day at a time (works for 10 M rows)
        out = Path(args.out)
        out.mkdir(parents=True, exist_ok=True)
        total = 0
        for day, part in iter_demo_days(args.rows, args.seed):
            part.to_parquet(out / f"pings_{day}.parquet", engine="pyarrow", index=False,
                            coerce_timestamps="ms", allow_truncated_timestamps=True)
            total += len(part)
        print(f"wrote {total:,} rows in {DAYS} daily files to {args.out}")
        return 0
    df = make_pings(args.rows, args.seed)
    n_files = write_pings(df, Path(args.out), args.split_days)
    print(f"wrote {len(df):,} rows in {n_files} file(s) to {args.out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())