"""GeoPulse H3 hexagon aggregation (Member 2, Day 11).

Reads all GPS pings (Contract 9.1), assigns each ping to an H3 hexagon at resolution ``--res``
(default 8, avg hex area ~0.74 km^2, edge ~0.46 km - close to a 500 m catchment), derives
``hour_of_day`` (0..23) and ``day_type`` ('weekday' | 'weekend'), aggregates ``ping_count`` and
``unique_devices`` per ``(hex_id, hour_of_day, day_type)``, attaches each hex's ``(center_lat,
center_lon)``, and writes Parquet output matching Contract 9.4 (``H3_HEXBINS``) to
``data/processed/h3_hexbins/``.

Coordinate order guard (Section 0.8 / 8.6)
------------------------------------------
    H3 takes LATITUDE FIRST        : h3.geo_to_h3(lat, lon, res)
    Sedona / Snowflake LONGITUDE   : ST_Point(lon, lat)

Usage (repository root)
-----------------------
    python -m src.spatial.hexagon_aggregation --in data/raw/demo_pings_1m.parquet --out data/processed/h3_hexbins --res 8 --audit
    python -m src.spatial.hexagon_aggregation --in data/raw/gps_pings.parquet --out data/processed/h3_hexbins --res 8 --audit

Exit code 0 = aggregation finished and Contract 9.4 checks passed; 1 = failure.
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

import numpy as np
import pandas as pd

LOG = logging.getLogger("geopulse.hexagon_aggregation")

# ------------------------------------------------------------------ Contract 9.4 & constants ---
HEX_COLUMNS: Tuple[str, ...] = (
    "hex_id",
    "resolution",
    "hour_of_day",
    "day_type",
    "ping_count",
    "unique_devices",
    "center_lat",
    "center_lon",
)
HEX_TYPES: Dict[str, str] = {
    "hex_id": "string",
    "resolution": "int",
    "hour_of_day": "int",
    "day_type": "string",
    "ping_count": "bigint",
    "unique_devices": "bigint",
    "center_lat": "double",
    "center_lon": "double",
}
HEX_DDL = ", ".join(f"{c} {HEX_TYPES[c]}" for c in HEX_COLUMNS)

MIN_LAT, MAX_LAT = 12.50, 13.40
MIN_LON, MAX_LON = 77.05, 77.95
HEX_BBOX_MARGIN_DEG = 0.02          # res-8 hex center can sit ~0.5 km outside bbox edge

DEFAULT_IN = "data/raw/gps_pings_sample.parquet"
DEFAULT_OUT = "data/processed/h3_hexbins"
DEFAULT_AUDIT = "data/processed/h3_audit.json"
DEFAULT_RES = 8
DEFAULT_COALESCE = 8
DEFAULT_BATCH_ROWS = 500_000

PINGS_FALLBACKS = (
    "data/raw/gps_pings_sample.parquet",
    "data/raw/demo_pings_1m.parquet",
    "data/raw/gps_pings.parquet",
)


@dataclass(frozen=True)
class CheckResult:
    """One output verification result."""

    name: str
    passed: bool
    detail: str = ""

    def __str__(self) -> str:
        tag = "PASS" if self.passed else "FAIL"
        return f"[{tag}] {self.name}" + (f" - {self.detail}" if self.detail else "")


def _res(name: str, ok: bool, ok_detail: str = "", fail_detail: str = "") -> CheckResult:
    return CheckResult(name, bool(ok), ok_detail if ok else fail_detail)


# =========================================================================================
# H3 wrappers (locked to h3==3.7.7 API, with graceful v4 compatibility)
# =========================================================================================
def _import_h3():
    try:
        import h3  # type: ignore
    except ImportError as exc:
        raise ImportError('h3 is required: pip install "h3==3.7.7"') from exc
    return h3


def latlon_to_h3(lat: float, lon: float, res: int = DEFAULT_RES) -> str:
    """Convert (lat, lon) -> H3 hex index at ``res``. LATITUDE FIRST."""
    h3 = _import_h3()
    if hasattr(h3, "geo_to_h3"):
        return str(h3.geo_to_h3(float(lat), float(lon), int(res)))
    return str(h3.latlng_to_cell(float(lat), float(lon), int(res)))


def h3_to_latlon(hex_id: str) -> Tuple[float, float]:
    """Convert H3 hex index -> (center_lat, center_lon)."""
    h3 = _import_h3()
    if hasattr(h3, "h3_to_geo"):
        la, lo = h3.h3_to_geo(hex_id)
    else:
        la, lo = h3.cell_to_latlng(hex_id)
    return float(la), float(lo)


def h3_resolution(hex_id: str) -> int:
    """Return the H3 resolution of ``hex_id``."""
    h3 = _import_h3()
    if hasattr(h3, "h3_get_resolution"):
        return int(h3.h3_get_resolution(hex_id))
    return int(h3.get_resolution(hex_id))


def h3_is_valid(hex_id: str) -> bool:
    """Return True if ``hex_id`` is a valid H3 cell string."""
    h3 = _import_h3()
    if hasattr(h3, "h3_is_valid"):
        return bool(h3.h3_is_valid(hex_id))
    return bool(h3.is_valid_cell(hex_id))


# =========================================================================================
# Pure helpers (unit tested without Spark)
# =========================================================================================
def list_ping_files(path) -> List[Path]:
    """Return sorted Parquet file(s) behind ``path``."""
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"pings path {p} does not exist")
    if p.is_file():
        return [p]
    files = sorted(f for f in p.glob("*.parquet") if f.is_file())
    if not files:
        raise FileNotFoundError(f"no *.parquet files inside {p}")
    return files


def resolve_pings_path(path) -> Path:
    """Return ``path`` if present, else first existing fallback (used only when --in is default)."""
    if Path(path).exists():
        return Path(path)
    for cand in PINGS_FALLBACKS:
        if Path(cand).exists():
            LOG.warning("pings path %s does not exist - using %s instead", path, cand)
            return Path(cand)
    raise FileNotFoundError(
        f"pings path {path} does not exist and no fallback was found in data/raw. "
        "Create a 1M stand-in: python -m src.spatial.make_demo_pings"
    )


def spark_path(path) -> str:
    return Path(path).resolve().as_posix()


def schema_problems(fields: Sequence[Tuple[str, str]]) -> List[str]:
    """Compare ``[(name, spark_type)]`` with Contract 9.4. Empty list means exact match."""
    got = [(n.lower(), t.lower()) for n, t in fields]
    want = [(c, HEX_TYPES[c]) for c in HEX_COLUMNS]
    if got == want:
        return []
    problems: List[str] = []
    if [n for n, _ in got] != list(HEX_COLUMNS):
        problems.append(f"columns are {[n for n, _ in got]}, contract wants {list(HEX_COLUMNS)}")
    for n, t in got:
        if n in HEX_TYPES and HEX_TYPES[n] != t:
            problems.append(f"column {n} is {t}, contract wants {HEX_TYPES[n]}")
    return problems


def classify_day_type_series(ts: pd.Series) -> pd.Series:
    """Map naive timestamps -> 'weekend' (Sat/Sun) or 'weekday' (Mon..Fri)."""
    return pd.Series(np.where(ts.dt.dayofweek >= 5, "weekend", "weekday"), index=ts.index, dtype="object")


def compute_hex_series(lat: pd.Series, lon: pd.Series, res: int = DEFAULT_RES) -> pd.Series:
    """Assign H3 cells with LATITUDE FIRST."""
    h3 = _import_h3()
    fn = h3.geo_to_h3 if hasattr(h3, "geo_to_h3") else h3.latlng_to_cell
    r = int(res)
    return pd.Series([fn(float(la), float(lo), r) for la, lo in zip(lat, lon)], index=lat.index, dtype="object")


def attach_hex_centers(df: pd.DataFrame) -> pd.DataFrame:
    """Add ``center_lat`` and ``center_lon`` columns from unique ``hex_id`` values."""
    if df.empty:
        df["center_lat"] = pd.Series(dtype=np.float64)
        df["center_lon"] = pd.Series(dtype=np.float64)
        return df
    uniq = df["hex_id"].unique()
    centers = {h: h3_to_latlon(h) for h in uniq}
    df["center_lat"] = df["hex_id"].map(lambda h: centers[h][0]).astype(np.float64)
    df["center_lon"] = df["hex_id"].map(lambda h: centers[h][1]).astype(np.float64)
    return df


# =========================================================================================
# Spark + pandas_udf engine (Spec Section 8.6)
# =========================================================================================
def run_spark_aggregation(
    sedona,
    pings_path,
    out_path,
    res: int = DEFAULT_RES,
    coalesce: int = DEFAULT_COALESCE,
) -> Dict[str, Any]:
    """Aggregate all pings into H3 hexbins using a PySpark ``pandas_udf`` (Spec 8.6)."""
    from pyspark.sql import functions as F  # type: ignore
    from pyspark.sql.types import DoubleType, StringType, StructField, StructType  # type: ignore

    files = list_ping_files(pings_path)
    t0 = time.time()

    @F.pandas_udf(StringType())
    def latlon_to_h3_udf(lat: pd.Series, lon: pd.Series) -> pd.Series:
        import h3  # type: ignore

        fn = h3.geo_to_h3 if hasattr(h3, "geo_to_h3") else h3.latlng_to_cell
        r = int(res)
        return pd.Series([fn(float(la), float(lo), r) for la, lo in zip(lat, lon)])

    center_schema = StructType([
        StructField("center_lat", DoubleType(), True),
        StructField("center_lon", DoubleType(), True),
    ])

    @F.pandas_udf(center_schema)
    def h3_center_udf(hex_ids: pd.Series) -> pd.DataFrame:
        import h3  # type: ignore

        fn = h3.h3_to_geo if hasattr(h3, "h3_to_geo") else h3.cell_to_latlng
        coords = [fn(h) for h in hex_ids]
        return pd.DataFrame({
            "center_lat": [float(c[0]) for c in coords],
            "center_lon": [float(c[1]) for c in coords],
        })

    pings_df = sedona.read.parquet(*[spark_path(p) for p in files]).selectExpr(
        "CAST(device_id AS STRING) AS device_id",
        "CAST(lat AS DOUBLE) AS lat",
        "CAST(lon AS DOUBLE) AS lon",
        "CAST(ts AS TIMESTAMP) AS ts",
    )
    rows_in = pings_df.count()

    # Spark dayofweek(ts): 1 = Sunday, 7 = Saturday
    enriched = (
        pings_df
        .withColumn("hex_id", latlon_to_h3_udf(F.col("lat"), F.col("lon")))
        .withColumn("hour_of_day", F.hour(F.col("ts")).cast("int"))
        .withColumn(
            "day_type",
            F.when(F.dayofweek(F.col("ts")).isin(1, 7), F.lit("weekend")).otherwise(F.lit("weekday")),
        )
    )

    grouped = (
        enriched.groupBy("hex_id", "hour_of_day", "day_type")
        .agg(
            F.count("*").cast("bigint").alias("ping_count"),
            F.countDistinct("device_id").cast("bigint").alias("unique_devices"),
        )
        .withColumn("resolution", F.lit(int(res)).cast("int"))
        .withColumn("_c", h3_center_udf(F.col("hex_id")))
        .select(
            F.col("hex_id").cast("string").alias("hex_id"),
            F.col("resolution").cast("int").alias("resolution"),
            F.col("hour_of_day").cast("int").alias("hour_of_day"),
            F.col("day_type").cast("string").alias("day_type"),
            F.col("ping_count").cast("bigint").alias("ping_count"),
            F.col("unique_devices").cast("bigint").alias("unique_devices"),
            F.col("_c.center_lat").cast("double").alias("center_lat"),
            F.col("_c.center_lon").cast("double").alias("center_lon"),
        )
    )

    grouped.coalesce(max(1, int(coalesce))).write.mode("overwrite").parquet(spark_path(out_path))
    seconds = round(time.time() - t0, 2)

    out_df = sedona.read.parquet(spark_path(out_path))
    agg_stats = out_df.selectExpr(
        "count(*) AS rows_out",
        "count(DISTINCT hex_id) AS unique_hexes",
        "sum(ping_count) AS total_pings_out",
    ).collect()[0]

    return {
        "finished_at": datetime.now().isoformat(timespec="seconds"),
        "engine": "spark",
        "pings_path": str(pings_path),
        "out": str(out_path),
        "ping_files": len(files),
        "resolution": int(res),
        "rows_in": int(rows_in),
        "rows_out": int(agg_stats["rows_out"]),
        "unique_hexes": int(agg_stats["unique_hexes"]),
        "total_pings_out": int(agg_stats["total_pings_out"] or 0),
        "runtime_seconds": seconds,
        "driver_memory": sedona.conf.get("spark.driver.memory", "?"),
        "shuffle_partitions": sedona.conf.get("spark.sql.shuffle.partitions", "?"),
    }


# =========================================================================================
# Streaming PyArrow engine (fast pure-Python fallback, identical Contract 9.4 output)
# =========================================================================================
def run_pyarrow_aggregation(
    pings_path,
    out_path,
    res: int = DEFAULT_RES,
    batch_rows: int = DEFAULT_BATCH_ROWS,
) -> Dict[str, Any]:
    """Stream pings in batches via PyArrow and write Contract 9.4 Parquet output."""
    import pyarrow as pa  # type: ignore
    import pyarrow.parquet as pq  # type: ignore

    files = list_ping_files(pings_path)
    t0 = time.time()
    rows_in = 0
    count_parts: List[pd.DataFrame] = []
    dev_parts: List[pd.DataFrame] = []

    for f in files:
        pf = pq.ParquetFile(f)
        for batch in pf.iter_batches(batch_size=batch_rows, columns=["device_id", "lat", "lon", "ts"]):
            df = batch.to_pandas()
            if df.empty:
                continue
            rows_in += len(df)
            ts = pd.to_datetime(df["ts"], errors="coerce")
            if ts.isna().any():
                raise ValueError(f"found {int(ts.isna().sum()):,} unparseable timestamps in {f}")

            work = pd.DataFrame({
                "hex_id": compute_hex_series(df["lat"], df["lon"], res=res),
                "hour_of_day": ts.dt.hour.astype(np.int32),
                "day_type": classify_day_type_series(ts),
                "device_id": df["device_id"].astype(str),
            })

            cnt = (
                work.groupby(["hex_id", "hour_of_day", "day_type"], sort=False)
                .size()
                .rename("ping_count")
                .reset_index()
            )
            count_parts.append(cnt)

            devs = work[["hex_id", "hour_of_day", "day_type", "device_id"]].drop_duplicates()
            dev_parts.append(devs)
            if len(dev_parts) >= 8:
                dev_parts = [pd.concat(dev_parts, ignore_index=True).drop_duplicates()]

    if rows_in == 0:
        result = pd.DataFrame({
            "hex_id": pd.Series(dtype="object"),
            "resolution": pd.Series(dtype=np.int32),
            "hour_of_day": pd.Series(dtype=np.int32),
            "day_type": pd.Series(dtype="object"),
            "ping_count": pd.Series(dtype=np.int64),
            "unique_devices": pd.Series(dtype=np.int64),
            "center_lat": pd.Series(dtype=np.float64),
            "center_lon": pd.Series(dtype=np.float64),
        })
    else:
        pings_agg = (
            pd.concat(count_parts, ignore_index=True)
            .groupby(["hex_id", "hour_of_day", "day_type"], as_index=False)["ping_count"]
            .sum()
        )
        devs_agg = (
            pd.concat(dev_parts, ignore_index=True)
            .drop_duplicates()
            .groupby(["hex_id", "hour_of_day", "day_type"], as_index=False)["device_id"]
            .count()
            .rename(columns={"device_id": "unique_devices"})
        )
        result = pings_agg.merge(devs_agg, on=["hex_id", "hour_of_day", "day_type"], how="inner")
        result["resolution"] = np.int32(res)
        result = attach_hex_centers(result)
        result["hour_of_day"] = result["hour_of_day"].astype(np.int32)
        result["ping_count"] = result["ping_count"].astype(np.int64)
        result["unique_devices"] = result["unique_devices"].astype(np.int64)
        result = result[list(HEX_COLUMNS)].sort_values(["hex_id", "day_type", "hour_of_day"]).reset_index(drop=True)

    arrow_schema = pa.schema([
        ("hex_id", pa.string()),
        ("resolution", pa.int32()),
        ("hour_of_day", pa.int32()),
        ("day_type", pa.string()),
        ("ping_count", pa.int64()),
        ("unique_devices", pa.int64()),
        ("center_lat", pa.float64()),
        ("center_lon", pa.float64()),
    ])
    out_dir = Path(out_path)
    if out_dir.exists() and out_dir.is_dir():
        for old in out_dir.glob("*.parquet"):
            old.unlink()
    out_dir.mkdir(parents=True, exist_ok=True)
    table = pa.Table.from_pandas(result, schema=arrow_schema, preserve_index=False)
    pq.write_table(table, out_dir / "part-00000.parquet")

    seconds = round(time.time() - t0, 2)
    return {
        "finished_at": datetime.now().isoformat(timespec="seconds"),
        "engine": "pyarrow",
        "pings_path": str(pings_path),
        "out": str(out_path),
        "ping_files": len(files),
        "resolution": int(res),
        "rows_in": int(rows_in),
        "rows_out": int(len(result)),
        "unique_hexes": int(result["hex_id"].nunique()) if len(result) else 0,
        "total_pings_out": int(result["ping_count"].sum()) if len(result) else 0,
        "runtime_seconds": seconds,
        "driver_memory": "n/a",
        "shuffle_partitions": "n/a",
    }


# =========================================================================================
# Output Contract 9.4 Verification
# =========================================================================================
_ARROW_TO_SPARK_TYPE = {
    "string": "string",
    "large_string": "string",
    "int32": "int",
    "int64": "bigint",
    "double": "double",
}


def verify_output(out_path, expected_res: int, expected_total_pings: Optional[int] = None) -> List[CheckResult]:
    """Re-read the written H3 Parquet output and verify Contract 9.4 & spatial sanity."""
    import pyarrow.parquet as pq  # type: ignore

    files = list_ping_files(out_path)
    first_schema = pq.ParquetFile(files[0]).schema_arrow
    fields = [(f.name, _ARROW_TO_SPARK_TYPE.get(str(f.type), str(f.type))) for f in first_schema]
    problems = schema_problems(fields)
    results: List[CheckResult] = [
        _res("output: columns and dtypes match contract 9.4", not problems, HEX_DDL, "; ".join(problems))
    ]
    if problems:
        return results

    df = pd.concat([pq.read_table(f).to_pandas() for f in files], ignore_index=True)
    n = len(df)
    results.append(_res("output: has rows", n > 0, f"{n:,} rows, {df['hex_id'].nunique():,} unique hexes", "0 rows"))
    if n == 0:
        return results

    nulls = int(df.isna().sum().sum())
    results.append(_res("output: no nulls", nulls == 0, "0 null values", f"{nulls:,} nulls"))

    res_ok = bool((df["resolution"] == int(expected_res)).all())
    results.append(_res(
        f"output: resolution == {expected_res}",
        res_ok,
        f"all rows resolution={expected_res}",
        f"found resolutions {sorted(df['resolution'].unique().tolist())}",
    ))

    hour_ok = bool(df["hour_of_day"].between(0, 23).all())
    day_ok = bool(df["day_type"].isin(["weekday", "weekend"]).all())
    results.append(_res("output: hour_of_day in 0..23 and valid day_type", hour_ok and day_ok, "valid", "invalid values"))

    m = HEX_BBOX_MARGIN_DEG
    inside_ok = bool(
        df["center_lat"].between(MIN_LAT - m, MAX_LAT + m).all()
        and df["center_lon"].between(MIN_LON - m, MAX_LON + m).all()
    )
    results.append(_res("output: hex centers inside city box", inside_ok, "all centers inside bbox", "centers outside bbox"))

    grain_ok = not df.duplicated(subset=["hex_id", "hour_of_day", "day_type"]).any()
    results.append(_res("output: grain (hex_id, hour_of_day, day_type) unique", grain_ok, "unique", "duplicate keys"))

    if expected_total_pings is not None:
        total_out = int(df["ping_count"].sum())
        results.append(_res(
            "output: summed ping_count == input rows",
            total_out == int(expected_total_pings),
            f"{total_out:,} pings preserved",
            f"sum(ping_count)={total_out:,}, expected {expected_total_pings:,}",
        ))
    return results


def build_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(description="Aggregate GPS pings into H3 hexagons (Contract 9.4).")
    ap.add_argument("--in", dest="in_path", default=DEFAULT_IN, help=f"input pings Parquet (default {DEFAULT_IN})")
    ap.add_argument("--out", default=DEFAULT_OUT, help=f"output H3 Parquet dir (default {DEFAULT_OUT})")
    ap.add_argument("--res", type=int, default=DEFAULT_RES, help=f"H3 resolution 0..15 (default {DEFAULT_RES})")
    ap.add_argument("--engine", choices=("spark", "pyarrow"), default="spark", help="execution engine (default spark)")
    ap.add_argument("--driver-memory", default=None, help="Spark driver memory, e.g. 4g")
    ap.add_argument("--shuffle-partitions", type=int, default=None, help="Spark shuffle partitions (default 64)")
    ap.add_argument("--coalesce", type=int, default=DEFAULT_COALESCE, help="output Parquet file count (default 8)")
    ap.add_argument("--skip-if-exists", action="store_true", help="skip if --out already contains *.parquet files")
    ap.add_argument("--audit", action="store_true", help="print and save JSON audit")
    ap.add_argument("--audit-out", default=DEFAULT_AUDIT, help=f"audit JSON path (default {DEFAULT_AUDIT})")
    ap.add_argument("--verbose", action="store_true", help="show full traceback on failure")
    return ap


def main(argv: Optional[list] = None) -> int:
    args = build_parser().parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    sedona = None
    try:
        if not (0 <= args.res <= 15):
            raise ValueError(f"H3 resolution must be 0..15, got {args.res}")

        if args.skip_if_exists and Path(args.out).is_dir() and any(Path(args.out).glob("*.parquet")):
            LOG.info("skipping: %s already has Parquet output (--skip-if-exists)", args.out)
            return 0

        in_path = str(resolve_pings_path(args.in_path)) if args.in_path == DEFAULT_IN else args.in_path
        list_ping_files(in_path)  # fail fast before starting Spark

        if args.engine == "spark":
            try:
                from src.spatial.sedona_config import get_sedona  # type: ignore
            except ImportError:
                from sedona_config import get_sedona  # type: ignore
            sedona = get_sedona(
                app_name="geopulse-h3-aggregation",
                driver_memory=args.driver_memory,
                shuffle_partitions=args.shuffle_partitions,
            )
            LOG.info("aggregating %s -> %s at H3 res=%d (engine=spark)", in_path, args.out, args.res)
            audit = run_spark_aggregation(sedona, in_path, args.out, res=args.res, coalesce=args.coalesce)
        else:
            LOG.info("aggregating %s -> %s at H3 res=%d (engine=pyarrow)", in_path, args.out, args.res)
            audit = run_pyarrow_aggregation(in_path, args.out, res=args.res)

        results = verify_output(args.out, expected_res=args.res, expected_total_pings=audit["rows_in"])
        for r in results:
            print(r)
        ok = all(r.passed for r in results)

        if args.audit:
            print("H3 AGGREGATION AUDIT")
            print("-" * 52)
            for k, v in audit.items():
                print(f"{k:<20} {v:,}" if isinstance(v, int) and not isinstance(v, bool) else f"{k:<20} {v}")
            ap_out = Path(args.audit_out)
            ap_out.parent.mkdir(parents=True, exist_ok=True)
            ap_out.write_text(json.dumps(audit, indent=2), encoding="utf-8")
            LOG.info("audit saved to %s", ap_out)

        print("H3 AGGREGATION: PASS" if ok else "H3 AGGREGATION: FAIL")
        return 0 if ok else 1
    except Exception as exc:  # noqa: BLE001
        LOG.error(
            "H3 aggregation failed: %s: %s",
            type(exc).__name__,
            str(exc).splitlines()[0] if str(exc) else "",
            exc_info=args.verbose,
        )
        return 1
    finally:
        if sedona is not None:
            if not args.verbose:
                try:
                    sedona.sparkContext.setLogLevel("OFF")
                except Exception:  # noqa: BLE001
                    pass
            sedona.stop()


if __name__ == "__main__":
    sys.exit(main())