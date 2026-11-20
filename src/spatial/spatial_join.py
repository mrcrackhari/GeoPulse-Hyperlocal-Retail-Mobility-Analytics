"""GeoPulse spatial join (Member 2, Day 4) - GPS pings x 5,000 store catchments with Apache Sedona.

For every ping that falls inside a store's catchment polygon it writes one row (device_id, store_id,
ts, lat, lon) to ``data/processed/ping_store_matches/`` - the PING_STORE_MATCHES contract (plan 9.3).
A ping in the overlap lens of two stores is written twice (once per store); pings outside every
catchment are not written.

How it works
------------
    pings   -> ST_Point(lon, lat)              <- LONGITUDE FIRST
    stores  -> ST_GeomFromWKT(catchment_wkt)   <- 5,000 small polygons, BROADCAST to every task
    join    -> ST_Intersects(store.geom, ping.geom), executed as a broadcast index join
Pings are never collected to the driver; the result is written straight to Parquet.

Usage (repository root)
-----------------------
    python -m src.spatial.spatial_join --audit
    python -m src.spatial.spatial_join --pings data/raw/gps_pings_sample.parquet --audit
    python -m src.spatial.spatial_join --pings data/raw/gps_pings.parquet --by-day --audit --driver-memory 4g

Flags
-----
--pings           Parquet file or directory (default data/raw/gps_pings_sample.parquet)
--stores          stores.csv (default data/seed/stores.csv)
--out             output directory (default data/processed/ping_store_matches); overwritten on each run
--by-day          join one Parquet file at a time and append (keeps memory low for the 10 M run)
--audit           print the audit table and save it as JSON (see --audit-out)
--audit-out       audit file (default data/processed/join_audit.json)
--driver-memory   e.g. 4g (default: SPARK_DRIVER_MEMORY env var, else 4g)
--shuffle-partitions   Spark shuffle partitions (default 64)
--coalesce        number of output files for the whole run (default 16)

Exit code 0 = join finished and the output matches contract 9.3; 1 = failure.
"""
from __future__ import annotations

import argparse
import json
import logging
import math
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

LOG = logging.getLogger("geopulse.spatial_join")

# ------------------------------------------------------------------ contract 9.3 ---
MATCH_COLUMNS: Tuple[str, ...] = ("device_id", "store_id", "ts", "lat", "lon")
MATCH_TYPES: Dict[str, str] = {"device_id": "string", "store_id": "string", "ts": "timestamp",
                               "lat": "double", "lon": "double"}
MATCH_DDL = ", ".join(f"{c} {MATCH_TYPES[c]}" for c in MATCH_COLUMNS)

DEFAULT_PINGS = "data/raw/gps_pings_sample.parquet"
DEFAULT_STORES = "data/seed/stores.csv"
DEFAULT_OUT = "data/processed/ping_store_matches"
DEFAULT_AUDIT = "data/processed/join_audit.json"
DEFAULT_COALESCE = 16

# Longitude goes FIRST in ST_Point. Pings are exposed to SQL as view `pings` (with `geom`), stores as `stores`.
POINT_EXPR = "ST_Point(CAST(lon AS DOUBLE), CAST(lat AS DOUBLE))"
JOIN_SQL = """
SELECT /*+ BROADCAST(s) */ p.device_id, s.store_id, p.ts, p.lat, p.lon
FROM pings p
JOIN stores s ON ST_Intersects(s.geom, p.geom)
"""


# =========================================================================================
# Pure helpers (no Spark - unit tested)
# =========================================================================================
def list_ping_files(path) -> List[Path]:
    """The Parquet file(s) behind ``path``: a single file, or every *.parquet in a directory (sorted)."""
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"pings path {p} does not exist")
    if p.is_file():
        return [p]
    files = sorted(f for f in p.glob("*.parquet") if f.is_file())
    if not files:
        raise FileNotFoundError(f"no *.parquet files inside {p}")
    return files


def coalesce_for(total: int, n_files: int) -> int:
    """Output files per job: ``total`` spread over ``n_files`` jobs, at least 1 each."""
    if total < 1 or n_files < 1:
        raise ValueError("total and n_files must be >= 1")
    return max(1, math.ceil(total / n_files))


def spark_path(path) -> str:
    """Absolute path with forward slashes - the form Spark accepts on Windows and Linux."""
    return Path(path).resolve().as_posix()


def schema_problems(fields: Sequence[Tuple[str, str]]) -> List[str]:
    """Compare ``[(name, type)]`` with contract 9.3 (names, order, types). Empty list = matches."""
    got = [(n.lower(), t.lower()) for n, t in fields]
    want = [(c, MATCH_TYPES[c]) for c in MATCH_COLUMNS]
    if got == want:
        return []
    problems = []
    if [n for n, _ in got] != list(MATCH_COLUMNS):
        problems.append(f"columns are {[n for n, _ in got]}, contract wants {list(MATCH_COLUMNS)}")
    for n, t in got:
        if n in MATCH_TYPES and MATCH_TYPES[n] != t:
            problems.append(f"column {n} is {t}, contract wants {MATCH_TYPES[n]}")
    return problems


def process_tree_rss_mb() -> Optional[float]:
    """Resident memory (MB) of this process plus child processes (the Spark JVM), if psutil is installed."""
    try:
        import psutil  # type: ignore
    except ImportError:
        return None
    me = psutil.Process()
    total = me.memory_info().rss
    for child in me.children(recursive=True):
        try:
            total += child.memory_info().rss
        except psutil.Error:
            pass
    return round(total / 1024 / 1024, 1)


# =========================================================================================
# Spark steps
# =========================================================================================
def read_pings(sedona, paths: Sequence[Path]):
    """Read ping Parquet file(s); keep only the contract-9.1 columns with the contract types."""
    df = sedona.read.parquet(*[spark_path(p) for p in paths])
    return df.selectExpr("CAST(device_id AS STRING) AS device_id", "CAST(lat AS DOUBLE) AS lat",
                         "CAST(lon AS DOUBLE) AS lon", "CAST(ts AS TIMESTAMP) AS ts")


def register_stores(sedona, stores_csv) -> None:
    """Create the temp view ``stores`` (store_id, geom) from stores.csv. WKT is (lon lat), EPSG:4326."""
    df = (sedona.read.option("header", True).option("quote", '"').option("escape", '"')
          .csv(spark_path(stores_csv)))
    df.selectExpr("store_id", "ST_GeomFromWKT(catchment_wkt) AS geom").createOrReplaceTempView("stores")


def register_pings(sedona, pings_df) -> None:
    """Create the temp view ``pings`` with a point geometry built as ST_Point(lon, lat)."""
    pings_df.selectExpr("device_id", "lat", "lon", "ts", f"{POINT_EXPR} AS geom").createOrReplaceTempView("pings")


def explain_join(sedona) -> str:
    """The physical plan of the join, as text (used to confirm a broadcast spatial join is chosen)."""
    return "\n".join(r[0] for r in sedona.sql("EXPLAIN " + JOIN_SQL).collect())


def plan_uses_broadcast(plan_text: str) -> bool:
    return "broadcast" in plan_text.lower()


def verify_output(sedona, out) -> List[Any]:
    """Re-read the written Parquet and check it against contract 9.3. Returns CheckResult objects."""
    try:
        from src.spatial.validate_inputs import _res, MIN_LAT, MAX_LAT, MIN_LON, MAX_LON  # type: ignore
    except ImportError:
        from validate_inputs import _res, MIN_LAT, MAX_LAT, MIN_LON, MAX_LON  # type: ignore

    df = sedona.read.parquet(spark_path(out))
    fields = [(f.name, f.dataType.simpleString()) for f in df.schema.fields]
    problems = schema_problems(fields)
    results = [_res("output: columns and dtypes match contract 9.3", not problems,
                           f"{MATCH_DDL}", "; ".join(problems))]
    if problems:
        return results
    s = df.selectExpr(
        "count(*) AS n",
        "sum(CASE WHEN device_id IS NULL OR store_id IS NULL OR ts IS NULL OR lat IS NULL OR lon IS NULL "
        "THEN 1 ELSE 0 END) AS nulls",
        f"sum(CASE WHEN lat BETWEEN {MIN_LAT} AND {MAX_LAT} AND lon BETWEEN {MIN_LON} AND {MAX_LON} "
        "THEN 0 ELSE 1 END) AS outside",
        "count(DISTINCT store_id) AS stores",
    ).collect()[0]
    n, nulls, outside = int(s["n"]), int(s["nulls"] or 0), int(s["outside"] or 0)
    results += [
        _res("output: has rows", n > 0, f"{n:,} rows, {int(s['stores']):,} stores hit", "output is empty"),
        _res("output: no nulls", nulls == 0, "0 null values", f"{nulls:,} rows with nulls"),
        _res("output: lat/lon inside city box", outside == 0, "all inside", f"{outside:,} rows outside the box"),
    ]
    return results


def run_join(sedona, pings, stores, out, by_day: bool = False, coalesce: int = DEFAULT_COALESCE) -> Dict[str, Any]:
    """Run the broadcast spatial join and write ``out``. Returns the audit dict.

    Output is overwritten (the first write uses overwrite, later --by-day writes append), so a re-run
    gives the same result and never doubles the rows.
    """
    files = list_ping_files(pings)
    t0 = time.time()
    register_stores(sedona, stores)

    jobs: List[Sequence[Path]] = [[f] for f in files] if by_day else [files]
    per_job = coalesce_for(coalesce, len(jobs))
    rows_in = 0
    plan = ""
    for i, job in enumerate(jobs, 1):
        label = job[0].name if by_day else f"{len(job)} file(s)"
        pings_df = read_pings(sedona, job)
        n_in = pings_df.count()                      # reads only Parquet metadata, no join work
        rows_in += n_in
        register_pings(sedona, pings_df)
        if not plan:
            plan = explain_join(sedona)
            if not plan_uses_broadcast(plan):
                LOG.warning("physical plan has no broadcast join - check the Spark UI (SQL tab)")
        t_job = time.time()
        (sedona.sql(JOIN_SQL).coalesce(per_job).write
               .mode("overwrite" if i == 1 else "append").parquet(spark_path(out)))
        LOG.info("[%d/%d] %s: %s pings joined in %.1fs", i, len(jobs), label, f"{n_in:,}", time.time() - t_job)

    seconds = time.time() - t0
    rows_out = sedona.read.parquet(spark_path(out)).count()
    n_stores = sedona.sql("SELECT count(*) AS n FROM stores").collect()[0]["n"]
    conf = sedona.conf
    return {
        "finished_at": datetime.now().isoformat(timespec="seconds"),
        "pings_path": str(pings), "stores_path": str(stores), "out": str(out),
        "ping_files": len(files), "by_day": bool(by_day),
        "rows_in": rows_in, "stores": int(n_stores), "rows_out": int(rows_out),
        "matches_per_100_pings": round(100.0 * rows_out / rows_in, 3) if rows_in else None,
        "runtime_seconds": round(seconds, 1),
        "pings_per_second": round(rows_in / seconds) if seconds > 0 else None,
        "driver_memory": conf.get("spark.driver.memory", "?"),
        "shuffle_partitions": conf.get("spark.sql.shuffle.partitions", "?"),
        "output_files_target": per_job * len(jobs),
        "broadcast_in_plan": plan_uses_broadcast(plan),
        "process_tree_rss_mb": process_tree_rss_mb(),
        "spark_version": sedona.version,
    }


# =========================================================================================
# Audit output
# =========================================================================================
def format_audit(audit: Dict[str, Any]) -> str:
    keys = ["pings_path", "stores_path", "out", "ping_files", "by_day", "rows_in", "stores", "rows_out",
            "matches_per_100_pings", "runtime_seconds", "pings_per_second", "driver_memory",
            "shuffle_partitions", "output_files_target", "broadcast_in_plan", "process_tree_rss_mb",
            "spark_version", "finished_at"]
    lines = ["JOIN AUDIT", "-" * 52]
    for k in keys:
        v = audit.get(k)
        lines.append(f"{k:<24} {v:,}" if isinstance(v, int) and not isinstance(v, bool) else f"{k:<24} {v}")
    return "\n".join(lines)


def write_audit(audit: Dict[str, Any], path) -> Path:
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(audit, indent=2), encoding="utf-8")
    return p


# =========================================================================================
# CLI
# =========================================================================================
def build_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(description="Broadcast spatial join: GPS pings x store catchments (Sedona).")
    ap.add_argument("--pings", default=DEFAULT_PINGS, help=f"pings Parquet file or folder (default {DEFAULT_PINGS})")
    ap.add_argument("--stores", default=DEFAULT_STORES, help=f"stores csv (default {DEFAULT_STORES})")
    ap.add_argument("--out", default=DEFAULT_OUT, help=f"output folder (default {DEFAULT_OUT})")
    ap.add_argument("--by-day", action="store_true", help="join one Parquet file at a time and append")
    ap.add_argument("--audit", action="store_true", help="print and save the audit")
    ap.add_argument("--audit-out", default=DEFAULT_AUDIT, help=f"audit JSON path (default {DEFAULT_AUDIT})")
    ap.add_argument("--driver-memory", default=None, help="Spark driver memory, e.g. 4g")
    ap.add_argument("--shuffle-partitions", type=int, default=None, help="spark.sql.shuffle.partitions (default 64)")
    ap.add_argument("--coalesce", type=int, default=DEFAULT_COALESCE, help="output file count (default 16)")
    ap.add_argument("--verbose", action="store_true", help="show the full Python traceback on failure")
    return ap


def main(argv: Optional[list] = None) -> int:
    args = build_parser().parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    sedona = None
    try:
        list_ping_files(args.pings)                  # fail fast, before the slow Spark start
        if not Path(args.stores).exists():
            raise FileNotFoundError(f"{args.stores} not found - run `python -m src.spatial.generate_stores`")
        try:
            from src.spatial.sedona_config import get_sedona  # type: ignore
        except ImportError:
            from sedona_config import get_sedona  # type: ignore
        sedona = get_sedona(app_name="geopulse-spatial-join", driver_memory=args.driver_memory,
                            shuffle_partitions=args.shuffle_partitions)
        LOG.info("joining %s with %s -> %s (by_day=%s)", args.pings, args.stores, args.out, args.by_day)
        audit = run_join(sedona, args.pings, args.stores, args.out, by_day=args.by_day, coalesce=args.coalesce)

        results = verify_output(sedona, args.out)
        for r in results:
            print(r)
        ok = all(r.passed for r in results)
        if args.audit:
            print(format_audit(audit))
            LOG.info("audit saved to %s", write_audit(audit, args.audit_out))
        print("SPATIAL JOIN: PASS" if ok else "SPATIAL JOIN: FAIL")
        return 0 if ok else 1
    except Exception as exc:  # noqa: BLE001 - one clear message and exit code 1
        LOG.error("spatial join failed: %s: %s", type(exc).__name__, str(exc).splitlines()[0] if str(exc) else "",
                  exc_info=args.verbose)
        return 1
    finally:
        if sedona is not None:
            sedona.stop()


if __name__ == "__main__":
    sys.exit(main())