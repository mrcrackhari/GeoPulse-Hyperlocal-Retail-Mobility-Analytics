"""Memory audit runner (Member 2, Day 7): run the spatial join several times with different settings.

Each run is a fresh ``python -m src.spatial.spatial_join`` process (fresh Spark JVM, so memory numbers do
not leak between runs). It varies shuffle partitions, ``--by-day`` and the BROADCAST hint, records runtime,
peak memory and whether it finished, and prints the markdown table for docs/spatial_join_notes.md section 5.

    python -m src.spatial.memory_audit --pings data/raw/gps_pings.parquet --driver-memory 4g
    python -m src.spatial.memory_audit --pings data/raw/demo_pings_1m.parquet --runs 1,2,4
    python -m src.spatial.memory_audit --list                                              # show the plan

Needs ``pip install psutil`` for the peak-memory column. Default plan has 5 runs (plan asks for >= 3).
If --pings does not exist, the same fallbacks as spatial_join are used (and the chosen file is shown).
"""
from __future__ import annotations

import argparse
import json
import shutil
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))                      # also works when run as a plain script from VS Code
WORK_DIR = "data/processed/audit_runs"
TABLE_HEADER = ("| Run | Shuffle partitions | `--by-day` | Broadcast hint | Driver memory | Runtime | Peak memory | OOM? |\n"
                "|---|---|---|---|---|---|---|---|")


@dataclass(frozen=True)
class RunSpec:
    shuffle_partitions: int
    by_day: bool
    broadcast_hint: bool
    note: str = ""


DEFAULT_RUNS: Sequence[RunSpec] = (
    RunSpec(64, False, True, "baseline (current defaults)"),
    RunSpec(16, False, True, "fewer shuffle partitions"),
    RunSpec(200, False, True, "more shuffle partitions"),
    RunSpec(64, True, True, "one daily file at a time"),
    RunSpec(64, False, False, "no BROADCAST hint (Spark chooses)"),
)


def build_command(spec: RunSpec, pings: str, stores: str, out: str, audit_out: str,
                  driver_memory: str, python: str = sys.executable) -> List[str]:
    cmd = [python, "-m", "src.spatial.spatial_join", "--pings", pings, "--stores", stores, "--out", out,
           "--driver-memory", driver_memory, "--shuffle-partitions", str(spec.shuffle_partitions),
           "--audit", "--audit-out", audit_out]
    if spec.by_day:
        cmd.append("--by-day")
    if not spec.broadcast_hint:
        cmd.append("--no-broadcast-hint")
    return cmd


def run_result(i: int, spec: RunSpec, driver_memory: str, returncode: int, audit: Optional[Dict[str, Any]],
               wall_seconds: float) -> Dict[str, Any]:
    """One result record. A non-zero exit (or a missing audit file) counts as a failed / OOM run."""
    ok = returncode == 0 and audit is not None
    return {"run": i, "spec": spec, "ok": ok, "driver_memory": driver_memory,
            "runtime_seconds": audit["runtime_seconds"] if ok else round(wall_seconds, 1),
            "peak_mb": audit.get("peak_process_tree_rss_mb") if ok else None,
            "rows_out": audit["rows_out"] if ok else None,
            "broadcast_in_plan": audit.get("broadcast_in_plan") if ok else None}


def table_row(r: Dict[str, Any]) -> str:
    s = r["spec"]
    peak = f"{r['peak_mb']:,.0f} MB" if r["peak_mb"] else "n/a"
    runtime = f"{r['runtime_seconds']:.0f} s ({r['runtime_seconds'] / 60:.1f} min)" if r["ok"] else \
        f"failed after {r['runtime_seconds']:.0f} s"
    return (f"| {r['run']} | {s.shuffle_partitions} | {'yes' if s.by_day else 'no'} | "
            f"{'yes' if s.broadcast_hint else 'no'} | {r['driver_memory']} | {runtime} | {peak} | "
            f"{'no' if r['ok'] else 'YES / failed'} |")


def render_table(results: Sequence[Dict[str, Any]]) -> str:
    return "\n".join([TABLE_HEADER] + [table_row(r) for r in results])


def pick_best(results: Sequence[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """Fastest run that finished; ties broken by lower peak memory. None if every run failed."""
    done = [r for r in results if r["ok"]]
    return min(done, key=lambda r: (r["runtime_seconds"], r["peak_mb"] or 0)) if done else None


def check_rows_agree(results: Sequence[Dict[str, Any]]) -> bool:
    """All finished runs must produce the same number of match rows (settings change speed, not answers)."""
    return len({r["rows_out"] for r in results if r["ok"]}) <= 1


def recommendation(best: Optional[Dict[str, Any]]) -> str:
    if best is None:
        return "Every run failed - apply the OOM ladder (plan section 12) and re-run."
    s = best["spec"]
    return (f"Best run: #{best['run']} ({s.note}) - {best['runtime_seconds']:.0f} s.\n"
            f"Set in src/spatial/sedona_config.py:  DEFAULT_SHUFFLE_PARTITIONS = {s.shuffle_partitions}\n"
            f"Use `--by-day` by default for the full run: {'yes' if s.by_day else 'no'}.   "
            f"Keep the BROADCAST hint: {'yes' if s.broadcast_hint else 'no (but it is safest to keep it)'}.")


def input_notes(n_files: int, n_runs_by_day: int) -> List[str]:
    """Warnings about the chosen input, shown before the runs start."""
    notes = []
    if n_files == 1 and n_runs_by_day:
        notes.append("NOTE: the input is a single file, so --by-day cannot differ from the baseline here "
                     "(it joins one file at a time). Use the 30 daily files for a meaningful --by-day run.")
    return notes


def main(argv: Optional[list] = None) -> int:
    ap = argparse.ArgumentParser(description="Run the spatial join with several settings and tabulate the results.")
    ap.add_argument("--pings", default="data/raw/gps_pings.parquet")
    ap.add_argument("--stores", default="data/seed/stores.csv")
    ap.add_argument("--driver-memory", default="4g")
    ap.add_argument("--runs", default=None, help="comma-separated run numbers to execute, e.g. 1,4,5 (default all)")
    ap.add_argument("--timeout-min", type=float, default=60, help="kill a run after this many minutes")
    ap.add_argument("--keep", action="store_true", help="keep each run's output folder (default: delete to save disk)")
    ap.add_argument("--list", action="store_true", help="print the run plan and exit")
    ap.add_argument("--table-out", default="data/processed/memory_audit.md")
    args = ap.parse_args(argv)

    wanted = {int(x) for x in args.runs.split(",")} if args.runs else set(range(1, len(DEFAULT_RUNS) + 1))
    plan = [(i, s) for i, s in enumerate(DEFAULT_RUNS, 1) if i in wanted]
    if args.list or not plan:
        for i, s in enumerate(DEFAULT_RUNS, 1):
            print(f"run {i}: shuffle={s.shuffle_partitions} by_day={s.by_day} broadcast_hint={s.broadcast_hint}  # {s.note}")
        return 0 if args.list else 1

    from src.spatial.spatial_join import list_ping_files, resolve_pings_path
    try:
        pings = resolve_pings_path(ROOT / args.pings if not Path(args.pings).is_absolute() else args.pings)
        files = list_ping_files(pings)
    except FileNotFoundError as exc:
        print(f"ERROR: {exc}")
        return 1
    print(f"Input: {pings} ({len(files)} file(s))")
    for note in input_notes(len(files), sum(1 for _, s in plan if s.by_day)):
        print(note)

    work = ROOT / WORK_DIR
    work.mkdir(parents=True, exist_ok=True)
    results = []
    for i, spec in plan:
        out, audit_path = work / f"run_{i}", work / f"run_{i}.json"
        audit_path.unlink(missing_ok=True)
        cmd = build_command(spec, str(pings), args.stores, str(out), str(audit_path), args.driver_memory)
        print(f"\n=== run {i}/{len(DEFAULT_RUNS)}: {spec.note} ===\n{' '.join(cmd)}", flush=True)
        t0 = time.time()
        try:
            rc = subprocess.run(cmd, cwd=ROOT, timeout=args.timeout_min * 60).returncode
        except subprocess.TimeoutExpired:
            rc = -1
        audit = json.loads(audit_path.read_text()) if audit_path.exists() else None
        results.append(run_result(i, spec, args.driver_memory, rc, audit, time.time() - t0))
        if not args.keep:
            shutil.rmtree(out, ignore_errors=True)

    table = render_table(results)
    Path(args.table_out).parent.mkdir(parents=True, exist_ok=True)
    Path(args.table_out).write_text(f"Input: {pings} ({len(files)} file(s))\n\n{table}\n", encoding="utf-8")
    print("\n" + table)
    if not check_rows_agree(results):
        print("\nWARNING: finished runs produced different row counts - investigate before trusting the table.")
    print("\n" + recommendation(pick_best(results)))
    print(f"\nTable saved to {args.table_out}")
    return 0 if all(r["ok"] for r in results) else 1


if __name__ == "__main__":
    sys.exit(main())