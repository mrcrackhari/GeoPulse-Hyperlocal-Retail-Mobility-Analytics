"""Spark + Apache Sedona session factory for GeoPulse (Member 2, Day 1).

Usage
-----
    from src.spatial.sedona_config import get_sedona

    sedona = get_sedona()                      # 4 GB driver by default
    sedona = get_sedona(driver_memory="6g")    # or set SPARK_DRIVER_MEMORY=6g in .env / shell

Smoke test (prints versions, checks ST_Contains on 3 points and 1 polygon, and shows what a
lon/lat swap looks like):

    python -m src.spatial.sedona_config

What this module does for you
-----------------------------
* Pinned, matched versions:  pyspark 3.4.1 <-> apache-sedona 1.5.1
  <-> sedona-spark-shaded-3.4_2.12:1.5.1 <-> geotools-wrapper 1.5.1-28.2   (Java 17).
* Picks Java 17 automatically (Windows / Linux / macOS install locations) because Spark 3.4
  does not run on Java 21+. Only the current Python process is changed, never your system.
* Sets PYSPARK_PYTHON / PYSPARK_DRIVER_PYTHON to the running interpreter (avoids the Windows
  "python3 not found" worker error) and warns when HADOOP_HOME is missing on Windows.
* Creates the session the way Sedona 1.5.1 documents it:
  ``SedonaContext.builder()...getOrCreate()`` and then ``SedonaContext.create(spark)``.
  ``create`` needs a real SparkSession - passing the un-built builder fails.
* Pins ``spark.sql.session.timeZone`` to UTC. Our pings are city-local wall-clock time with no
  timezone (rule G2); with a UTC session Spark never shifts them, so ``hour(ts)`` in the H3
  aggregation is the same hour Snowflake sees.
* pyspark and sedona are imported lazily, so the helpers and their tests work without Spark.

In local mode ``spark.driver.memory`` only takes effect on the FIRST ``get_sedona()`` call of a
Python process (the JVM starts once).
"""
from __future__ import annotations

import argparse
import glob
import importlib.metadata
import logging
import os
import re
import sys
from pathlib import Path
from typing import Any, Dict, List, MutableMapping, Optional, Sequence, Tuple

LOG = logging.getLogger("geopulse.sedona_config")

# --- Pinned versions (locked decision, plan section 3.1) -------------------------------
SPARK_VERSION = "3.4.1"
SEDONA_VERSION = "1.5.1"
SEDONA_SPARK_PACKAGE = "org.apache.sedona:sedona-spark-shaded-3.4_2.12:1.5.1"
GEOTOOLS_PACKAGE = "org.datasyslab:geotools-wrapper:1.5.1-28.2"
SPARK_PACKAGES = f"{SEDONA_SPARK_PACKAGE},{GEOTOOLS_PACKAGE}"

# --- Defaults ----------------------------------------------------------------------------
DEFAULT_DRIVER_MEMORY = "4g"
DEFAULT_SHUFFLE_PARTITIONS = 64
DRIVER_MEMORY_ENV = "SPARK_DRIVER_MEMORY"
_MEMORY_RE = re.compile(r"^[1-9]\d*(k|m|g|t)b?$", re.IGNORECASE)


# =========================================================================================
# Environment: Java 17, Python workers, Windows hints
# =========================================================================================
# "17" as a major version: not preceded by a digit / "." / "_" / "+" (so jdk-11.0.17, 1.8.0_17 and
# jdk-21+17 do not count) and not followed by a digit (so 171 / 117 do not count).
_JAVA17_RE = re.compile(r"(?<![\d._+])17(?!\d)")
# Folders that sit INSIDE a JDK home and carry no version themselves (macOS / Homebrew layouts).
_WRAPPER_DIRS = frozenset({"home", "contents", "libexec", "openjdk.jdk", "jre", "bin"})

JAVA17_SEARCH_PATTERNS: Tuple[str, ...] = (
    # Windows
    r"C:\Program Files\Eclipse Adoptium\jdk-17*",
    r"C:\Program Files\Java\jdk-17*",
    r"C:\Program Files\Microsoft\jdk-17*",
    r"C:\Program Files\Amazon Corretto\jdk17*",
    r"C:\Program Files\Zulu\zulu-17*",
    r"C:\Program Files\OpenJDK\jdk-17*",
    # Linux
    "/usr/lib/jvm/*17*",
    # macOS (system installs and Homebrew)
    "/Library/Java/JavaVirtualMachines/*17*/Contents/Home",
    "/opt/homebrew/opt/openjdk@17",
    "/usr/local/opt/openjdk@17",
)


def _java_exe() -> str:
    return "java.exe" if os.name == "nt" else "java"


def _natural_key(text: str) -> List[Any]:
    """Sort key that orders 17.0.10 after 17.0.9 (plain string sort would not)."""
    return [int(tok) if tok.isdigit() else tok.lower() for tok in re.split(r"(\d+)", text)]


def _jdk_folder_name(path: str) -> str:
    """The folder that names the JDK, e.g. 'jdk-17.0.9' (ignores parents such as C:\\Users\\dev17\\ and
    macOS wrapper folders such as .../temurin-17.jdk/Contents/Home)."""
    parts = [p for p in re.split(r"[\\/]+", path.strip()) if p]
    while len(parts) > 1 and parts[-1].lower() in _WRAPPER_DIRS:
        parts.pop()
    return parts[-1] if parts else ""


def is_java17_path(path: Optional[str]) -> bool:
    """True if the JDK folder name says Java 17 (jdk-17.0.9, java-17-openjdk-amd64, openjdk@17, ...)."""
    return bool(path) and bool(_JAVA17_RE.search(_jdk_folder_name(path)))


def find_java17_home(current_home: Optional[str] = None,
                     patterns: Sequence[str] = JAVA17_SEARCH_PATTERNS) -> Optional[str]:
    """Return a Java 17 home directory, or None if none is found.

    Order: the current JAVA_HOME if it already is Java 17, otherwise the newest installed
    Java 17 that has ``bin/java`` under one of the search patterns.
    """
    if current_home and is_java17_path(current_home) and Path(current_home).exists():
        return current_home
    candidates = [m for pattern in patterns for m in glob.glob(pattern)
                  if is_java17_path(m) and (Path(m) / "bin" / _java_exe()).exists()]
    if not candidates:
        return None
    return sorted(candidates, key=_natural_key, reverse=True)[0]


def ensure_java17(env: Optional[MutableMapping[str, str]] = None,
                  patterns: Sequence[str] = JAVA17_SEARCH_PATTERNS) -> Optional[str]:
    """Point JAVA_HOME / PATH of ``env`` (default: this process) at Java 17 when one is installed.

    Returns the JAVA_HOME that will be used (possibly unchanged, possibly None). If no Java 17 is
    found nothing is changed - the ``java`` already on PATH may still be fine.
    """
    env = os.environ if env is None else env
    current = env.get("JAVA_HOME", "")
    home = find_java17_home(current, patterns)
    if home is None:
        return current or None
    if home != current:
        env["JAVA_HOME"] = home
        env["PATH"] = f"{Path(home) / 'bin'}{os.pathsep}{env.get('PATH', '')}"
        LOG.info("Auto-selected Java 17 at JAVA_HOME=%s", home)
    return home


def windows_hadoop_warning(env: Optional[MutableMapping[str, str]] = None,
                           is_windows: Optional[bool] = None) -> Optional[str]:
    """Hint shown on Windows when HADOOP_HOME is missing (Spark needs winutils to write files)."""
    env = os.environ if env is None else env
    is_windows = (os.name == "nt") if is_windows is None else is_windows
    if is_windows and not env.get("HADOOP_HOME") and not env.get("hadoop.home.dir"):
        return ("Windows detected and HADOOP_HOME is not set: Spark may fail when WRITING files "
                "(winutils.exe). Set HADOOP_HOME to a folder that contains bin\\winutils.exe, "
                "or run the project in WSL2.")
    return None


def configure_environment(env: Optional[MutableMapping[str, str]] = None) -> Optional[str]:
    """Prepare the process environment before the JVM starts; returns the JAVA_HOME in use."""
    env = os.environ if env is None else env
    java_home = ensure_java17(env)
    env.setdefault("PYSPARK_PYTHON", sys.executable)
    env.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
    warning = windows_hadoop_warning(env)
    if warning:
        LOG.warning(warning)
    return java_home


# =========================================================================================
# Spark configuration and session
# =========================================================================================
def _load_dotenv_if_available() -> None:
    """Load ``.env`` (SPARK_DRIVER_MEMORY etc.) when python-dotenv is installed."""
    try:
        from dotenv import load_dotenv  # type: ignore
    except ImportError:
        return
    load_dotenv()


def resolve_driver_memory(explicit: Optional[str] = None) -> str:
    """Driver memory precedence: explicit argument > SPARK_DRIVER_MEMORY env var > ``4g``.

    Raises ValueError for strings Spark would not understand (e.g. ``"4 GB"``).
    """
    value = explicit or os.environ.get(DRIVER_MEMORY_ENV) or DEFAULT_DRIVER_MEMORY
    value = value.strip()
    if not _MEMORY_RE.match(value):
        raise ValueError(
            f"Invalid driver memory {value!r}: use a number plus k/m/g/t, e.g. '4g' or '4096m'."
        )
    return value


def build_spark_conf(
    driver_memory: Optional[str] = None,
    shuffle_partitions: Optional[int] = None,
    extra_conf: Optional[Dict[str, Any]] = None,
) -> Dict[str, str]:
    """Return the Spark configuration (as strings) used for every GeoPulse session.

    Pure function: no Spark needed, so it is unit-tested everywhere.
    """
    partitions = DEFAULT_SHUFFLE_PARTITIONS if shuffle_partitions is None else int(shuffle_partitions)
    if partitions < 1:
        raise ValueError("shuffle_partitions must be >= 1")
    conf: Dict[str, str] = {
        "spark.driver.memory": resolve_driver_memory(driver_memory),
        "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
        "spark.kryo.registrator": "org.apache.sedona.core.serde.SedonaKryoRegistrator",
        "spark.sql.adaptive.enabled": "true",
        "spark.sql.shuffle.partitions": str(partitions),
        "spark.jars.packages": SPARK_PACKAGES,
        "spark.sql.session.timeZone": "UTC",  # see module docstring (rule G2)
    }
    if extra_conf:
        conf.update({str(k): str(v) for k, v in extra_conf.items()})
    return conf


def get_sedona(
    app_name: str = "geopulse",
    driver_memory: Optional[str] = None,
    shuffle_partitions: Optional[int] = None,
    extra_conf: Optional[Dict[str, Any]] = None,
):
    """Create (or reuse) a Sedona-enabled local Spark session and return it.

    Parameters
    ----------
    app_name:
        Spark application name (shows up in the Spark UI at http://localhost:4040).
    driver_memory:
        e.g. ``"4g"``. Defaults to ``SPARK_DRIVER_MEMORY`` or ``4g``.
    shuffle_partitions:
        Overrides ``spark.sql.shuffle.partitions`` (default 64).
    extra_conf:
        Extra Spark settings; they override the defaults.

    Note: the first run downloads the Sedona jars from Maven Central (needs internet).
    """
    _load_dotenv_if_available()
    configure_environment()                       # Java 17 + Python workers, BEFORE the JVM starts
    conf = build_spark_conf(driver_memory, shuffle_partitions, extra_conf)

    # Lazy imports: only needed when a session is really created.
    from pyspark.sql import SparkSession  # type: ignore
    from sedona.spark import SedonaContext  # type: ignore

    builder_factory = getattr(SedonaContext, "builder", None)
    builder = builder_factory() if callable(builder_factory) else SparkSession.builder
    builder = builder.appName(app_name).master("local[*]")
    for key, value in conf.items():
        builder = builder.config(key, value)

    LOG.info("Starting Spark/Sedona (driver memory %s, %s shuffle partitions)",
             conf["spark.driver.memory"], conf["spark.sql.shuffle.partitions"])
    spark = builder.getOrCreate()                 # a real SparkSession ...
    sedona = SedonaContext.create(spark)          # ... which is what create() requires
    sedona.sparkContext.setLogLevel("WARN")
    return sedona


# =========================================================================================
# Smoke test: 3 points x 1 polygon, real GeoPulse coordinates (lon/lat swap would fail it)
# =========================================================================================
SMOKE_POLYGON_WKT = ("POLYGON ((77.5900 12.9680, 77.6000 12.9680, "
                     "77.6000 12.9750, 77.5900 12.9750, 77.5900 12.9680))")
# (point_id, lat, lon, expected ST_Contains) - note the order: lat first here, lon first in ST_Point.
SMOKE_POINTS: Tuple[Tuple[str, float, float, bool], ...] = (
    ("P1_STORE_A_CENTER", 12.9716, 77.5946, True),
    ("P2_INSIDE_BOX", 12.9725, 77.5960, True),
    ("P3_OUTSIDE_NORTH_EAST", 13.0358, 77.6408, False),
)


def smoke_test(sedona) -> Dict[str, bool]:
    """Run ST_Contains(polygon, ST_Point(lon, lat)) for the 3 points; return {point_id: contained?}."""
    rows = [(pid, lat, lon) for pid, lat, lon, _ in SMOKE_POINTS]
    sedona.createDataFrame(rows, ["point_id", "lat", "lon"]).createOrReplaceTempView("smoke_points")
    result = sedona.sql(
        f"""
        SELECT point_id,
               ST_Contains(ST_GeomFromWKT('{SMOKE_POLYGON_WKT}'), ST_Point(lon, lat)) AS inside
        FROM smoke_points
        ORDER BY point_id
        """
    ).collect()
    return {row["point_id"]: bool(row["inside"]) for row in result}


def smoke_test_passed(result: Dict[str, bool]) -> bool:
    """True when ``result`` equals the expected ST_Contains answers."""
    return result == {pid: exp for pid, _, _, exp in SMOKE_POINTS}


def swapped_order_demo(sedona) -> bool:
    """Store A's centre with the coordinates in the WRONG order, ST_Point(lat, lon). Expected: False.

    This is exactly what a lon/lat mix-up looks like: a point that is really inside the polygon
    is reported as outside. (Answer is False for the swapped call, True for the correct one.)
    """
    _, lat, lon, _ = SMOKE_POINTS[0]
    row = sedona.sql(
        f"SELECT ST_Contains(ST_GeomFromWKT('{SMOKE_POLYGON_WKT}'), ST_Point({lat}, {lon})) AS inside"
    ).collect()[0]
    return bool(row["inside"])


def _package_version(dist: str) -> str:
    try:
        return importlib.metadata.version(dist)
    except importlib.metadata.PackageNotFoundError:
        return "not installed"


def main(argv: Optional[list] = None) -> int:
    parser = argparse.ArgumentParser(description="GeoPulse: start Spark + Sedona and run a smoke test.")
    parser.add_argument("--driver-memory", default=None, help="e.g. 4g (default: $SPARK_DRIVER_MEMORY or 4g)")
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")

    print(f"python            : {sys.version.split()[0]}")
    print(f"pyspark (pip)     : {_package_version('pyspark')}  (expected {SPARK_VERSION})")
    print(f"apache-sedona(pip): {_package_version('apache-sedona')}  (expected {SEDONA_VERSION})")

    try:
        sedona = get_sedona(driver_memory=args.driver_memory)
    except ImportError as exc:
        print(f"FAIL: pyspark / apache-sedona is not installed in this environment ({exc}).")
        print("      pip install \"pyspark==3.4.1\" \"apache-sedona==1.5.1\" \"numpy<2\"")
        return 1
    except Exception as exc:  # JVM / Java / Maven problems land here
        print(f"FAIL: could not start Spark + Sedona: {exc}")
        print("      Check `java -version` (needs 17) and see docs/spatial_join_notes.md, 'Known issues'.")
        return 1

    try:
        jvm = sedona.sparkContext._jvm
        print(f"Spark (running)   : {sedona.version}")
        print(f"Java (running)    : {jvm.System.getProperty('java.version')}")
        print(f"JAVA_HOME         : {os.environ.get('JAVA_HOME', 'system default')}")
        print(f"driver memory     : {sedona.conf.get('spark.driver.memory')}")
        if not sedona.version.startswith("3.4"):
            print("WARNING: Sedona 1.5.1 targets Spark 3.4.x - versions must match.")

        result = smoke_test(sedona)
        print("-" * 70)
        print("ST_Contains(polygon, ST_Point(lon, lat)):")
        for pid, lat, lon, expected in SMOKE_POINTS:
            got = result.get(pid)
            mark = "ok" if got == expected else "WRONG"
            print(f"  {pid:<24} lat={lat:.4f} lon={lon:.4f}  expected={expected!s:<5} got={got!s:<5} {mark}")
        swapped = swapped_order_demo(sedona)
        print(f"  Store A centre as ST_Point(lat, lon) (WRONG order) -> {swapped}  (must be False)")
        print("-" * 70)
        ok = smoke_test_passed(result) and swapped is False
        print("SMOKE TEST: PASS" if ok else "SMOKE TEST: FAIL")
        return 0 if ok else 1
    finally:
        sedona.stop()


if __name__ == "__main__":
    raise SystemExit(main())
