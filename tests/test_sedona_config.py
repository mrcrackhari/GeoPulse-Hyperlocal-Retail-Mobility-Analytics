"""Tests for src/spatial/sedona_config.py (Member 2, Day 1).

Everything except the last test runs without Spark: configuration, the Java 17 auto-select helper
(against fake JDK folders), the Windows hint and the smoke-test expectations. The live session
test is skipped automatically when pyspark / apache-sedona / Java are not available.
"""
from __future__ import annotations

import os
import shutil
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from src.spatial import sedona_config as sc  # noqa: E402


@pytest.fixture(autouse=True)
def _clean_env(monkeypatch):
    monkeypatch.delenv(sc.DRIVER_MEMORY_ENV, raising=False)


# ------------------------------------------------------------------ Spark configuration ---
def test_conf_has_all_spec_keys():
    conf = sc.build_spark_conf()
    assert conf["spark.driver.memory"] == "4g"
    assert conf["spark.serializer"] == "org.apache.spark.serializer.KryoSerializer"
    assert conf["spark.kryo.registrator"] == "org.apache.sedona.core.serde.SedonaKryoRegistrator"
    assert conf["spark.sql.adaptive.enabled"] == "true"
    assert conf["spark.sql.shuffle.partitions"] == "64"


def test_packages_are_pinned_and_matched():
    pkgs = sc.build_spark_conf()["spark.jars.packages"].split(",")
    assert pkgs == [
        "org.apache.sedona:sedona-spark-shaded-3.4_2.12:1.5.1",
        "org.datasyslab:geotools-wrapper:1.5.1-28.2",
    ]


def test_session_timezone_is_utc():
    assert sc.build_spark_conf()["spark.sql.session.timeZone"] == "UTC"


def test_driver_memory_precedence(monkeypatch):
    assert sc.resolve_driver_memory() == "4g"                      # default
    monkeypatch.setenv(sc.DRIVER_MEMORY_ENV, "6g")
    assert sc.resolve_driver_memory() == "6g"                      # env var
    assert sc.resolve_driver_memory("2g") == "2g"                  # explicit argument wins
    assert sc.build_spark_conf(driver_memory="3g")["spark.driver.memory"] == "3g"


@pytest.mark.parametrize("bad", ["4 GB", "g4", "0g", "-1g", "4gigs", "abc"])
def test_invalid_driver_memory_rejected(bad):
    with pytest.raises(ValueError):
        sc.resolve_driver_memory(bad)


@pytest.mark.parametrize("good", ["4g", "4096m", "4G", "512M", "1t", "2gb"])
def test_valid_driver_memory_accepted(good):
    assert sc.resolve_driver_memory(good) == good


def test_shuffle_partitions_and_extra_conf_override():
    conf = sc.build_spark_conf(shuffle_partitions=128, extra_conf={"spark.sql.adaptive.enabled": "false", "x.y": 1})
    assert conf["spark.sql.shuffle.partitions"] == "128"
    assert conf["spark.sql.adaptive.enabled"] == "false"
    assert conf["x.y"] == "1"
    with pytest.raises(ValueError):
        sc.build_spark_conf(shuffle_partitions=0)


# ---------------------------------------------------------------------- Java 17 selection ---
@pytest.mark.parametrize("path,expected", [
    (r"C:\Program Files\Java\jdk-17.0.9", True),
    (r"C:\Program Files\Eclipse Adoptium\jdk-17.0.10.7-hotspot", True),
    ("/usr/lib/jvm/java-17-openjdk-amd64", True),
    ("/Library/Java/JavaVirtualMachines/temurin-17.jdk/Contents/Home", True),
    (r"C:\Program Files\Java\jdk-21", False),
    (r"C:\Program Files\Java\jdk-11.0.2", False),
    (r"C:\Program Files\Java\jdk1.8.0_171", False),      # "171" is not 17
    ("/usr/lib/jvm/java-117-custom", False),
    (r"C:\Program Files\Java\jdk-11.0.17", False),        # Java 11 update 17 is not Java 17
    (r"C:\Program Files\Java\jdk-21.0.2+17", False),      # build number 17 is not the major version
    (r"C:\Users\dev17\Java\jdk-21", False),               # "17" in a parent folder is ignored
    ("/home/dev17/.sdkman/candidates/java/21.0.1-tem", False),
    ("/home/dev/.sdkman/candidates/java/17.0.9-tem", True),
    (r"C:\Program Files\Amazon Corretto\jdk17.0.9_8", True),
    ("/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home", True),
    ("", False),
    (None, False),
])
def test_is_java17_path(path, expected):
    assert sc.is_java17_path(path) is expected


def _fake_jdk(root: Path, name: str) -> Path:
    home = root / name
    (home / "bin").mkdir(parents=True)
    (home / "bin" / sc._java_exe()).write_text("fake")
    return home


def test_find_java17_returns_current_home_when_already_17(tmp_path):
    home = _fake_jdk(tmp_path, "jdk-17.0.2")
    other = _fake_jdk(tmp_path, "jdk-17.0.9")
    assert sc.find_java17_home(str(home), [str(tmp_path / "jdk-17*")]) == str(home)
    assert str(other) != str(home)


def test_find_java17_replaces_java21_and_picks_newest(tmp_path):
    jdk21 = _fake_jdk(tmp_path, "jdk-21.0.1")
    _fake_jdk(tmp_path, "jdk-17.0.9")
    newest = _fake_jdk(tmp_path, "jdk-17.0.10")          # 17.0.10 must beat 17.0.9 (natural sort)
    assert sc.find_java17_home(str(jdk21), [str(tmp_path / "jdk-17*")]) == str(newest)


def test_find_java17_ignores_folders_without_a_java_binary(tmp_path):
    (tmp_path / "jdk-17.0.1").mkdir()                    # looks like a JDK but is empty
    assert sc.find_java17_home(None, [str(tmp_path / "jdk-17*")]) is None


def test_ensure_java17_updates_env_dict_only(tmp_path):
    jdk17 = _fake_jdk(tmp_path, "jdk-17.0.9")
    env = {"JAVA_HOME": str(tmp_path / "jdk-21"), "PATH": "base"}
    real_java_home = os.environ.get("JAVA_HOME")
    used = sc.ensure_java17(env, [str(tmp_path / "jdk-17*")])
    assert used == str(jdk17) and env["JAVA_HOME"] == str(jdk17)
    assert env["PATH"] == f"{jdk17 / 'bin'}{os.pathsep}base"
    assert os.environ.get("JAVA_HOME") == real_java_home  # the real process env was not touched


def test_ensure_java17_leaves_env_alone_when_none_found(tmp_path):
    env = {"JAVA_HOME": "/somewhere/jdk-21", "PATH": "base"}
    assert sc.ensure_java17(env, [str(tmp_path / "nope*")]) == "/somewhere/jdk-21"
    assert env == {"JAVA_HOME": "/somewhere/jdk-21", "PATH": "base"}
    assert sc.ensure_java17({"PATH": "base"}, [str(tmp_path / "nope*")]) is None


def test_ensure_java17_keeps_env_when_already_17(tmp_path):
    home = _fake_jdk(tmp_path, "jdk-17.0.9")
    env = {"JAVA_HOME": str(home), "PATH": "base"}
    sc.ensure_java17(env, [])
    assert env == {"JAVA_HOME": str(home), "PATH": "base"}


# ------------------------------------------------------------- Python workers / Windows ---
def test_configure_environment_sets_pyspark_python_but_keeps_existing(tmp_path):
    env = {"PATH": "base"}
    sc.configure_environment(env)
    assert env["PYSPARK_PYTHON"] == sys.executable and env["PYSPARK_DRIVER_PYTHON"] == sys.executable
    env2 = {"PYSPARK_PYTHON": "/custom/python"}
    sc.configure_environment(env2)
    assert env2["PYSPARK_PYTHON"] == "/custom/python"


def test_windows_hadoop_warning():
    assert "HADOOP_HOME" in sc.windows_hadoop_warning({}, is_windows=True)
    assert sc.windows_hadoop_warning({"HADOOP_HOME": "C:\\hadoop"}, is_windows=True) is None
    assert sc.windows_hadoop_warning({}, is_windows=False) is None


# --------------------------------------------------------------------- smoke-test design ---
def _in_polygon_box(lat: float, lon: float) -> bool:
    """Plain-Python reference for the rectangle in SMOKE_POLYGON_WKT (lon 77.59-77.60, lat 12.968-12.975)."""
    return 77.5900 < lon < 77.6000 and 12.9680 < lat < 12.9750


def test_smoke_expectations_match_a_plain_python_reference():
    assert len(sc.SMOKE_POINTS) == 3
    for pid, lat, lon, expected in sc.SMOKE_POINTS:
        assert _in_polygon_box(lat, lon) is expected, pid
    assert sc.SMOKE_POLYGON_WKT.startswith("POLYGON ((77.5900 12.9680")     # lon first in WKT too


def test_smoke_test_would_catch_a_lon_lat_swap():
    """With the coordinates swapped (x = lat, y = lon) the points that should be inside are outside."""
    for pid, lat, lon, expected in sc.SMOKE_POINTS:
        if expected:
            assert _in_polygon_box(lat=lon, lon=lat) is False, pid          # swapped call fails
    assert sc.smoke_test_passed({pid: exp for pid, _, _, exp in sc.SMOKE_POINTS})
    assert not sc.smoke_test_passed({"P1_STORE_A_CENTER": False, "P2_INSIDE_BOX": True,
                                     "P3_OUTSIDE_NORTH_EAST": False})


# ---------------------------------------------------------------------- live Spark session ---
@pytest.fixture(scope="module")
def sedona_session():
    pytest.importorskip("pyspark")
    pytest.importorskip("sedona.spark")
    sc.configure_environment()
    if shutil.which("java") is None and not os.environ.get("JAVA_HOME"):
        pytest.skip("Java 17 is not available")
    session = sc.get_sedona(app_name="geopulse-tests", driver_memory="1g", shuffle_partitions=4)
    yield session
    session.stop()


def test_real_sedona_smoke(sedona_session):
    result = sc.smoke_test(sedona_session)
    assert sc.smoke_test_passed(result), result
    assert sc.swapped_order_demo(sedona_session) is False