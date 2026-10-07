"""Shared test setup: import paths, Windows Hadoop settings, a shared Spark session, helpers."""
import csv
import importlib.util
import json
import os
import pathlib
import sys

import pytest

ROOT = pathlib.Path(__file__).resolve().parents[1]
for sub in ("jobs", "data_gen"):
    sys.path.insert(0, str(ROOT / sub))

# On Windows Spark needs winutils.exe / hadoop.dll. Set them up before Spark is imported.
if os.name == "nt":
    hadoop_home = os.environ.get("HADOOP_HOME", r"C:\hadoop")
    if os.path.isdir(os.path.join(hadoop_home, "bin")):
        os.environ["HADOOP_HOME"] = hadoop_home
        os.environ["PATH"] = os.path.join(hadoop_home, "bin") + os.pathsep + os.environ["PATH"]
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)


def load_job(filename, module_name):
    """Import a job script whose file name starts with a digit (so a normal import is impossible)."""
    spec = importlib.util.spec_from_file_location(module_name, ROOT / "jobs" / filename)
    module = importlib.util.module_from_spec(spec)
    saved_argv = sys.argv
    sys.argv = ["pytest"]                      # the jobs parse sys.argv at import time
    try:
        spec.loader.exec_module(module)
    finally:
        sys.argv = saved_argv
    return module


def posix(path):
    """Spark is happiest with forward slashes, also on Windows."""
    return str(path).replace("\\", "/")


def parquet_mtimes(folder):
    """{relative path: modification time} of every Parquet file under folder (to prove a file was not rewritten)."""
    folder = pathlib.Path(folder)
    return {str(p.relative_to(folder)): p.stat().st_mtime_ns for p in folder.rglob("*.parquet")}


def write_csv(path, header, rows):
    path = pathlib.Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(header)
        w.writerows(rows)


def write_json_lines(path, records):
    path = pathlib.Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        for r in records:
            f.write(json.dumps(r) + "\n")


@pytest.fixture(scope="session")
def spark():
    from pyspark.sql import SparkSession
    session = (SparkSession.builder.master("local[2]").appName("finsight-tests")
               .config("spark.sql.shuffle.partitions", "4")
               .config("spark.ui.enabled", "false")
               .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
               .getOrCreate())
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()
