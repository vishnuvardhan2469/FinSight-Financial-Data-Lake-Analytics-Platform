"""Environment smoke test: Java, Hadoop, Spark + Parquet round trip, AWS identity."""
import os
import shutil
import subprocess
import sys
import tempfile

from pyspark.sql import SparkSession


def main():
    print("Python :", sys.version.split()[0])
    print("HADOOP_HOME:", os.environ.get("HADOOP_HOME"))
    print("winutils present:", os.path.exists(r"C:\hadoop\bin\winutils.exe"))

    spark = SparkSession.builder.master("local[*]").appName("check_env").getOrCreate()
    out = os.path.join(tempfile.mkdtemp(), "out")
    spark.range(10).write.mode("overwrite").parquet(out)
    print("Spark", spark.version, "- parquet round trip rows:", spark.read.parquet(out).count())
    spark.stop()
    shutil.rmtree(os.path.dirname(out), ignore_errors=True)

    r = subprocess.run(["aws", "sts", "get-caller-identity"], capture_output=True, text=True)
    print("AWS identity:", "OK" if r.returncode == 0 else "FAILED\n" + r.stderr.strip())


if __name__ == "__main__":
    main()
