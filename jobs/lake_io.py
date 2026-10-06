"""Storage helpers that work on a local folder or an S3 location."""
import argparse
import json
import os
import sys
from urllib.parse import urlparse


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--base", default="data")          # local folder or s3://bucket
    p.add_argument("--ingest-date", default=None)
    p.add_argument("--until", default=None)
    args, _ = p.parse_known_args()                    # Glue adds its own arguments
    return args


def is_s3(path):
    return path.startswith("s3://")


def _split(path):
    u = urlparse(path)
    return u.netloc, u.path.lstrip("/")


def _s3():
    import boto3
    return boto3.client("s3")


def list_ingest_dates(path):
    """Return sorted ingest_date values found as subfolders of path."""
    if not is_s3(path):
        if not os.path.isdir(path):
            return []
        return sorted(n.split("=", 1)[1] for n in os.listdir(path)
                      if n.startswith("ingest_date=") and os.path.isdir(os.path.join(path, n)))
    bucket, prefix = _split(path.rstrip("/") + "/")
    dates = []
    for page in _s3().get_paginator("list_objects_v2").paginate(
            Bucket=bucket, Prefix=prefix, Delimiter="/"):
        for cp in page.get("CommonPrefixes", []):
            name = cp["Prefix"][len(prefix):].strip("/")
            if name.startswith("ingest_date="):
                dates.append(name.split("=", 1)[1])
    return sorted(dates)


def exists(path):
    if not is_s3(path):
        return os.path.exists(path)
    bucket, prefix = _split(path.rstrip("/") + "/")
    return _s3().list_objects_v2(Bucket=bucket, Prefix=prefix, MaxKeys=1).get("KeyCount", 0) > 0


def read_json(path, default):
    if not is_s3(path):
        if not os.path.exists(path):
            return default
        with open(path) as f:
            return json.load(f)
    bucket, key = _split(path)
    s3 = _s3()
    try:
        return json.loads(s3.get_object(Bucket=bucket, Key=key)["Body"].read())
    except s3.exceptions.NoSuchKey:
        return default


def write_json(path, obj):
    if not is_s3(path):
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w") as f:
            json.dump(obj, f, indent=2)
        return
    bucket, key = _split(path)
    _s3().put_object(Bucket=bucket, Key=key, Body=json.dumps(obj, indent=2).encode("utf-8"))


def get_spark(app_name, local):
    if local:                                         # must be set before the JVM starts
        os.environ["PYSPARK_PYTHON"] = sys.executable
        os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
    from pyspark.sql import SparkSession
    builder = SparkSession.builder.appName(app_name)
    if local:
        builder = builder.master("local[*]")
    spark = builder.getOrCreate()
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    if local:
        spark.conf.set("spark.sql.shuffle.partitions", "12")
    spark.sparkContext.setLogLevel("ERROR")
    return spark