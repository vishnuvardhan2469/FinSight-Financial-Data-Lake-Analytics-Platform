"""lake_io: the helpers that let the same code run on a local folder or on S3."""
import io
import json
import sys

import pytest

import lake_io


# ---------- plain helpers ----------
def test_is_s3_recognises_s3_paths_only():
    assert lake_io.is_s3("s3://bucket/key")
    assert not lake_io.is_s3("data/raw")
    assert not lake_io.is_s3("C:/data/raw")


def test_split_separates_bucket_and_key():
    assert lake_io._split("s3://my-bucket/raw/transactions") == ("my-bucket", "raw/transactions")
    assert lake_io._split("s3://my-bucket") == ("my-bucket", "")


def test_parse_args_defaults_and_glue_extras(monkeypatch):
    monkeypatch.setattr(sys, "argv", ["job.py"])
    a = lake_io.parse_args()
    assert (a.base, a.until, a.ingest_date) == ("data", None, None)

    # Glue adds arguments of its own; they must be ignored, not rejected.
    monkeypatch.setattr(sys, "argv", ["job.py", "--base", "s3://b", "--JOB_NAME", "x",
                                      "--enable-metrics", "--extra-py-files", "s3://b/scripts/lake_io.py"])
    assert lake_io.parse_args().base == "s3://b"


# ---------- local folders ----------
def test_list_ingest_dates_local_is_sorted_and_ignores_noise(tmp_path):
    root = tmp_path / "raw" / "transactions"
    (root / "ingest_date=2026-10-08").mkdir(parents=True)
    (root / "ingest_date=2026-10-07").mkdir()
    (root / "something_else").mkdir()
    (root / "ingest_date=2026-10-09").write_text("a file, not a folder")
    assert lake_io.list_ingest_dates(str(root)) == ["2026-10-07", "2026-10-08"]


def test_list_ingest_dates_missing_folder_is_empty(tmp_path):
    assert lake_io.list_ingest_dates(str(tmp_path / "nope")) == []


def test_exists_local(tmp_path):
    (tmp_path / "there").mkdir()
    assert lake_io.exists(str(tmp_path / "there"))
    assert not lake_io.exists(str(tmp_path / "missing"))


def test_json_state_round_trip_and_default(tmp_path):
    path = str(tmp_path / "state" / "x.json")           # parent folder does not exist yet
    assert lake_io.read_json(path, {"d": []}) == {"d": []}
    lake_io.write_json(path, {"processed_ingest_dates": ["2026-10-07"]})
    assert lake_io.read_json(path, {}) == {"processed_ingest_dates": ["2026-10-07"]}


# ---------- S3, with a fake client (no network, no AWS account) ----------
class NoSuchKey(Exception):
    pass


class FakeS3:
    def __init__(self):
        self.calls = []
        self.objects = {}
        self.listing_pages = []
        self.key_count = 0
        self.exceptions = type("Ex", (), {"NoSuchKey": NoSuchKey})

    def get_paginator(self, name):
        assert name == "list_objects_v2"
        outer = self

        class Paginator:
            def paginate(self, **kwargs):
                outer.calls.append(("paginate", kwargs))
                return iter(outer.listing_pages)
        return Paginator()

    def list_objects_v2(self, **kwargs):
        self.calls.append(("list_objects_v2", kwargs))
        return {"KeyCount": self.key_count}

    def get_object(self, Bucket, Key):
        self.calls.append(("get_object", {"Bucket": Bucket, "Key": Key}))
        if (Bucket, Key) not in self.objects:
            raise NoSuchKey()
        return {"Body": io.BytesIO(self.objects[(Bucket, Key)])}

    def put_object(self, Bucket, Key, Body):
        self.calls.append(("put_object", {"Bucket": Bucket, "Key": Key}))
        self.objects[(Bucket, Key)] = Body


@pytest.fixture
def fake_s3(monkeypatch):
    fake = FakeS3()
    monkeypatch.setattr(lake_io, "_s3", lambda: fake)
    return fake


def test_list_ingest_dates_s3(fake_s3):
    fake_s3.listing_pages = [
        {"CommonPrefixes": [{"Prefix": "raw/transactions/ingest_date=2026-10-08/"},
                            {"Prefix": "raw/transactions/not_a_batch/"}]},
        {"CommonPrefixes": [{"Prefix": "raw/transactions/ingest_date=2026-10-07/"}]},
    ]
    assert lake_io.list_ingest_dates("s3://bkt/raw/transactions") == ["2026-10-07", "2026-10-08"]
    name, kwargs = fake_s3.calls[0]
    assert (kwargs["Bucket"], kwargs["Prefix"], kwargs["Delimiter"]) == ("bkt", "raw/transactions/", "/")


def test_exists_s3(fake_s3):
    fake_s3.key_count = 1
    assert lake_io.exists("s3://bkt/transformed/transactions")
    assert fake_s3.calls[0][1] == {"Bucket": "bkt", "Prefix": "transformed/transactions/", "MaxKeys": 1}
    fake_s3.key_count = 0
    assert not lake_io.exists("s3://bkt/transformed/transactions")


def test_state_file_round_trip_on_s3(fake_s3):
    path = "s3://bkt/state/transactions_processed.json"
    assert lake_io.read_json(path, {"processed_ingest_dates": []}) == {"processed_ingest_dates": []}   # NoSuchKey -> default
    lake_io.write_json(path, {"processed_ingest_dates": ["2026-10-07", "2026-10-08"]})
    assert ("bkt", "state/transactions_processed.json") in fake_s3.objects
    assert json.loads(fake_s3.objects[("bkt", "state/transactions_processed.json")]) == \
        {"processed_ingest_dates": ["2026-10-07", "2026-10-08"]}
    assert lake_io.read_json(path, {})["processed_ingest_dates"] == ["2026-10-07", "2026-10-08"]
