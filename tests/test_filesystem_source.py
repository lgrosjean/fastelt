"""Tests for LocalFileSystemSource and GCSFileSystemSource."""

from __future__ import annotations

import csv
import json
import shutil
import tempfile
from pathlib import Path

import duckdb
import pytest

from fastelt import FastELT
from fastelt.destinations import DuckDBDestination
from fastelt.sources.filesystem import (
    FileResource,
    GCSFileSystemSource,
    LocalFileSystemSource,
    _infer_format,
)

PIPELINES: list[str] = []


def _cleanup_pipeline(pipeline_name: str):
    for suffix in [".duckdb", ".duckdb.wal"]:
        p = Path(f"{pipeline_name}{suffix}")
        if p.exists():
            p.unlink()
    working_dir = Path(f"/var/dlt/pipelines/{pipeline_name}")
    if working_dir.exists():
        shutil.rmtree(working_dir, ignore_errors=True)


@pytest.fixture(autouse=True)
def cleanup():
    PIPELINES.clear()
    yield
    for name in PIPELINES:
        _cleanup_pipeline(name)


def _app(name: str, **kwargs) -> FastELT:
    PIPELINES.append(name)
    return FastELT(pipeline_name=name, destination=DuckDBDestination(), **kwargs)


def _query(pipeline_name: str, sql: str):
    conn = duckdb.connect(f"{pipeline_name}.duckdb")
    result = conn.sql(sql).fetchall()
    conn.close()
    return result


@pytest.fixture
def data_dir(tmp_path):
    """Create a temp directory with CSV and JSONL test files."""
    # CSV files
    csv_dir = tmp_path / "users"
    csv_dir.mkdir()
    with open(csv_dir / "users.csv", "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["id", "name", "age"])
        writer.writeheader()
        writer.writerow({"id": "1", "name": "Alice", "age": "30"})
        writer.writerow({"id": "2", "name": "Bob", "age": "25"})

    # JSONL files
    jsonl_dir = tmp_path / "events"
    jsonl_dir.mkdir()
    with open(jsonl_dir / "events.jsonl", "w") as f:
        f.write(json.dumps({"id": 1, "type": "click", "ts": "2024-01-01"}) + "\n")
        f.write(json.dumps({"id": 2, "type": "view", "ts": "2024-01-02"}) + "\n")

    return tmp_path


# -- Unit tests --


def test_infer_format_csv():
    assert _infer_format("data/*.csv") == "csv"


def test_infer_format_jsonl():
    assert _infer_format("logs/*.jsonl") == "jsonl"


def test_infer_format_ndjson():
    assert _infer_format("logs/*.ndjson") == "jsonl"


def test_infer_format_parquet():
    assert _infer_format("data/*.parquet") == "parquet"


def test_infer_format_unknown():
    with pytest.raises(ValueError, match="Cannot infer format"):
        _infer_format("data/*.txt")


def test_file_resource_infers_format():
    r = FileResource(name="test", file_glob="data/*.csv")
    assert r.format == "csv"


def test_file_resource_explicit_format():
    r = FileResource(name="test", file_glob="data/*.dat", format="csv")
    assert r.format == "csv"


def test_local_list_resources():
    src = LocalFileSystemSource(
        name="test",
        bucket_url="/tmp/data",
        resources=[
            {"name": "users", "file_glob": "users/*.csv", "format": "csv"},
            {"name": "events", "file_glob": "events/*.jsonl", "format": "jsonl"},
        ],
    )
    assert src.list_resources() == ["users", "events"]


def test_local_list_resources_file_resource():
    src = LocalFileSystemSource(
        name="test",
        bucket_url="/tmp/data",
        resources=[
            FileResource(name="users", file_glob="users/*.csv"),
        ],
    )
    assert src.list_resources() == ["users"]


def test_gcs_source_has_credentials():
    gcs = GCSFileSystemSource(
        name="gcs",
        bucket_url="gs://my-bucket",
        credentials="/path/to/creds.json",
        resources=[
            {"name": "data", "file_glob": "*.csv", "format": "csv"},
        ],
    )
    assert gcs.credentials == "/path/to/creds.json"
    assert gcs.list_resources() == ["data"]


# -- Integration tests --


def test_local_csv_pipeline(data_dir):
    """Load CSV files from local filesystem into duckdb."""
    app = _app("p_fs_csv")

    src = LocalFileSystemSource(
        name="local",
        bucket_url=str(data_dir),
        resources=[
            {
                "name": "users",
                "file_glob": "users/*.csv",
                "format": "csv",
                "write_disposition": "replace",
            },
        ],
    )

    app.include_source(src)
    app.run()

    rows = _query("p_fs_csv", "SELECT name FROM p_fs_csv_data.users ORDER BY name")
    names = [r[0] for r in rows]
    assert "Alice" in names
    assert "Bob" in names


def test_local_jsonl_pipeline(data_dir):
    """Load JSONL files from local filesystem into duckdb."""
    app = _app("p_fs_jsonl")

    src = LocalFileSystemSource(
        name="local",
        bucket_url=str(data_dir),
        resources=[
            {
                "name": "events",
                "file_glob": "events/*.jsonl",
                "format": "jsonl",
                "write_disposition": "replace",
            },
        ],
    )

    app.include_source(src)
    app.run()

    rows = _query("p_fs_jsonl", "SELECT type FROM p_fs_jsonl_data.events ORDER BY id")
    types = [r[0] for r in rows]
    assert "click" in types
    assert "view" in types


def test_local_multi_resource_pipeline(data_dir):
    """Load multiple resources from the same directory."""
    app = _app("p_fs_multi")

    src = LocalFileSystemSource(
        name="local",
        bucket_url=str(data_dir),
        resources=[
            {
                "name": "users",
                "file_glob": "users/*.csv",
                "format": "csv",
                "write_disposition": "replace",
            },
            {
                "name": "events",
                "file_glob": "events/*.jsonl",
                "format": "jsonl",
                "write_disposition": "replace",
            },
        ],
    )

    app.include_source(src)
    app.run()

    # Both tables should exist
    tables = _query(
        "p_fs_multi",
        "SELECT table_name FROM information_schema.tables WHERE table_schema = 'p_fs_multi_data'",
    )
    table_names = [t[0] for t in tables]
    assert "users" in table_names
    assert "events" in table_names


def test_local_selective_resources(data_dir):
    """Run only specific resources from a filesystem source."""
    app = _app("p_fs_sel")

    src = LocalFileSystemSource(
        name="local",
        bucket_url=str(data_dir),
        resources=[
            {"name": "users", "file_glob": "users/*.csv", "format": "csv"},
            {"name": "events", "file_glob": "events/*.jsonl", "format": "jsonl"},
        ],
    )

    app.include_source(src)
    app.run(resources=["users"])

    tables = _query(
        "p_fs_sel",
        "SELECT table_name FROM information_schema.tables WHERE table_schema = 'p_fs_sel_data'",
    )
    table_names = [t[0] for t in tables]
    assert "users" in table_names
    # events should NOT be loaded
    assert "events" not in table_names


def test_include_source_fastelt_integration(data_dir):
    """FastELT properly handles filesystem sources."""
    app = _app("p_fs_incl")

    src = LocalFileSystemSource(
        name="myfiles",
        bucket_url=str(data_dir),
        resources=[
            {"name": "users", "file_glob": "users/*.csv", "format": "csv"},
        ],
    )

    app.include_source(src)
    assert "myfiles" in app.list_sources()
    assert app.list_resources() == {"myfiles": ["users"]}
