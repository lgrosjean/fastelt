"""Filesystem sources — load files from local disk or cloud storage.

Wraps dlt's ``filesystem`` source with fastELT's declarative config pattern.

Usage::

    from fastelt import FastELT
    from fastelt.sources import LocalFileSystemSource, GCSFileSystemSource

    # Local files
    local = LocalFileSystemSource(
        name="local_data",
        bucket_url="/path/to/data",
        resources=[
            {"name": "users", "file_glob": "users/*.csv", "format": "csv"},
            {"name": "events", "file_glob": "events/*.jsonl", "format": "jsonl"},
        ],
    )

    # GCS bucket
    gcs = GCSFileSystemSource(
        name="gcs_data",
        bucket_url="gs://my-bucket/prefix",
        credentials=Env("GCS_CREDENTIALS"),
        resources=[
            {"name": "orders", "file_glob": "orders/*.parquet", "format": "parquet"},
        ],
    )

    app = FastELT(pipeline_name="file_pipeline", destination="duckdb")
    app.include_source(local)
    app.include_source(gcs)
    app.run()
"""

from fastelt.sources.filesystem import GCSFileSystemSource, LocalFileSystemSource
from fastelt.sources.types import Incremental

__all__ = ["GCSFileSystemSource", "Incremental", "LocalFileSystemSource"]
