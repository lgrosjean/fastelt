"""Filesystem-based destinations."""

from __future__ import annotations

from typing import Any

import dlt

from fastelt.destinations.base import Destination


class FileSystemDestination(Destination):
    """Base filesystem destination wrapping dlt's filesystem destination.

    Subclass this to create specific filesystem-backed destinations
    (local, GCS, S3, etc.).

    Usage::

        from fastelt.destinations import FileSystemDestination

        dest = FileSystemDestination(bucket_url="gs://my-bucket/data")

        app.include_destination(dest)
        app.run(destination=dest)
    """

    destination_type: str = "filesystem"
    bucket_url: str = "outputs"
    loader_file_format: str = "jsonl"
    disable_compression: bool = True
    auto_mkdir: bool = True

    def _to_dlt_kwargs(self) -> dict[str, Any]:
        if self.auto_mkdir:
            from pathlib import Path

            Path(self.bucket_url).mkdir(parents=True, exist_ok=True)
        kwargs: dict[str, Any] = {
            "destination": dlt.destinations.filesystem(bucket_url=self.bucket_url),
            "loader_file_format": self.loader_file_format,
        }
        if self.dataset_name:
            kwargs["dataset_name"] = self.dataset_name
        return kwargs


