"""Filesystem sources — load files from local disk or cloud storage.

Wraps dlt's ``filesystem`` source to provide a declarative, multi-resource
interface for loading files from local directories or cloud buckets.

Each "resource" maps to a file glob pattern loaded as a separate table::

    LocalFileSystemSource(
        name="data",
        bucket_url="/data/lake",
        resources=[
            {"name": "users", "file_glob": "users/*.csv", "format": "csv"},
            {"name": "events", "file_glob": "events/*.jsonl", "format": "jsonl"},
        ],
    )
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from loguru import logger

from fastelt.types import Env


def _resolve_env_values(obj: Any) -> Any:
    """Recursively resolve Env instances in nested dicts/lists."""
    if isinstance(obj, Env):
        return obj.resolve()
    if isinstance(obj, dict):
        return {k: _resolve_env_values(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_resolve_env_values(v) for v in obj]
    return obj


_FORMAT_READERS = {
    "csv": "read_csv",
    "jsonl": "read_jsonl",
    "parquet": "read_parquet",
}


@dataclass
class FileResource:
    """Configuration for a single file resource within a filesystem source.

    Parameters
    ----------
    name:
        Resource/table name at the destination.
    file_glob:
        Glob pattern to match files (e.g. ``"users/*.csv"``).
    format:
        File format: ``"csv"``, ``"jsonl"``, or ``"parquet"``.
        If not specified, inferred from file_glob extension.
    primary_key:
        Column(s) used as primary key.
    write_disposition:
        How data is written: ``"append"``, ``"replace"``, or ``"merge"``.
    merge_key:
        Column(s) used to match records for merge.
    """

    name: str
    file_glob: str
    format: str | None = None
    primary_key: str | list[str] | None = None
    write_disposition: str = "append"
    merge_key: str | list[str] | None = None

    def __post_init__(self) -> None:
        if self.format is None:
            self.format = _infer_format(self.file_glob)


def _infer_format(file_glob: str) -> str:
    """Infer file format from glob pattern extension."""
    lower = file_glob.lower()
    if lower.endswith(".csv"):
        return "csv"
    if lower.endswith(".jsonl") or lower.endswith(".ndjson"):
        return "jsonl"
    if lower.endswith(".parquet"):
        return "parquet"
    raise ValueError(
        f"Cannot infer format from '{file_glob}'. "
        f"Specify format explicitly: 'csv', 'jsonl', or 'parquet'."
    )


@dataclass
class LocalFileSystemSource:
    """Load files from local filesystem into dlt destinations.

    Wraps ``dlt.sources.filesystem`` with multi-resource support.
    Each resource maps a file glob pattern to a destination table.

    Parameters
    ----------
    name:
        Source name (used for referencing in FastELT).
    bucket_url:
        Local directory path (e.g. ``"/data/lake"`` or ``"./data"``).
    resources:
        List of resource configs. Each can be a ``FileResource`` or a dict
        with the same fields (``name``, ``file_glob``, ``format``, etc.).

    Usage::

        from fastelt.sources import LocalFileSystemSource

        src = LocalFileSystemSource(
            name="local_data",
            bucket_url="/path/to/data",
            resources=[
                {"name": "users", "file_glob": "users/*.csv", "format": "csv"},
                {"name": "events", "file_glob": "events/*.jsonl", "format": "jsonl"},
            ],
        )
    """

    name: str
    bucket_url: str
    resources: list[FileResource | dict[str, Any]] = field(default_factory=list)

    def _normalize_resources(self) -> list[FileResource]:
        """Convert dict resources to FileResource objects."""
        result = []
        for r in self.resources:
            if isinstance(r, dict):
                r = _resolve_env_values(r)
                result.append(FileResource(**r))
            else:
                result.append(r)
        return result

    def _build_dlt_source(
        self,
        resource_names: list[str] | None = None,
    ) -> Any:
        """Build dlt resources from filesystem config."""
        try:
            from dlt.sources.filesystem import filesystem, read_csv, read_jsonl, read_parquet
        except ImportError as e:
            raise ImportError(
                "dlt filesystem source is required. "
                "Install with: pip install dlt[filesystem]"
            ) from e

        readers = {
            "csv": read_csv,
            "jsonl": read_jsonl,
            "parquet": read_parquet,
        }

        file_resources = self._normalize_resources()
        if resource_names:
            file_resources = [r for r in file_resources if r.name in resource_names]

        bucket_url = _resolve_env_values(self.bucket_url)

        dlt_resources = []
        for res in file_resources:
            fmt = res.format
            if fmt not in readers:
                raise ValueError(
                    f"Unsupported format '{fmt}' for resource '{res.name}'. "
                    f"Supported: {list(readers.keys())}"
                )

            reader = readers[fmt]

            logger.debug(
                "Building filesystem resource '{}': {} ({})",
                res.name,
                res.file_glob,
                fmt,
            )

            # Create filesystem source piped through reader
            fs_resource = filesystem(
                bucket_url=bucket_url,
                file_glob=res.file_glob,
            ) | reader()

            # Apply hints for table name, primary key, write disposition
            hints: dict[str, Any] = {"table_name": res.name}
            if res.primary_key:
                hints["primary_key"] = res.primary_key
            if res.write_disposition:
                hints["write_disposition"] = res.write_disposition
            if res.merge_key:
                hints["merge_key"] = res.merge_key

            fs_resource.apply_hints(**hints)
            dlt_resources.append(fs_resource)

        logger.debug(
            "Building filesystem source '{}' with {} resources",
            self.name,
            len(dlt_resources),
        )

        # Return as a list — FastELT.run() passes this to pipeline.run()
        import dlt

        @dlt.source(name=self.name)
        def _make_source() -> Any:
            return dlt_resources

        return _make_source()

    def list_resources(self) -> list[str]:
        """Return resource names."""
        return [
            r.name if isinstance(r, FileResource) else r["name"]
            for r in self.resources
        ]


@dataclass
class GCSFileSystemSource(LocalFileSystemSource):
    """Load files from Google Cloud Storage into dlt destinations.

    Same as ``LocalFileSystemSource`` but with GCS-specific config.
    The ``bucket_url`` should use the ``gs://`` protocol.

    Parameters
    ----------
    name:
        Source name.
    bucket_url:
        GCS bucket URL (e.g. ``"gs://my-bucket/prefix"``).
    credentials:
        GCS credentials — either a path to a service account JSON file,
        a dict with service account info, or an ``Env`` reference.
        If not provided, uses Application Default Credentials.
    resources:
        List of resource configs (same as ``LocalFileSystemSource``).

    Usage::

        from fastelt import Env
        from fastelt.sources import GCSFileSystemSource

        gcs = GCSFileSystemSource(
            name="gcs_data",
            bucket_url="gs://my-bucket/data",
            credentials=Env("GOOGLE_APPLICATION_CREDENTIALS"),
            resources=[
                {"name": "orders", "file_glob": "orders/*.parquet", "format": "parquet"},
                {"name": "users", "file_glob": "users/*.csv", "format": "csv"},
            ],
        )
    """

    credentials: Any | None = None

    def _build_dlt_source(
        self,
        resource_names: list[str] | None = None,
    ) -> Any:
        """Build dlt resources with GCS credentials."""
        try:
            from dlt.sources.filesystem import filesystem, read_csv, read_jsonl, read_parquet
        except ImportError as e:
            raise ImportError(
                "dlt filesystem source is required. "
                "Install with: pip install dlt[filesystem] gcsfs"
            ) from e

        readers = {
            "csv": read_csv,
            "jsonl": read_jsonl,
            "parquet": read_parquet,
        }

        file_resources = self._normalize_resources()
        if resource_names:
            file_resources = [r for r in file_resources if r.name in resource_names]

        bucket_url = _resolve_env_values(self.bucket_url)
        credentials = _resolve_env_values(self.credentials)

        dlt_resources = []
        for res in file_resources:
            fmt = res.format
            if fmt not in readers:
                raise ValueError(
                    f"Unsupported format '{fmt}' for resource '{res.name}'. "
                    f"Supported: {list(readers.keys())}"
                )

            reader = readers[fmt]

            logger.debug(
                "Building GCS filesystem resource '{}': {} ({})",
                res.name,
                res.file_glob,
                fmt,
            )

            # Build filesystem kwargs
            fs_kwargs: dict[str, Any] = {
                "bucket_url": bucket_url,
                "file_glob": res.file_glob,
            }

            # Pass credentials to dlt filesystem if provided
            # dlt uses fsspec under the hood — credentials flow through config
            fs_resource = filesystem(**fs_kwargs) | reader()

            # Apply hints
            hints: dict[str, Any] = {"table_name": res.name}
            if res.primary_key:
                hints["primary_key"] = res.primary_key
            if res.write_disposition:
                hints["write_disposition"] = res.write_disposition
            if res.merge_key:
                hints["merge_key"] = res.merge_key

            fs_resource.apply_hints(**hints)
            dlt_resources.append(fs_resource)

        logger.debug(
            "Building GCS filesystem source '{}' with {} resources",
            self.name,
            len(dlt_resources),
        )

        import dlt

        # Inject GCS credentials into dlt config if provided
        if credentials:
            import os
            if isinstance(credentials, str) and not credentials.startswith("{"):
                # Path to service account JSON
                os.environ.setdefault("CREDENTIALS__FILESYSTEM__BUCKET_URL", bucket_url)
                os.environ.setdefault("CREDENTIALS__FILESYSTEM__CREDENTIALS", credentials)

        @dlt.source(name=self.name)
        def _make_source() -> Any:
            return dlt_resources

        return _make_source()
