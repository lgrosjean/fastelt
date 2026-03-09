# Filesystem Sources

::: fastelt.sources.filesystem

Sources for loading files from local disk or cloud storage.

## LocalFileSystemSource

```python
from fastelt.sources.filesystem import LocalFileSystemSource
```

### Constructor

```python
LocalFileSystemSource(
    name: str,
    bucket_url: str,
    resources: list[FileResource | dict] = [],
)
```

| Parameter | Type | Description |
|-----------|------|-------------|
| `name` | `str` | Source name |
| `bucket_url` | `str` | Local directory path |
| `resources` | `list` | File resource configs |

Extends [`Source`](source.md).

---

## GCSFileSystemSource

```python
from fastelt.sources.filesystem import GCSFileSystemSource
```

### Constructor

```python
GCSFileSystemSource(
    name: str,
    bucket_url: str,
    credentials: str | None = None,
    resources: list[FileResource | dict] = [],
)
```

| Parameter | Type | Description |
|-----------|------|-------------|
| `name` | `str` | Source name |
| `bucket_url` | `str` | GCS bucket URL (`gs://...`) |
| `credentials` | `str \| None` | Path to service account JSON (or `Env()` reference) |
| `resources` | `list` | File resource configs |

Extends `LocalFileSystemSource`.

---

## FileResource

```python
from fastelt.sources.filesystem import FileResource
```

Configuration for a single file resource.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `name` | `str` | required | Resource/table name |
| `file_glob` | `str` | required | Glob pattern (e.g. `"*.csv"`) |
| `format` | `str \| None` | inferred | `"csv"`, `"jsonl"`, or `"parquet"` |
| `primary_key` | `str \| list[str] \| None` | `None` | Primary key column(s) |
| `write_disposition` | `str` | `"append"` | Write disposition |
| `merge_key` | `str \| list[str] \| None` | `None` | Merge key column(s) |

See the [Filesystem Sources guide](../guide/filesystem.md) for detailed usage examples.

## Installation

```bash
pip install fastelt[filesystem]
```
