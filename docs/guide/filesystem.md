# Filesystem Sources

Load files from local disk or cloud storage into dlt destinations. Each "resource" maps a file glob pattern to a destination table.

## LocalFileSystemSource

```python
from fastelt import FastELT
from fastelt.sources.filesystem import LocalFileSystemSource

src = LocalFileSystemSource(
    name="local_data",
    bucket_url="/path/to/data",
    resources=[
        {"name": "users", "file_glob": "users/*.csv", "format": "csv"},
        {"name": "events", "file_glob": "events/*.jsonl", "format": "jsonl"},
        {"name": "metrics", "file_glob": "metrics/*.parquet", "format": "parquet"},
    ],
)

app = FastELT(pipeline_name="file_pipeline", destination="duckdb")
app.include_source(src)
app.run()
```

## GCSFileSystemSource

Same interface as `LocalFileSystemSource` but for Google Cloud Storage:

```python
from fastelt import Env
from fastelt.sources.filesystem import GCSFileSystemSource

gcs = GCSFileSystemSource(
    name="gcs_data",
    bucket_url="gs://my-bucket/data",
    credentials=Env("GOOGLE_APPLICATION_CREDENTIALS"),
    resources=[
        {"name": "orders", "file_glob": "orders/*.parquet", "format": "parquet"},
        {"name": "users", "file_glob": "users/*.csv", "format": "csv"},
    ],
)
```

## Resource config

Each resource accepts:

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `name` | `str` | required | Resource/table name at destination |
| `file_glob` | `str` | required | Glob pattern (e.g. `"users/*.csv"`) |
| `format` | `str \| None` | inferred | `"csv"`, `"jsonl"`, or `"parquet"` |
| `primary_key` | `str \| list[str] \| None` | `None` | Primary key column(s) |
| `write_disposition` | `str` | `"append"` | `"append"`, `"replace"`, or `"merge"` |
| `merge_key` | `str \| list[str] \| None` | `None` | Merge key column(s) |

Format is auto-inferred from the file extension if not specified.

## Supported formats

| Format | Extensions | Reader |
|--------|-----------|--------|
| CSV | `.csv` | `dlt.sources.filesystem.read_csv` |
| JSONL | `.jsonl`, `.ndjson` | `dlt.sources.filesystem.read_jsonl` |
| Parquet | `.parquet` | `dlt.sources.filesystem.read_parquet` |

## Installation

```bash
pip install fastelt[filesystem]
```
