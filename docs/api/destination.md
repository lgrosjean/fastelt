# Destination

::: fastelt.destinations.Destination

Typed configuration objects describing where data is loaded. The destination name is auto-derived from the class name (e.g. `BigQueryDestination` → `"bigquery"`).

```python
from fastelt.destinations import Destination
```

## Built-in Destinations

### DuckDBDestination

```python
from fastelt.destinations import DuckDBDestination

db = DuckDBDestination()
db = DuckDBDestination(database="my_pipeline.duckdb")
db = DuckDBDestination(dataset_name="raw_data")
```

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `database` | `str \| None` | `None` | Path to DuckDB file |
| `dataset_name` | `str \| None` | `None` | Dataset/schema name |

---

### BigQueryDestination

```python
from fastelt.destinations import BigQueryDestination
from fastelt.config import Secret

bq = BigQueryDestination(
    project_id="my-project",
    location="EU",
    dataset_name="analytics",
    credentials=Secret("GOOGLE_APPLICATION_CREDENTIALS"),
)
```

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `project_id` | `str \| None` | `None` | GCP project ID |
| `location` | `str` | `"US"` | BigQuery location |
| `credentials` | `str \| None` | `None` | Service account credentials |
| `dataset_name` | `str \| None` | `None` | Dataset name |

---

## Custom Destinations

### Class-based

Create a subclass of `Destination` with typed fields:

```python
from fastelt.destinations import Destination

class PostgresDestination(Destination):
    destination_type: str = "postgres"
    host: str = "localhost"
    port: int = 5432
    database: str = "analytics"

pg = PostgresDestination(host="db.example.com")
app.include_destination(pg)
app.run(destination=pg)
```

The name is derived from the class: `PostgresDestination` → `"postgres"`.

---

### Decorator-based (`@app.destination`)

For custom sink functions, use `@app.destination()`:

```python
@app.destination(batch_size=100)
def my_sink(items, table):
    for item in items:
        print(f"{table['name']}: {item}")

app.run(destination=my_sink)
```

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `batch_size` | `int` | `10` | Number of items per call (0 for file paths) |
| `loader_file_format` | `str \| None` | `None` | Load file format |
| `naming_convention` | `str` | `"direct"` | Table/column name normalization |
| `skip_dlt_columns_and_tables` | `bool` | `True` | Exclude internal dlt tables/columns |
| `max_table_nesting` | `int` | `0` | How deep to flatten nested fields |
| `dataset_name` | `str \| None` | `None` | Dataset/schema name |

---

## Base class API

### `name` (property)

```python
destination.name -> str
```

Auto-derived from the class name: `BigQueryDestination` → `"bigquery"`, `DuckDBDestination` → `"duckdb"`.

### Fields

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `destination_type` | `str` | required | dlt destination identifier |
| `dataset_name` | `str \| None` | `None` | Dataset/schema name at the destination |

### Usage with `Env` / `Secret`

Destination fields support `Env` and `Secret` for environment variable resolution:

```python
from fastelt.config import Env, Secret

bq = BigQueryDestination(
    project_id=Env("GCP_PROJECT"),
    credentials=Secret("GOOGLE_APPLICATION_CREDENTIALS"),
)
```

Values are resolved at construction time.
