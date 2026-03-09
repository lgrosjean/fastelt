# FastELT

::: fastelt.app.FastELT

The main application class — like `FastAPI()` but for ELT pipelines.

```python
from fastelt import FastELT

app = FastELT(pipeline_name="my_pipeline")
```

## Constructor

```python
FastELT(
    pipeline_name: str = "fastelt",
)
```

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `pipeline_name` | `str` | `"fastelt"` | Name for the dlt pipeline |

## Methods

### `source()`

```python
@app.source(
    name: str | None = None,
    *,
    primary_key: str | list[str] | None = None,
    write_disposition: str = "append",
    merge_key: str | list[str] | None = None,
    table_name: str | None = None,
    response_model: type[BaseModel] | None = None,
    frozen: bool = False,
) -> Callable
```

Decorator to register a single-resource source — like `@app.get` in FastAPI.

**Parameters:**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `name` | `str \| None` | function name | Source and resource name |
| `primary_key` | `str \| list[str] \| None` | `None` | Primary key column(s) |
| `write_disposition` | `str` | `"append"` | `"append"`, `"replace"`, or `"merge"` |
| `merge_key` | `str \| list[str] \| None` | `None` | Merge key column(s) |
| `table_name` | `str \| None` | `None` | Destination table name |
| `response_model` | `type[BaseModel] \| None` | `None` | Pydantic model for validation |
| `frozen` | `bool` | `False` | Reject extra columns |

```python
@app.source("users", primary_key="id", response_model=UserModel)
def users():
    yield {"id": 1, "name": "Alice"}
```

---

### `include_source()`

```python
app.include_source(source: Source, name: str | None = None) -> None
```

Register a source with its resources — like `app.include_router()` in FastAPI.

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `source` | `Source` | required | A Source instance (or subclass) |
| `name` | `str \| None` | `None` | Optional name override |

```python
app.include_source(github)
```

---

### `destination()`

```python
@app.destination(
    *,
    batch_size: int = 10,
    loader_file_format: str | None = None,
    naming_convention: str = "direct",
    skip_dlt_columns_and_tables: bool = True,
    max_table_nesting: int = 0,
    dataset_name: str | None = None,
) -> Callable
```

Decorator to register a custom sink function as a destination — like `@app.get` in FastAPI.

The function name becomes the destination name.

**Parameters:**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `batch_size` | `int` | `10` | Number of items per call (0 for file paths) |
| `loader_file_format` | `str \| None` | `None` | Load file format (`"typed-jsonl"`, `"parquet"`, etc.) |
| `naming_convention` | `str` | `"direct"` | Table/column name normalization |
| `skip_dlt_columns_and_tables` | `bool` | `True` | Exclude internal dlt tables/columns |
| `max_table_nesting` | `int` | `0` | How deep to flatten nested fields |
| `dataset_name` | `str \| None` | `None` | Dataset/schema name |

```python
@app.destination(batch_size=100)
def my_sink(items, table):
    for item in items:
        print(f"{table['name']}: {item}")

app.run(destination=my_sink)
```

---

### `include_destination()`

```python
app.include_destination(destination: Destination) -> None
```

Register a destination.

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `destination` | `Destination` | required | A Destination instance (or subclass) |

```python
from fastelt.destinations import DuckDBDestination

db = DuckDBDestination()
app.include_destination(db)
```

---

### `run()`

```python
app.run(
    *,
    destination: Destination | str,
    source: str | None = None,
    resources: list[str] | None = None,
    dataset_name: str | None = None,
    write_disposition: str | None = None,
    **pipeline_kwargs,
) -> Any
```

Run the pipeline — extract from sources, load to destination via dlt.

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `destination` | `Destination \| str` | required | A `Destination` object or registered name |
| `source` | `str \| None` | `None` | Run only this source |
| `resources` | `list[str] \| None` | `None` | Run only these resources |
| `dataset_name` | `str \| None` | `None` | Override dataset name |
| `write_disposition` | `str \| None` | `None` | Override write disposition |
| `**pipeline_kwargs` | | | Extra kwargs forwarded to `dlt.pipeline()` |

**Raises:**

- `ValueError` — if no sources are registered
- `KeyError` — if source or destination name is not found

```python
app.run(destination=db)
app.run(destination=db, source="github")
app.run(destination=db, resources=["repos", "issues"])
```

---

### `list_sources()`

```python
app.list_sources() -> list[str]
```

Returns the names of all registered sources.

---

### `list_destinations()`

```python
app.list_destinations() -> list[str]
```

Returns the names of all registered destinations.

---

### `list_resources()`

```python
app.list_resources(source: str | None = None) -> dict[str, list[str]]
```

Returns resources grouped by source name.

---

### `get_source()`

```python
app.get_source(name: str) -> Source
```

Returns the registered `Source` instance by name.

**Raises:** `KeyError` if not found.

---

### `get_destination()`

```python
app.get_destination(name: str) -> Destination
```

Returns the registered `Destination` instance by name.

**Raises:** `KeyError` if not found.
