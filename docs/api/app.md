# FastELT

::: fastelt.app.FastELT

The main application class — like `FastAPI()` but for ELT pipelines.

```python
from fastelt import FastELT

app = FastELT(pipeline_name="my_pipeline", destination="duckdb")
```

## Constructor

```python
FastELT(
    pipeline_name: str = "fastelt",
    *,
    destination: str | None = None,
    dataset_name: str | None = None,
)
```

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `pipeline_name` | `str` | `"fastelt"` | Name for the dlt pipeline |
| `destination` | `str \| None` | `None` | Default dlt destination (e.g. `"duckdb"`, `"postgres"`) |
| `dataset_name` | `str \| None` | `None` | Default dataset/schema name |

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

### `run()`

```python
app.run(
    *,
    source: str | None = None,
    resources: list[str] | None = None,
    destination: str | None = None,
    dataset_name: str | None = None,
    write_disposition: str | None = None,
    **pipeline_kwargs,
) -> Any
```

Run the pipeline — extract from sources, load to destination via dlt.

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `source` | `str \| None` | `None` | Run only this source |
| `resources` | `list[str] \| None` | `None` | Run only these resources |
| `destination` | `str \| None` | `None` | Override destination |
| `dataset_name` | `str \| None` | `None` | Override dataset name |
| `write_disposition` | `str \| None` | `None` | Override write disposition |
| `**pipeline_kwargs` | | | Extra kwargs forwarded to `dlt.pipeline()` |

**Raises:**

- `ValueError` — if no destination is specified
- `KeyError` — if source name is not registered

---

### `list_sources()`

```python
app.list_sources() -> list[str]
```

Returns the names of all registered sources.

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
