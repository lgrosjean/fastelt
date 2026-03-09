# FastELT

**A FastAPI-inspired wrapper around [dlt](https://dlthub.com/) for ELT pipelines.**

Like FastAPI wraps Starlette, FastELT wraps dlt with decorator-driven DX. You get dlt's battle-tested engine (20+ destinations, incremental loading, schema evolution, merge strategies) with FastAPI's developer experience.

```python
import csv
from fastelt import FastELT, Source
from fastelt.destinations import DuckDBDestination

local_data = Source(name="local")

@local_data.resource(primary_key="name", write_disposition="replace")
def users():
    with open("users.csv") as f:
        for row in csv.DictReader(f):
            yield row

db = DuckDBDestination()
app = FastELT(pipeline_name="my_pipeline")
app.include_source(local_data)
app.run(destination=db)
```

## Why FastELT?

| Feature | FastELT | Meltano / Singer | dlt (raw) |
|---------|---------|-------------------|-----------|
| Define pipelines | Python decorators | YAML / JSON config | Python decorators |
| Config | Source fields + `Env()` | Manual YAML | `dlt.secrets` |
| Data validation | Pydantic v2 `response_model` | None built-in | Schema inference |
| Env var management | `Env()` / `Secret()` + auto-resolve | `.env` files | `dlt.secrets` |
| Destinations | 20+ (via dlt) | 300+ connectors | 20+ |
| Learning curve | Familiar if you know FastAPI | Tool-specific DSL | dlt-specific API |

[See the full comparison](getting-started/why-fastelt.md){ .md-button }

## Key Concepts

- **Sources** group related resources with shared config — like FastAPI's `APIRouter`
- **Resources** are generator functions that `yield` dict records — like dlt resources
- **Destinations** are typed config objects (DuckDB, BigQuery, custom sinks) — registered with `app.include_destination()`
- **`@app.source`** registers a single-resource source inline — like `@app.get` in FastAPI
- **`@app.destination`** registers a custom sink function as a destination
- **`Env` / `Secret`** resolve environment variables automatically — like FastAPI's `Query()`
- **`Incremental`** tracks cursors for efficient syncing — like `Annotated[str, Incremental(...)]`
- **`response_model`** validates records through Pydantic — like FastAPI's `response_model`
- **RESTAPISource** defines REST APIs declaratively — dlt handles pagination and auth
- **Filesystem sources** load files from local disk or cloud storage (GCS)

## Quick Links

- [Why FastELT?](getting-started/why-fastelt.md) — how it compares to Meltano, Singer, and dlt
- [Installation](getting-started/installation.md) — get up and running
- [Quickstart](getting-started/quickstart.md) — build your first pipeline in 5 minutes
- [User Guide](guide/sources.md) — learn the core concepts
- [API Reference](api/app.md) — detailed API documentation
