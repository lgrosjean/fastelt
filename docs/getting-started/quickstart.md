# Quickstart

This guide walks you through building your first FastELT pipeline.

## 1. Create a Source and register a resource

Like FastAPI uses `APIRouter` to group endpoints, FastELT uses `Source` to group resources:

```python
import csv
from fastelt import FastELT, Source

local_data = Source(name="local")

@local_data.resource(primary_key="name", write_disposition="replace")
def users():
    with open("users.csv") as f:
        for row in csv.DictReader(f):
            yield row
```

Resources are generator functions that `yield` dict records. dlt handles schema inference and loading.

## 2. Create a destination and run

```python
from fastelt.destinations import DuckDBDestination

db = DuckDBDestination()
app = FastELT(pipeline_name="my_pipeline")
app.include_source(local_data)
app.run(destination=db)
```

That's it — data flows from your CSV into DuckDB.

## 3. Or use `@app.source` for quick inline sources

For single-resource sources, skip the `Source` object entirely:

```python
from fastelt import FastELT
from fastelt.destinations import DuckDBDestination

db = DuckDBDestination()
app = FastELT(pipeline_name="demo")

@app.source("users", primary_key="id")
def users():
    yield {"id": 1, "name": "Alice"}
    yield {"id": 2, "name": "Bob"}

app.run(destination=db)
```

## 4. Add data validation with `response_model`

Like FastAPI's `response_model`, validate each record through a Pydantic model:

```python
from pydantic import BaseModel, field_validator

class UserModel(BaseModel):
    name: str
    email: str
    age: int

    @field_validator("age")
    @classmethod
    def age_must_be_positive(cls, v):
        if v <= 0:
            raise ValueError(f"age must be > 0, got {v}")
        return v

@local_data.resource(
    response_model=UserModel,
    primary_key="name",
    write_disposition="replace",
)
def users():
    with open("users.csv") as f:
        for row in csv.DictReader(f):
            yield row  # pydantic coerces age from str to int
```

## 5. Use environment variables

Inject secrets without hardcoding:

```python
from fastelt import Source
from fastelt.config import Env

github = Source(
    name="github",
    base_url="https://api.github.com",
    token=Env("GH_TOKEN"),
)
```

## 6. Or use the CLI

```bash
pip install fastelt[cli]

fastelt run duckdb
fastelt list
fastelt describe local:users
```

The CLI auto-discovers your `FastELT` app instance from `fastelt_app.py`, `main.py`, or `app.py`.

## Next steps

- [Sources & Resources](../guide/sources.md) — shared config, env vars, source injection
- [REST API Source](../guide/rest-api.md) — declarative REST API extraction
- [Filesystem Sources](../guide/filesystem.md) — load files from local disk or GCS
- [Data Validation](../guide/validation.md) — `response_model`, `frozen`, schema control
- [CLI](../guide/cli.md) — run pipelines from the command line
