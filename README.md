# FastELT

**A FastAPI-inspired wrapper around [dlt](https://dlthub.com/) for ELT pipelines.**

FastELT brings FastAPI's developer experience to data pipelines: decorators, type hints, Pydantic v2 validation, automatic environment variable resolution — all built on top of dlt's battle-tested engine (20+ destinations, incremental loading, schema evolution, merge strategies).

## Installation

```bash
pip install fastelt
```

With optional extras:

```bash
pip install fastelt[cli]        # CLI support (Typer)
pip install fastelt[rest_api]   # REST API source (dlt rest_api)
pip install fastelt[filesystem] # Filesystem sources (local, GCS)
```

## Quickstart

```python
import csv
from fastelt import FastELT, Source

local_data = Source(name="local")

@local_data.resource(primary_key="name", write_disposition="replace")
def users():
    with open("users.csv") as f:
        for row in csv.DictReader(f):
            yield row

app = FastELT(pipeline_name="my_pipeline", destination="duckdb")
app.include_source(local_data)
app.run()
```

## Key Concepts

### Sources and Resources

A `Source` groups related resources with shared config — like FastAPI's `APIRouter`. Resources are generator functions that `yield` dict records:

```python
from fastelt import Source, Env

github = Source(
    name="github",
    base_url="https://api.github.com",
    token=Env("GH_TOKEN"),
    org="anthropics",
)

@github.resource(primary_key="id", write_disposition="merge")
def repositories():
    headers = {"Authorization": f"Bearer {github.token}"}
    resp = httpx.get(f"{github.base_url}/orgs/{github.org}/repos", headers=headers)
    yield from resp.json()
```

### `@app.source` — Quick Inline

For single-resource sources, skip the `Source` object — like `@app.get` in FastAPI:

```python
app = FastELT(pipeline_name="demo", destination="duckdb")

@app.source("users", primary_key="id")
def users():
    yield {"id": 1, "name": "Alice"}
    yield {"id": 2, "name": "Bob"}

app.run()
```

### Environment Variables

Three ways to inject env vars — all resolved automatically:

```python
from typing import Annotated
from fastelt import Env, Secret, Source

# 1. As a Source field value
github = Source(name="github", token=Env("GH_TOKEN"))

# 2. As an Annotated type hint on a resource function
@source.resource()
def repos(token: Annotated[str, Secret("GH_TOKEN")]):
    ...

# 3. Auto-resolved from plain str params (uppercased)
@source.resource()
def repos(gh_token: str):  # resolves from GH_TOKEN env var
    ...
```

`Secret` works like `Env` but masks the value in logs/repr.

### Pydantic `response_model`

Validate, coerce types, and normalize columns — like FastAPI's `response_model`:

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
    # CSV yields strings — pydantic coerces age from str to int
    with open("users.csv") as f:
        for row in csv.DictReader(f):
            yield row
```

Use `frozen=True` to reject unexpected columns with `SchemaFrozenError` instead of a warning.

**Tip:** If your resource uses a `-> list[Model]` return annotation, `response_model` is set automatically — no need to specify it twice.

### Parent-Child Resource Chaining

Resources can depend on other resources via type annotations — no extra decorator needed:

```python
from pydantic import BaseModel

class User(BaseModel):
    id: int
    name: str

class Repo(BaseModel):
    id: int
    user_id: int
    name: str

github = Source(name="github", token=Env("GH_TOKEN"))

@github.resource(primary_key="id")
def users() -> list[User]:
    yield {"id": 1, "name": "Alice"}
    yield {"id": 2, "name": "Bob"}

# Auto-detected: `user: User` matches `users() -> list[User]`
@github.resource(primary_key="id")
def repos(user: User) -> list[Repo]:
    yield {"id": 100, "user_id": user.id, "name": f"repo-{user.name}"}
```

FastELT matches the `User` type annotation on `repos(user: User)` to the `users() -> list[User]` return type. Under the hood, `users` is built as a `dlt.resource` and `repos` as a `dlt.transformer(data_from=users)`. The child function receives a validated Pydantic model instance with dot-access to fields.

Chains of any depth work: `users → repos → commits`. When running selectively (e.g., `resources=["repos"]`), parent resources are auto-included.

### REST API Source (Declarative)

For standard REST APIs, define endpoints as config — dlt handles pagination, auth, and incremental loading:

```python
from fastelt import Env, FastELT
from fastelt.sources.rest_api import RESTAPISource, BearerTokenAuth

github = RESTAPISource(
    name="github",
    base_url="https://api.github.com",
    auth=BearerTokenAuth(token=Env("GH_TOKEN")),
    paginator="header_link",
    resources=[
        {
            "name": "repos",
            "endpoint": {
                "path": "/orgs/{org}/repos",
                "params": {"org": "anthropics", "per_page": 100},
            },
            "primary_key": "id",
            "write_disposition": "merge",
        },
    ],
)

app = FastELT(pipeline_name="github_pipeline", destination="duckdb")
app.include_source(github)
app.run()
```

### Filesystem Sources

Load files from local disk or cloud storage:

```python
from fastelt.sources.filesystem import LocalFileSystemSource

src = LocalFileSystemSource(
    name="local_data",
    bucket_url="/path/to/data",
    resources=[
        {"name": "users", "file_glob": "users/*.csv", "format": "csv"},
        {"name": "events", "file_glob": "events/*.jsonl", "format": "jsonl"},
    ],
)
```

Also available: `GCSFileSystemSource` for Google Cloud Storage (`gs://` URLs).

### Incremental Loading

Use dlt's incremental cursors for efficient syncing:

```python
import dlt

@api.resource(primary_key="id", write_disposition="merge")
def events(
    updated_at=dlt.sources.incremental("updated_at", initial_value="2024-01-01"),
):
    yield {"id": 1, "name": "signup", "updated_at": "2024-06-15T10:00:00"}
```

### CLI

```bash
pip install fastelt[cli]

fastelt run --destination duckdb --source github
fastelt list
fastelt describe github:repos
```

The CLI auto-discovers your `FastELT` app instance, like `fastapi run`.

## Why FastELT?

| Feature | FastELT | Meltano / Singer | dlt (raw) |
|---------|---------|-------------------|-----------|
| Define pipelines | Python decorators | YAML / JSON config | Python decorators |
| Config | Inferred from Source fields | Manual definition | Manual / partial |
| Data validation | Pydantic v2 `response_model` | None built-in | Schema inference |
| Resource chaining | Type annotations (`User` → `Repo`) | Config-based | `dlt.transformer` + manual wiring |
| Env var management | `Env()` / `Secret()` + auto-resolve | `.env` files | `dlt.secrets` |
| Destinations | 20+ (via dlt) | 300+ connectors | 20+ |
| Learning curve | Familiar if you know FastAPI | Tool-specific DSL | dlt-specific API |

## Documentation

Full docs: [fastelt.dev](https://fastelt.dev)

## Requirements

- Python >= 3.12
- [dlt](https://dlthub.com/) (installed automatically)
- [Pydantic](https://docs.pydantic.dev/) >= 2.0 (installed automatically)
- [Loguru](https://loguru.readthedocs.io/) (installed automatically)

## License

MIT
