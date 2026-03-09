# Bigger Applications — Multiple Sources & Files

When your pipeline grows beyond a single file, you need a clear structure. This guide shows how to split your FastELT project into multiple modules — one per data source — and wire them together in a main app.

The pattern mirrors how **FastAPI** scales with routers:

| FastAPI | FastELT |
|---------|---------|
| `APIRouter()` per domain | `Source()` per data system |
| `@router.get()` | `@source.resource()` |
| `app.include_router(router)` | `app.include_source(source)` |

## File structure

```
my_pipeline/
├── __init__.py
├── main.py              ← FastELT app, includes all sources
├── destinations.py      ← shared destination config
├── models.py            ← shared Pydantic models (optional)
└── sources/
    ├── __init__.py
    ├── github.py        ← Source + resources for GitHub
    ├── stripe.py        ← Source + resources for Stripe
    └── postgres.py      ← Source + resources for Postgres
```

Each file in `sources/` defines one `Source` with its resources — just like each FastAPI router file defines one `APIRouter` with its endpoints.

## Source modules

### `sources/github.py`

```python
from pydantic import BaseModel
from fastelt import Source
from fastelt.config import Env


class Repo(BaseModel):
    id: int
    name: str
    full_name: str
    stargazers_count: int


class Issue(BaseModel):
    id: int
    title: str
    state: str
    repo_name: str


github = Source(
    name="github",
    base_url="https://api.github.com",
    token=Env("GH_TOKEN"),
    org="my-org",
)


@github.resource(primary_key="id", write_disposition="merge")
def repos() -> list[Repo]:
    """Fetch organization repositories."""
    yield {
        "id": 1,
        "name": "fastelt",
        "full_name": "my-org/fastelt",
        "stargazers_count": 42,
    }
    yield {
        "id": 2,
        "name": "databot",
        "full_name": "my-org/databot",
        "stargazers_count": 7,
    }


@github.resource(primary_key="id", write_disposition="append")
def issues(repo: Repo) -> list[Issue]:
    """Fetch issues for each repository (parent-child)."""
    yield {
        "id": 100 + repo.id,
        "title": f"Bug in {repo.name}",
        "state": "open",
        "repo_name": repo.name,
    }
```

### `sources/stripe.py`

```python
from pydantic import BaseModel
from fastelt import Source
from fastelt.config import Env


class Customer(BaseModel):
    id: str
    email: str
    name: str


class Invoice(BaseModel):
    id: str
    customer_id: str
    amount_due: int
    status: str


stripe = Source(
    name="stripe",
    base_url="https://api.stripe.com/v1",
    api_key=Env("STRIPE_API_KEY"),
)


@stripe.resource(primary_key="id", write_disposition="merge")
def customers() -> list[Customer]:
    """Fetch Stripe customers."""
    yield {"id": "cus_001", "email": "alice@example.com", "name": "Alice"}
    yield {"id": "cus_002", "email": "bob@example.com", "name": "Bob"}


@stripe.resource(primary_key="id", write_disposition="append")
def invoices() -> list[Invoice]:
    """Fetch Stripe invoices."""
    yield {
        "id": "inv_001",
        "customer_id": "cus_001",
        "amount_due": 5000,
        "status": "paid",
    }
    yield {
        "id": "inv_002",
        "customer_id": "cus_002",
        "amount_due": 3000,
        "status": "open",
    }
```

## Destinations module

### `destinations.py`

Keep destination config in one place so every source shares the same target:

```python
from fastelt.destinations import DuckDBDestination

# Shared across all sources
db = DuckDBDestination(path="warehouse.duckdb")
```

## Main app

### `main.py`

Import sources and destinations, then assemble the app:

```python
from fastelt import FastELT

from my_pipeline.destinations import db
from my_pipeline.sources.github import github
from my_pipeline.sources.stripe import stripe

app = FastELT(pipeline_name="my_pipeline")

# Include each source — like FastAPI's app.include_router()
app.include_source(github)
app.include_source(stripe)

if __name__ == "__main__":
    app.run(destination=db)
```

That's it. Each source module is self-contained, and `main.py` is just wiring.

## Selective runs

You don't always want to run everything. Pick a single source or specific resources:

```python
# Run only GitHub
app.run(destination=db, source="github")

# Run only specific resources across sources
app.run(destination=db, resources=["repos", "customers"])

# Run one resource from one source
app.run(destination=db, source="github", resources=["repos"])
```

This is useful during development or when scheduling sources on different cadences.

## Shared models

When multiple sources produce or consume the same data shape, extract models into a shared `models.py`:

### `models.py`

```python
from pydantic import BaseModel


class User(BaseModel):
    id: int
    email: str
    name: str
```

Then import from both source modules:

```python
# sources/github.py
from my_pipeline.models import User

@github.resource(primary_key="id", write_disposition="merge")
def github_users() -> list[User]:
    yield {"id": 1, "email": "alice@example.com", "name": "Alice"}


# sources/stripe.py
from my_pipeline.models import User

@stripe.resource(primary_key="id", write_disposition="merge")
def stripe_users() -> list[User]:
    yield {"id": 1, "email": "alice@example.com", "name": "Alice"}
```

!!! tip
    Shared models are optional. Start without them and extract when you see duplication.

## Recap

| Concept | What | Where |
|---------|------|-------|
| **Source module** | One `Source` + its resources | `sources/github.py` |
| **Destinations** | Shared destination config | `destinations.py` |
| **Main app** | `include_source()` for each | `main.py` |
| **Selective runs** | `source=` / `resources=` | `app.run(...)` |
| **Shared models** | Pydantic models reused across sources | `models.py` |

The pattern scales to any number of sources. Add a new data system? Create `sources/new_system.py`, define a `Source` with its resources, and add one `app.include_source()` line in `main.py`.
