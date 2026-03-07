# Why FastELT?

The Python ELT ecosystem already has established tools — [Meltano](https://meltano.com/), [Singer](https://www.singer.io/), and [dlt](https://dlthub.com/). FastELT takes a different approach: **bring FastAPI's developer experience to data pipelines**.

## The core idea

If you've used FastAPI, you know the feeling: define a function, add type hints, and the framework handles validation, documentation, and config for you. FastELT applies the same philosophy to ELT:

```python
# FastAPI                                  # FastELT
@app.get("/users")                          @app.extractor()
def get_users(                              def get_users(
    limit: int = Query(default=10),             limit: int = Field(default=10),
) -> list[User]:                            ) -> Iterator[User]:
    ...                                         ...
```

No YAML files. No config schemas to maintain. No plugin registries. Just Python functions with type hints.

## Comparison at a glance

| | FastELT | Meltano | Singer | dlt |
|---|---|---|---|---|
| Pipeline definition | Python decorators | YAML config | JSON config | Python decorators |
| Config schema | Inferred from signature | Manual YAML | Manual JSON | Manual/partial |
| Data validation | Pydantic v2 (rust-powered) | None built-in | None built-in | Schema inference |
| Core dependencies | 2 (`pydantic`, `loguru`) | 10+ (SQLAlchemy, Flask, ...) | Varies per tap | 5+ (duckdb, sqlalchemy, ...) |
| Learning curve | Familiar if you know FastAPI | Meltano-specific CLI/YAML | Singer spec | dlt-specific API |
| Extensibility | Plain Python functions | Plugin system | Tap/target repos | Decorators + config |

## What makes FastELT different

### 1. Zero-config schema inference

In Meltano or Singer, you define config schemas manually:

```yaml
# meltano.yml
plugins:
  extractors:
    - name: tap-csv
      config:
        csv_files_definition: files.json
        delimiter: ","
```

In FastELT, the config schema is your function signature:

```python
@app.extractor()
def csv_users(
    path: str = Field(..., description="Path to CSV file"),
    delimiter: str = Field(default=","),
) -> Iterator[User]:
    ...
```

The framework generates a Pydantic model from the signature, validates inputs, and can auto-generate documentation — exactly like FastAPI does with OpenAPI.

### 2. Type-safe records with Pydantic v2

Every record flowing through a FastELT pipeline is a Pydantic model, validated at extraction time:

```python
class User(BaseModel):
    name: str
    email: str
    age: int  # will reject non-integer values immediately

@app.extractor()
def api_users(endpoint: str = Field(...)) -> Iterator[User]:
    for item in fetch(endpoint):
        yield User(**item)  # validated here
```

Singer taps emit raw JSON dictionaries with no runtime validation. dlt has schema inference but doesn't leverage Pydantic's rust-powered validation engine. FastELT catches bad data at the source, not at the warehouse.

### 3. Lightweight and embeddable

FastELT is ~500 lines of code with 2 runtime dependencies. It's a library, not a platform:

```
fastelt: pydantic + loguru
meltano: SQLAlchemy + Alembic + Flask + click + structlog + ...
```

This means you can `pip install fastelt` into any existing Python project — a Django app, a data science notebook, a CLI tool — without pulling in a framework. There's no `meltano init`, no project scaffold, no database to manage.

### 4. Source = APIRouter pattern

When multiple extractors share the same connection (e.g., a GitHub API token), FastELT's `Source` groups them with shared config — like FastAPI's `APIRouter`:

```python
github = Source(
    base_url="https://api.github.com",
    token="ghp_...",
    org="anthropics",
)

@github.entity()
def repositories(min_stars: int = Field(default=0)) -> Iterator[Repository]:
    ...

@github.entity()
def pull_requests(repo: str = Field(...)) -> Iterator[PullRequest]:
    ...

app.include_source(github)  # registers both extractors
```

In Singer, each tap is a standalone process with its own config. In Meltano, you configure each extractor separately. FastELT lets you define connection details once and share them across related extractors.

### 5. Composable plugin system

Adding built-in or third-party extractors/loaders is a single line:

```python
app.include(csv_extractor(User))
app.include(json_loader(User))
```

Mix and match built-in plugins with custom functions freely — no plugin registry, no installation step, no version pinning. It's just Python imports.

## When to use FastELT

FastELT is a great fit when you:

- Want **code-first pipelines** without YAML/JSON config files
- Need to **embed ELT** into an existing Python application
- Value **type safety** and Pydantic validation in your data pipelines
- Prefer a **lightweight library** over a full platform
- Already know **FastAPI** and want the same developer experience

## When to consider alternatives

FastELT is focused on simplicity. If you need these features today, consider the alternatives:

| Feature | Alternative |
|---|---|
| 300+ pre-built connectors | **Meltano** / **Singer** (largest connector ecosystem) |
| Incremental sync / state management | **Meltano** (bookmarks), **dlt** (incremental loading) |
| Auto-create warehouse tables | **dlt** (schema evolution, auto-migration) |
| Built-in orchestration | **Meltano** (Airflow/Dagster integration) |
| Schema evolution / migrations | **dlt** (automatic schema management) |

FastELT focuses on doing one thing well: making it trivially easy to write, validate, and compose ELT pipelines in pure Python. Think of it as **the Flask/FastAPI of ELT** — minimal, explicit, and extensible.
