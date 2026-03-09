# Why FastELT?

The Python ELT ecosystem already has established tools — [Meltano](https://meltano.com/), [Singer](https://www.singer.io/), and [dlt](https://dlthub.com/). FastELT takes a different approach: **bring FastAPI's developer experience to dlt**.

## The core idea

Like FastAPI wraps Starlette with decorators and type hints, FastELT wraps dlt with a decorator-driven DX:

```python
# FastAPI                                  # FastELT
@app.get("/users")                          @app.source("users", primary_key="id")
def get_users():                            def get_users():
    return db.query(User)                       yield from db.query_all()
```

No YAML files. No config schemas to maintain. Just Python functions with decorators.

## Comparison at a glance

| | FastELT | Meltano | Singer | dlt (raw) |
|---|---|---|---|---|
| Pipeline definition | Python decorators | YAML config | JSON config | Python decorators |
| Config management | `Source` fields + `Env()` | YAML + `.env` | Manual JSON | `dlt.secrets` |
| Data validation | Pydantic v2 `response_model` | None built-in | None built-in | Schema inference |
| Env var resolution | Auto (`Env`, `Secret`, `Annotated`) | `.env` files | Manual | `dlt.secrets` |
| Destinations | 20+ (via dlt) | 300+ connectors | Varies per target | 20+ |
| Learning curve | Familiar if you know FastAPI | Meltano-specific CLI/YAML | Singer spec | dlt-specific API |

## What makes FastELT different

### 1. FastAPI-style decorator DX on top of dlt

dlt is powerful but verbose. FastELT adds the missing developer experience layer:

```python
# Raw dlt
@dlt.resource(primary_key="id", write_disposition="merge")
def repos():
    ...

source = dlt.source(repos)
pipeline = dlt.pipeline(pipeline_name="github", destination="duckdb", dataset_name="raw")
pipeline.run(source)

# FastELT — same dlt engine, cleaner DX
from fastelt.config import Env
from fastelt.destinations import DuckDBDestination

github = Source(name="github", token=Env("GH_TOKEN"))

@github.resource(primary_key="id", write_disposition="merge")
def repos():
    ...

db = DuckDBDestination()
app = FastELT(pipeline_name="github")
app.include_source(github)
app.run(destination=db)
```

### 2. Automatic environment variable resolution

Three levels of automatic env var injection — no manual `os.environ` calls:

```python
from fastelt.config import Env, Secret

# Source field values
github = Source(name="github", token=Env("GH_TOKEN"))

# Annotated type hints on resource functions
@source.resource()
def repos(token: Annotated[str, Secret("GH_TOKEN")]):
    ...

# Plain str params auto-resolve from UPPERCASED name
@source.resource()
def repos(gh_token: str):  # resolves from GH_TOKEN
    ...
```

### 3. Pydantic `response_model` for data quality

Validate, coerce types, and normalize columns — like FastAPI's `response_model`:

```python
class UserModel(BaseModel):
    name: str
    age: int  # CSV yields strings — pydantic coerces to int

    @field_validator("age")
    @classmethod
    def must_be_positive(cls, v):
        if v <= 0:
            raise ValueError("age must be > 0")
        return v

@source.resource(response_model=UserModel, frozen=True)
def users():
    yield from csv_reader()  # validated per-record
```

### 4. Source = APIRouter pattern

Group related resources with shared config:

```python
github = Source(
    name="github",
    base_url="https://api.github.com",
    token=Env("GH_TOKEN"),
    org="anthropics",
)

@github.resource(primary_key="id", write_disposition="merge")
def repositories():
    ...

@github.resource(primary_key="id", write_disposition="append")
def issues():
    ...

app.include_source(github)  # registers both resources
```

### 5. Typed destinations

Register destinations as typed config objects — like FastAPI's dependency injection:

```python
from fastelt.destinations import DuckDBDestination, BigQueryDestination

db = DuckDBDestination()
bq = BigQueryDestination(project_id="my-project", location="EU")

app.include_destination(db)
app.include_destination(bq)

app.run(destination=db)   # load to DuckDB
app.run(destination=bq)   # load to BigQuery
```

### 6. Multiple source types for different use cases

- **`Source`** — custom extraction with generator functions
- **`RESTAPISource`** — declarative REST API extraction (pagination, auth, incremental)
- **`LocalFileSystemSource`** — load files from local disk (CSV, JSONL, Parquet)
- **`GCSFileSystemSource`** — load files from Google Cloud Storage

## When to use FastELT

FastELT is a great fit when you:

- Want **FastAPI-style DX** for your dlt pipelines
- Need **automatic env var resolution** without boilerplate
- Value **Pydantic validation** in your data pipelines
- Want to **group resources** with shared config (Source pattern)
- Already know **FastAPI** and want the same developer experience

## When to consider alternatives

| Need | Alternative |
|---|---|
| 300+ pre-built connectors | **Meltano** / **Singer** (largest connector ecosystem) |
| No Python needed | **Meltano** (YAML-only config) |
| Maximum dlt control | **dlt** directly (FastELT adds a thin layer) |
