# Sources & Resources

Sources group related resources with shared configuration — like FastAPI's `APIRouter`.

## Why Sources?

When extracting from the same system (e.g., GitHub API, a database), multiple resources share the same connection details. A `Source` lets you define them once:

```python
from fastelt import Source, Env

github = Source(
    name="github",
    base_url="https://api.github.com",
    token=Env("GH_TOKEN"),
    org="anthropics",
)
```

## Registering resources

Use `@source.resource()` to register generator functions that yield dict records:

```python
import dlt
import httpx

@github.resource(primary_key="id", write_disposition="merge")
def repositories(
    updated_at=dlt.sources.incremental("updated_at", initial_value="2020-01-01"),
):
    headers = {"Authorization": f"Bearer {github.token}"}
    resp = httpx.get(
        f"{github.base_url}/orgs/{github.org}/repos",
        headers=headers,
    )
    yield from resp.json()
```

Then include the source in your app:

```python
from fastelt import FastELT

app = FastELT(pipeline_name="github_pipeline", destination="duckdb")
app.include_source(github)
app.run()
```

## Resource decorator parameters

```python
@source.resource(
    name="repos",              # Override function name (default: function name)
    description="Fetch repos", # Human-readable description (default: docstring)
    tags=["core", "github"],   # Categorization tags
    deprecated=False,          # Emit warning at runtime
    primary_key="id",          # Column(s) for primary key
    write_disposition="merge", # "append", "replace", or "merge"
    merge_key="updated_at",    # Column(s) for merge matching
    table_name="repositories", # Destination table name (default: resource name)
    selected=True,             # Whether this runs by default
    response_model=RepoModel,  # Pydantic model for validation
    frozen=False,              # Reject extra columns (requires response_model)
)
def repos():
    ...
```

## `@app.source` — quick inline

For single-resource sources, use `@app.source` to skip creating a `Source` object:

```python
app = FastELT(pipeline_name="demo", destination="duckdb")

@app.source("users", primary_key="id", write_disposition="replace")
def users():
    yield {"id": 1, "name": "Alice"}
    yield {"id": 2, "name": "Bob"}

app.run()
```

This creates a `Source` behind the scenes and registers the function as its single resource.

## Creating Sources

### Programmatic (simple)

Pass keyword arguments — a typed Pydantic subclass is created dynamically:

```python
github = Source(
    name="github",
    base_url="https://api.github.com",
    token=Env("GH_TOKEN"),
    org="anthropics",
)
```

### Class-based (advanced)

For complex schemas with validators, descriptions, or type constraints:

```python
from pydantic import field_validator

class GitHubSource(Source):
    base_url: str = "https://api.github.com"
    token: str
    org: str

    @field_validator("base_url")
    @classmethod
    def must_be_url(cls, v: str) -> str:
        if not v.startswith("https://"):
            raise ValueError("base_url must use HTTPS")
        return v

github = GitHubSource(token="ghp_...", org="anthropics")
```

Since `Source` extends `BaseModel`, you get full Pydantic v2 features: validators, serialization, JSON schema, etc.

## Environment variables

### `Env` — lazy env var reference

```python
from fastelt import Env, Source

# Resolved at Source construction time
github = Source(name="github", token=Env("GH_TOKEN"))

# With default value
github = Source(name="github", token=Env("GH_TOKEN", default="fallback"))
```

### `Secret` — masked in logs

```python
from fastelt import Secret, Source

github = Source(name="github", token=Secret("GH_TOKEN"))
# repr: Source(name='github', token=Secret('GH_TOKEN'))
```

### `Annotated` type hints on resource functions

```python
from typing import Annotated
from fastelt import Env, Secret

@source.resource()
def repos(token: Annotated[str, Secret("GH_TOKEN")]):
    ...
```

### Auto-resolution from plain `str` params

Any `str`-typed parameter auto-resolves from the uppercased env var name:

```python
@source.resource()
def repos(gh_token: str):  # resolves from GH_TOKEN
    ...

@source.resource()
def repos(gh_token: str = "fallback"):  # tries GH_TOKEN, falls back to "fallback"
    ...
```

## Source injection

Resources access source config through Python closures — just reference the source variable:

```python
github = Source(name="github", base_url="https://api.github.com", token=Env("GH_TOKEN"))

@github.resource()
def repos():
    headers = {"Authorization": f"Bearer {github.token}"}
    resp = httpx.get(f"{github.base_url}/repos", headers=headers)
    yield from resp.json()
```

Alternatively, declare the source as a function parameter (detected by type annotation or convention):

```python
# Explicit type annotation
@github.resource()
def repos(source: GitHubSource):
    print(source.base_url)
    ...

# Convention: unannotated param with no default
@github.resource()
def repos(src):
    print(src.base_url)
    ...
```

## Multiple resources

A single source can have multiple resources:

```python
@github.resource(primary_key="id", write_disposition="merge")
def repositories():
    ...

@github.resource(primary_key="id", write_disposition="append")
def issues():
    ...

@github.resource(primary_key="id", write_disposition="replace")
def stargazers():
    ...

app.include_source(github)  # registers all three
```

## Selective runs

Run specific sources or resources:

```python
# Run only the "github" source
app.run(source="github")

# Run only specific resources
app.run(resources=["repositories", "issues"])

# Both
app.run(source="github", resources=["repositories"])
```

## Incremental loading

Use dlt's incremental cursors for efficient syncing:

```python
import dlt

@api.resource(primary_key="id", write_disposition="merge")
def events(
    updated_at=dlt.sources.incremental("updated_at", initial_value="2024-01-01"),
):
    print(f"Fetching events since {updated_at.last_value}")
    yield {"id": 1, "name": "signup", "updated_at": "2024-06-15T10:00:00"}
```

On subsequent runs, `updated_at.last_value` reflects the last seen cursor value — dlt tracks this automatically.
