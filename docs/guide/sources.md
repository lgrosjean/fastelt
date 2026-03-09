# Sources & Resources

Sources group related resources with shared configuration — like FastAPI's `APIRouter`.

## Why Sources?

When extracting from the same system (e.g., GitHub API, a database), multiple resources share the same connection details. A `Source` lets you define them once:

```python
from fastelt import Source
from fastelt.config import Env

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
from typing import Annotated
from fastelt.sources import Incremental
import httpx

@github.resource(primary_key="id", write_disposition="merge")
def repositories(
    updated_at: Annotated[str, Incremental(initial_value="2020-01-01")],
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
from fastelt.destinations import DuckDBDestination

db = DuckDBDestination()
app = FastELT(pipeline_name="github_pipeline")
app.include_source(github)
app.run(destination=db)
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
from fastelt import FastELT
from fastelt.destinations import DuckDBDestination

db = DuckDBDestination()
app = FastELT(pipeline_name="demo")

@app.source("users", primary_key="id", write_disposition="replace")
def users():
    yield {"id": 1, "name": "Alice"}
    yield {"id": 2, "name": "Bob"}

app.run(destination=db)
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
from fastelt.config import Env

# Resolved at Source construction time
github = Source(name="github", token=Env("GH_TOKEN"))

# With default value
github = Source(name="github", token=Env("GH_TOKEN", default="fallback"))
```

### `Secret` — masked in logs

```python
from fastelt.config import Secret

github = Source(name="github", token=Secret("GH_TOKEN"))
# repr: Source(name='github', token=Secret('GH_TOKEN'))
```

### `Annotated` type hints on resource functions

```python
from typing import Annotated
from fastelt.config import Secret

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

!!! tip
    As your project grows, you'll want to split sources into separate files. See [Bigger Applications](bigger-applications.md) for the recommended structure.

## Parent-Child Resource Chaining

Resources can depend on other resources via type annotations. FastELT auto-detects the dependency by matching a parameter's `BaseModel` type to the return type of another resource — no new decorator needed.

### Basic chaining

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

# `user: User` matches `users() -> list[User]` → auto-detected as child
@github.resource(primary_key="id")
def repos(user: User) -> list[Repo]:
    yield {"id": 100, "user_id": user.id, "name": f"repo-{user.name}"}
```

The child function receives a **validated Pydantic model instance** — you get dot-access (`user.id`, `user.name`) instead of dict bracket access.

Under the hood, `users` becomes a `dlt.resource` and `repos` becomes a `dlt.transformer(data_from=users)`.

### Multi-level chains

Chains of any depth work:

```python
class Commit(BaseModel):
    id: int
    repo_id: int
    message: str

@github.resource(primary_key="id")
def commits(repo: Repo) -> list[Commit]:
    yield {"id": 1000, "repo_id": repo.id, "message": "init"}
```

This builds: `users → repos → commits`.

### Auto `response_model` from return type

When you annotate a resource with `-> list[Model]`, the `response_model` is set automatically:

```python
@github.resource(primary_key="id")
def users() -> list[User]:  # response_model=User is inferred
    yield {"id": 1, "name": "Alice"}
```

Supported return type forms: `list[Model]`, `Iterator[Model]`, `Generator[Model, ...]`, `Iterable[Model]`, or bare `Model`. If you pass an explicit `response_model`, it takes precedence.

### Composing with other features

Parent-child chaining composes with all existing features — env resolution, source injection, and incremental loading:

```python
@github.resource(primary_key="id")
def repos(
    user: User,                                          # parent dependency
    source: GitHubSource,                                # source injection
    token: Annotated[str, Secret("GH_TOKEN")],           # env resolution
) -> list[Repo]:
    headers = {"Authorization": f"Bearer {token}"}
    resp = httpx.get(f"{source.base_url}/users/{user.id}/repos", headers=headers)
    yield from resp.json()
```

Each resolution step strips its consumed parameters — no conflicts between features.

### Rules and constraints

| Rule | Detail |
|------|--------|
| **One producer per type** | If two resources return `-> list[User]`, a `ValueError` is raised at registration time |
| **One parent per child** | dlt transformers support a single parent; multiple `BaseModel` params raises `ValueError` |
| **Auto-include parents** | Running `resources=["repos"]` automatically includes `users` |
| **Decorator order doesn't matter** | Dependencies are resolved at build time, not registration time |

### Introspection

```python
# Which resources depend on "users"?
github.get_children("users")       # ["repos"]

# Full dependency tree
github.get_resource_tree()         # {"users": ["repos"], "repos": ["commits"]}
```

## Selective runs

Run specific sources or resources:

```python
# Run only the "github" source
app.run(destination=db, source="github")

# Run only specific resources
app.run(destination=db, resources=["repositories", "issues"])

# Both
app.run(destination=db, source="github", resources=["repositories"])
```

## Incremental loading

Use `Annotated` with `Incremental` for FastAPI-style incremental cursors:

```python
from typing import Annotated
from fastelt.sources import Incremental

@api.resource(primary_key="id", write_disposition="merge")
def events(
    updated_at: Annotated[str, Incremental(initial_value="2024-01-01")],
):
    print(f"Fetching events since {updated_at.last_value}")
    yield {"id": 1, "name": "signup", "updated_at": "2024-06-15T10:00:00"}
```

On subsequent runs, `updated_at.last_value` reflects the last seen cursor value — dlt tracks this automatically.

The `Incremental` marker supports the same parameters as `dlt.sources.incremental`:

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `cursor_path` | `str \| None` | parameter name | JSON path to the cursor field |
| `initial_value` | `Any` | `None` | Starting value for the first run |
| `end_value` | `Any` | `None` | Upper bound for the cursor |
| `row_order` | `str \| None` | `None` | `"asc"` or `"desc"` |
| `allow_external_schedulers` | `bool` | `False` | Allow external scheduler integration |
