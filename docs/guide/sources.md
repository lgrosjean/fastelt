# Sources

Sources group related extractors with shared configuration — like FastAPI's `APIRouter`.

## Why Sources?

When extracting from the same system (e.g., GitHub, a database, an API), multiple extractors share the same connection details. A `Source` lets you define them once:

```python
from fastelt import FastELT, Source

github = Source(
    base_url="https://api.github.com",
    token="ghp_...",
    org="anthropics",
)
```

## Registering entities

Use `@source.entity()` to register extractors on a source:

```python
from pydantic import BaseModel, Field
from typing import Iterator

class Repository(BaseModel):
    name: str
    stars: int

@github.entity()
def repositories(min_stars: int = Field(default=0)) -> Iterator[Repository]:
    # Access source config via closure — just use `github` from scope
    import requests
    resp = requests.get(
        f"{github.base_url}/orgs/{github.org}/repos",
        headers={"Authorization": f"Bearer {github.token}"},
    )
    for repo in resp.json():
        if repo["stargazers_count"] >= min_stars:
            yield Repository(name=repo["name"], stars=repo["stargazers_count"])
```

Then include the source in your app:

```python
app = FastELT()
app.include_source(github)

app.run(
    extractor="repositories",
    loader="console",
    extractor_config={"min_stars": 1000},
)
```

## Entity metadata

Entities support the same metadata as `@app.extractor()`:

```python
@github.entity(
    description="Fetch repositories from a GitHub org",
    tags=["core", "github"],
    primary_key="name",
)
def repositories(min_stars: int = Field(default=0)) -> Iterator[Repository]:
    ...

@github.entity(
    description="Fetch pull requests for a repository",
    tags=["core", "github"],
    deprecated=True,
    primary_key=["repo", "title"],
)
def pull_requests(repo: str = Field(...)) -> Iterator[PullRequest]:
    ...
```

## Creating Sources

### Programmatic (simple)

For straightforward config, just pass keyword arguments — types are inferred from values:

```python
github = Source(
    base_url="https://api.github.com",
    token="ghp_...",
    org="anthropics",
)
```

### Class-based (advanced)

For complex schemas with validators, descriptions, or defaults:

```python
from pydantic import field_validator

class MlflowSource(Source):
    tracking_uri: str
    token: str = ""
    experiment_prefix: str = ""

    @field_validator("tracking_uri")
    @classmethod
    def must_be_url(cls, v: str) -> str:
        if not v.startswith(("http://", "https://")):
            raise ValueError("tracking_uri must be a URL")
        return v

mlflow = MlflowSource(tracking_uri="http://localhost:5000")
```

## Source injection

Entities access the source config through Python closures — just reference the source variable from the enclosing scope. No special injection needed:

```python
github = Source(base_url="https://api.github.com", token="ghp_...")

@github.entity()
def repos() -> Iterator[Repository]:
    # Just use `github` — it's a regular Python closure
    print(github.base_url)  # "https://api.github.com"
    ...
```

Alternatively, the source can be injected as a function parameter (detected by type annotation or by convention for unannotated params with no default):

```python
# Explicit type annotation
@github.entity()
def repos(source: MlflowSource, limit: int = Field(default=10)) -> Iterator[Repo]:
    print(source.tracking_uri)
    ...

# Convention: unannotated param with no default
@github.entity()
def repos(src, limit: int = Field(default=10)) -> Iterator[Repo]:
    print(src.base_url)
    ...
```

!!! tip "Prefer closures"
    The closure pattern is simpler and more Pythonic. Use parameter injection only when you need the source to be explicit in the function signature.

## Multiple entities

A single source can have multiple entities:

```python
@github.entity()
def repositories(...) -> Iterator[Repository]: ...

@github.entity()
def pull_requests(...) -> Iterator[PullRequest]: ...

@github.entity()
def issues(...) -> Iterator[Issue]: ...

app.include_source(github)  # registers all three
```
