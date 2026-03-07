# Source

::: fastelt.types.Source

Shared configuration for a group of related extractors — like FastAPI's `APIRouter`.

```python
from fastelt import Source
```

## Creating a Source

### Programmatic

Types are inferred from the values you pass:

```python
github = Source(
    base_url="https://api.github.com",
    token="ghp_...",
    org="anthropics",
)
```

### Class-based

For complex schemas with validators, descriptions, or type constraints:

```python
class MlflowSource(Source):
    tracking_uri: str
    token: str = ""

mlflow = MlflowSource(tracking_uri="http://localhost:5000")
```

Since `Source` extends `BaseModel`, you get full Pydantic v2 features: validators, serialization, JSON schema, etc.

## Methods

### `entity()`

```python
@source.entity(
    name: str | None = None,
    *,
    description: str | None = None,
    tags: list[str] | None = None,
    deprecated: bool = False,
    primary_key: str | list[str] | None = None,
) -> Callable
```

Decorator to register an entity extractor on this source.

**Parameters:**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `name` | `str \| None` | function name | Registration key |
| `description` | `str \| None` | docstring | Human-readable description |
| `tags` | `list[str] \| None` | `[]` | Categorization tags |
| `deprecated` | `bool` | `False` | Mark as deprecated |
| `primary_key` | `str \| list[str] \| None` | `None` | Identity key(s) |

```python
@github.entity(
    description="Fetch repositories",
    tags=["core"],
    primary_key="name",
)
def repositories(min_stars: int = Field(default=0)) -> Iterator[Repository]:
    ...
```

## Including in an app

```python
app = FastELT()
app.include_source(github)  # registers all entities as extractors
```

## Accessing source config

Entities access the source via Python closure:

```python
github = Source(base_url="https://api.github.com", token="ghp_...")

@github.entity()
def repos() -> Iterator[Repo]:
    # `github` is captured from the enclosing scope
    requests.get(f"{github.base_url}/repos", ...)
```
