# Source

::: fastelt.types.Source

Shared configuration for a group of related resources — like FastAPI's `APIRouter`.

```python
from fastelt import Source
```

## Creating a Source

### Programmatic

Types are inferred from the values you pass:

```python
github = Source(
    name="github",
    base_url="https://api.github.com",
    token=Env("GH_TOKEN"),
    org="anthropics",
)
```

### Class-based

For complex schemas with validators or type constraints:

```python
class GitHubSource(Source):
    base_url: str = "https://api.github.com"
    token: str
    org: str

github = GitHubSource(token="ghp_...", org="anthropics")
```

Since `Source` extends `BaseModel`, you get full Pydantic v2 features.

## Methods

### `resource()`

```python
@source.resource(
    name: str | None = None,
    *,
    description: str | None = None,
    tags: list[str] | None = None,
    deprecated: bool = False,
    primary_key: str | list[str] | None = None,
    write_disposition: str = "append",
    merge_key: str | list[str] | None = None,
    table_name: str | None = None,
    selected: bool = True,
    response_model: type[BaseModel] | None = None,
    frozen: bool = False,
) -> Callable
```

Decorator to register a resource on this source.

**Parameters:**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `name` | `str \| None` | function name | Resource name |
| `description` | `str \| None` | docstring | Human-readable description |
| `tags` | `list[str] \| None` | `[]` | Categorization tags |
| `deprecated` | `bool` | `False` | Emit warning at runtime |
| `primary_key` | `str \| list[str] \| None` | `None` | Primary key column(s) |
| `write_disposition` | `str` | `"append"` | `"append"`, `"replace"`, or `"merge"` |
| `merge_key` | `str \| list[str] \| None` | `None` | Merge key column(s) |
| `table_name` | `str \| None` | resource name | Destination table name |
| `selected` | `bool` | `True` | Whether this runs by default |
| `response_model` | `type[BaseModel] \| None` | `None` | Pydantic model for validation |
| `frozen` | `bool` | `False` | Reject extra columns (requires `response_model`) |

```python
@github.resource(
    primary_key="id",
    write_disposition="merge",
    description="Fetch repositories",
    response_model=RepoModel,
)
def repositories():
    ...
```

---

### `list_resources()`

```python
source.list_resources() -> list[str]
```

Returns the names of all registered resources.

---

### `get_resource_meta()`

```python
source.get_resource_meta(name: str) -> _ResourceMeta
```

Returns the internal metadata for a resource.

## Subclasses

FastELT provides specialized Source subclasses:

- [`RESTAPISource`](rest-api-source.md) — declarative REST API extraction
- [`LocalFileSystemSource`](filesystem-source.md) — load files from local disk
- [`GCSFileSystemSource`](filesystem-source.md) — load files from Google Cloud Storage
