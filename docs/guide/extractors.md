# Extractors

Extractors are functions that produce records. They are the "E" in ELT.

## Basic extractor

Decorate a generator function with `@app.extractor()`:

```python
from fastelt import FastELT
from pydantic import BaseModel, Field
from typing import Iterator

app = FastELT()

class User(BaseModel):
    name: str
    email: str

@app.extractor()
def api_users(
    endpoint: str = Field(..., description="API endpoint URL"),
    limit: int = Field(default=100),
) -> Iterator[User]:
    import requests
    resp = requests.get(endpoint, params={"limit": limit})
    for item in resp.json():
        yield User(**item)
```

The function name becomes the extractor name. Override it with an explicit name:

```python
@app.extractor("my_custom_name")
def extract_users(...) -> Iterator[User]:
    ...
```

## Config inference

FastELT infers config from your function signature — no need for a separate config class:

```python
@app.extractor()
def my_extractor(
    path: str = Field(..., description="Input file"),       # required
    delimiter: str = Field(default=","),                     # optional, default ","
    skip_header: bool = Field(default=False),                # optional, default False
) -> Iterator[MyModel]:
    ...
```

This generates a Pydantic model equivalent to:

```python
class my_extractor_Config(BaseModel):
    path: str = Field(..., description="Input file")
    delimiter: str = ","
    skip_header: bool = False
```

## Streaming vs Batch

FastELT supports two extraction patterns:

### Streaming (yield)

Memory-efficient — records flow one at a time from extractor to loader:

```python
@app.extractor()
def stream_users(path: str = Field(...)) -> Iterator[User]:
    with open(path) as f:
        for line in f:
            yield User.model_validate_json(line)
```

### Batch (return)

All records at once — useful when the source doesn't support streaming:

```python
@app.extractor()
def batch_users(path: str = Field(...)) -> list[User]:
    import json
    with open(path) as f:
        return [User(**item) for item in json.load(f)]
```

Both patterns work identically from the loader's perspective.

## Entity metadata

Extractors accept metadata parameters for documentation, discovery, and loader hints:

```python
@app.extractor(
    description="Fetch user records from the REST API",
    tags=["core", "users"],
    deprecated=True,
    primary_key="email",
)
def api_users(endpoint: str = Field(...)) -> Iterator[User]:
    ...
```

| Parameter | Type | Description |
|-----------|------|-------------|
| `name` | `str` | Override the function name as the registration key |
| `description` | `str` | Human-readable description (defaults to docstring) |
| `tags` | `list[str]` | Categorization tags, filterable via CLI |
| `deprecated` | `bool` | Emits a warning at runtime, shown in CLI |
| `primary_key` | `str \| list[str]` | Identity key(s) for dedup/upserts |

### Deprecated extractors

When `deprecated=True`, a warning is logged every time the extractor runs:

```
WARNING  | Extractor 'api_users' is deprecated
```

### Primary key

The `primary_key` is metadata that loaders can use to perform upserts instead of appends:

```python
# Single key
@app.extractor(primary_key="id")
def users(...) -> Iterator[User]: ...

# Composite key
@app.extractor(primary_key=["repo", "pr_number"])
def pull_requests(...) -> Iterator[PullRequest]: ...
```

## Record validation

By default, FastELT validates each record against the declared return type. Disable it for performance:

```python
app.run(
    extractor="api_users",
    loader="json_file",
    validate_records=False,  # skip isinstance checks
)
```
