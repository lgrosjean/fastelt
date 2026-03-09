# Types

Core types used by FastELT.

## Env

```python
from fastelt import Env
```

Lazy reference to an environment variable. Can be used as a value or as a type annotation with `Annotated`.

### Constructor

```python
Env(var: str, default: str = _UNSET)
```

| Parameter | Type | Description |
|-----------|------|-------------|
| `var` | `str` | Environment variable name |
| `default` | `str` | Fallback value if env var is not set |

### Methods

#### `resolve()`

```python
env.resolve() -> str
```

Return the current value of the environment variable.

**Raises:** `EnvironmentError` if the variable is not set and no default was provided.

### Usage

```python
# As a Source field value (resolved at construction time)
github = Source(name="github", token=Env("GH_TOKEN"))

# As an Annotated type hint (resolved at resource call time)
@source.resource()
def repos(token: Annotated[str, Env("GH_TOKEN")]):
    ...
```

---

## Secret

```python
from fastelt import Secret
```

Like `Env` but masks the value in logs and repr. Use for sensitive values (API keys, tokens, passwords).

Subclass of `Env` — same constructor and `resolve()` method.

```python
# repr shows: Secret('GH_TOKEN') instead of the actual value
github = Source(name="github", token=Secret("GH_TOKEN"))
```

---

## SchemaFrozenError

```python
from fastelt import SchemaFrozenError
```

Raised when extra columns are detected on a resource with `frozen=True`.

```python
try:
    app.run()
except SchemaFrozenError as e:
    print(e)
    # Resource 'users' is frozen but received new columns: ['unknown'].
    # Update the response_model to accept them or remove frozen=True.
```

---

## _ResourceMeta

Internal dataclass holding metadata for a registered resource.

| Field | Type | Description |
|-------|------|-------------|
| `func` | `Callable` | The resource generator function |
| `name` | `str` | Resource name |
| `description` | `str \| None` | Human-readable description |
| `tags` | `list[str]` | Categorization tags |
| `deprecated` | `bool` | Whether the resource is deprecated |
| `primary_key` | `str \| list[str] \| None` | Primary key column(s) |
| `write_disposition` | `str` | Write disposition (`"append"`, `"replace"`, `"merge"`) |
| `merge_key` | `str \| list[str] \| None` | Merge key column(s) |
| `table_name` | `str \| None` | Destination table name |
| `selected` | `bool` | Whether the resource runs by default |
| `response_model` | `type[BaseModel] \| None` | Pydantic model for validation |
| `frozen` | `bool` | Whether extra columns raise an error |
