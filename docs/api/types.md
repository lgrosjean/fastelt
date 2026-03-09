# Types

Core types used by FastELT.

## SchemaFrozenError

```python
from fastelt import SchemaFrozenError
```

Raised when extra columns are detected on a resource with `frozen=True`.

```python
try:
    app.run(destination=db)
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
