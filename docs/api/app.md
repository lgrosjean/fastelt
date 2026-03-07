# FastELT

::: fastelt.app.FastELT

The main application class. Create an instance and register extractors/loaders via decorators.

```python
from fastelt import FastELT

app = FastELT()
```

## Methods

### `extractor()`

```python
@app.extractor(
    name: str | None = None,
    *,
    description: str | None = None,
    tags: list[str] | None = None,
    deprecated: bool = False,
    primary_key: str | list[str] | None = None,
) -> Callable[[F], F]
```

Decorator to register an extractor function.

**Parameters:**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `name` | `str \| None` | function name | Registration key |
| `description` | `str \| None` | docstring | Human-readable description |
| `tags` | `list[str] \| None` | `[]` | Categorization tags |
| `deprecated` | `bool` | `False` | Mark as deprecated |
| `primary_key` | `str \| list[str] \| None` | `None` | Identity key(s) |

The decorated function must return `Iterator[T]` (streaming) or `list[T]` (batch), where `T` is a `BaseModel` subclass.

---

### `loader()`

```python
@app.loader(
    name: str | None = None,
) -> Callable[[F], F]
```

Decorator to register a loader function.

**Parameters:**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `name` | `str \| None` | function name | Registration key |

The decorated function may optionally declare a `Records[T]` parameter to receive extracted data.

---

### `include()`

```python
app.include(plugin: PluginGroup) -> None
```

Include extractors and loaders from a `PluginGroup` (returned by built-in plugin factories).

```python
from fastelt.extractors.csv import csv_extractor
app.include(csv_extractor(User))
```

---

### `include_source()`

```python
app.include_source(source: Source) -> None
```

Include all entities from a `Source`.

```python
app.include_source(github)
```

---

### `run()`

```python
app.run(
    extractor: str,
    loader: str,
    extractor_config: dict[str, Any] | None = None,
    loader_config: dict[str, Any] | None = None,
    *,
    validate_records: bool = True,
) -> None
```

Run a pipeline from the named extractor to the named loader.

**Parameters:**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `extractor` | `str` | required | Registered extractor name |
| `loader` | `str` | required | Registered loader name |
| `extractor_config` | `dict` | `None` | Config values for the extractor |
| `loader_config` | `dict` | `None` | Config values for the loader |
| `validate_records` | `bool` | `True` | Validate each record's type |

**Raises:**

- `KeyError` — if extractor or loader name is not registered
- `ValidationError` — if config values fail Pydantic validation

---

### `list_extractors()`

```python
app.list_extractors() -> list[str]
```

Returns the names of all registered extractors.

---

### `list_loaders()`

```python
app.list_loaders() -> list[str]
```

Returns the names of all registered loaders.

---

### `get_extractor()`

```python
app.get_extractor(name: str) -> ExtractorRegistration
```

Returns the `ExtractorRegistration` for the given name.

---

### `get_loader()`

```python
app.get_loader(name: str) -> LoaderRegistration
```

Returns the `LoaderRegistration` for the given name.
