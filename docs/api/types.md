# Types

Internal types used by FastELT. You typically don't interact with these directly, but they are useful for building custom plugins or advanced integrations.

## ExtractorRegistration

```python
from fastelt.types import ExtractorRegistration
```

Dataclass holding the registration data for an extractor.

| Field | Type | Description |
|-------|------|-------------|
| `name` | `str` | Registration key |
| `func` | `Callable` | The extractor function |
| `config_model` | `type[BaseModel]` | Auto-generated Pydantic config model |
| `record_type` | `type[BaseModel]` | The record model (`T` from `Iterator[T]`) |
| `description` | `str \| None` | Human-readable description |
| `tags` | `list[str]` | Categorization tags |
| `deprecated` | `bool` | Whether the extractor is deprecated |
| `primary_key` | `str \| list[str] \| None` | Identity key(s) |

## LoaderRegistration

```python
from fastelt.types import LoaderRegistration
```

Dataclass holding the registration data for a loader.

| Field | Type | Description |
|-------|------|-------------|
| `name` | `str` | Registration key |
| `func` | `Callable` | The loader function |
| `config_model` | `type[BaseModel]` | Auto-generated Pydantic config model |
| `record_type` | `type[BaseModel] \| None` | Expected record type (from `Records[T]`), or `None` |
| `records_param` | `str \| None` | Name of the `Records` parameter, or `None` |

## PluginGroup

```python
from fastelt.types import PluginGroup
```

Container for a set of extractors and loaders, used by built-in plugins and `Source._build_plugin_group()`.

| Field | Type | Description |
|-------|------|-------------|
| `extractors` | `dict[str, ExtractorRegistration]` | Extractor registrations |
| `loaders` | `dict[str, LoaderRegistration]` | Loader registrations |

```python
from fastelt.extractors.csv import csv_extractor

plugin: PluginGroup = csv_extractor(User)
app.include(plugin)
```
