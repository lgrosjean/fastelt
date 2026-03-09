# RESTAPISource

::: fastelt.rest_api.RESTAPISource

Declarative REST API source wrapping dlt's `rest_api_source`.

```python
from fastelt.rest_api import RESTAPISource
```

## Constructor

```python
RESTAPISource(
    name: str,
    base_url: str,
    resources: list[dict],
    headers: dict[str, str] = {},
    auth: dict | str | None = None,
    paginator: dict | str | None = None,
    resource_defaults: dict | None = None,
)
```

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `name` | `str` | required | Source name |
| `base_url` | `str` | required | Base URL for all endpoints |
| `resources` | `list[dict]` | required | Resource configs (dlt `EndpointResource` schema) |
| `headers` | `dict[str, str]` | `{}` | Default headers |
| `auth` | `dict \| str \| None` | `None` | Authentication config |
| `paginator` | `dict \| str \| None` | `None` | Default paginator |
| `resource_defaults` | `dict \| None` | `None` | Defaults for all resources |

Extends [`Source`](source.md) — inherits `Env` resolution for all config values.

## Methods

### `list_resources()`

```python
source.list_resources() -> list[str]
```

Returns resource names from the config.

See the [REST API Source guide](../guide/rest-api.md) for detailed usage examples.

## Installation

```bash
pip install fastelt[rest_api]
```
