# RESTAPISource

::: fastelt.sources.rest_api.RESTAPISource

Declarative REST API source wrapping dlt's `rest_api_source`.

```python
from fastelt.sources.rest_api import RESTAPISource, BearerTokenAuth, APIKeyAuth
```

## Constructor

```python
RESTAPISource(
    name: str,
    base_url: str,
    resources: list[dict],
    headers: dict[str, str] = {},
    auth: AuthConfigBase | dict | str | None = None,
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
| `auth` | `AuthConfigBase \| dict \| str \| None` | `None` | Authentication (auth class instance or dict config) |
| `paginator` | `dict \| str \| None` | `None` | Default paginator |
| `resource_defaults` | `dict \| None` | `None` | Defaults for all resources |

Extends [`Source`](source.md) — inherits `Env` resolution for all config values.

## Authentication classes

Re-exported from `dlt.sources.helpers.rest_client.auth` for convenience:

| Class | Parameters | Description |
|-------|-----------|-------------|
| `BearerTokenAuth` | `token` | Bearer token (`Authorization: Bearer <token>`) |
| `APIKeyAuth` | `name`, `api_key`, `location` | API key in header, query, or cookie |
| `HttpBasicAuth` | `username`, `password` | HTTP Basic authentication |
| `OAuth2ClientCredentials` | `access_token_url`, `client_id`, `client_secret`, ... | OAuth 2.0 client credentials flow |
| `AuthConfigBase` | — | Base class for custom auth |

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
