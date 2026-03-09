# REST API Source

For standard REST APIs, `RESTAPISource` provides a declarative, zero-code extraction experience. Define endpoints as config — dlt handles pagination, authentication, and incremental loading.

## Basic usage

```python
from fastelt import Env, FastELT
from fastelt.sources.rest_api import RESTAPISource, BearerTokenAuth

github = RESTAPISource(
    name="github",
    base_url="https://api.github.com",
    auth=BearerTokenAuth(token=Env("GH_TOKEN")),
    paginator="header_link",
    resources=[
        {
            "name": "repos",
            "endpoint": {
                "path": "/orgs/{org}/repos",
                "params": {"org": "anthropics", "per_page": 100},
            },
            "primary_key": "id",
            "write_disposition": "merge",
        },
    ],
)

app = FastELT(pipeline_name="github_pipeline", destination="duckdb")
app.include_source(github)
app.run()
```

## Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| `name` | `str` | Source name |
| `base_url` | `str` | Base URL for all endpoints |
| `resources` | `list[dict]` | Resource configs (see below) |
| `headers` | `dict[str, str]` | Default headers for all requests |
| `auth` | `AuthConfigBase \| dict \| str \| None` | Authentication config (auth class or dict) |
| `paginator` | `dict \| str \| None` | Default paginator |
| `resource_defaults` | `dict \| None` | Defaults applied to all resources |

## Authentication

Pass dlt auth class instances directly — use `Env()` or `Secret()` for sensitive values:

```python
from fastelt.sources.rest_api import BearerTokenAuth, APIKeyAuth, HttpBasicAuth

# Bearer token
auth=BearerTokenAuth(token=Env("GH_TOKEN"))

# API key
auth=APIKeyAuth(name="X-API-Key", api_key=Env("API_KEY"))

# HTTP Basic
auth=HttpBasicAuth(username="user", password=Env("PASSWORD"))
```

Dict configs are also supported for backward compatibility:

```python
auth={"type": "bearer", "token": Env("GH_TOKEN")}
```

## Pagination

String shorthand for common paginators:

| Value | Description |
|-------|-------------|
| `"header_link"` | GitHub-style `Link` header |
| `"json_link"` | JSON response with next link |
| `"offset"` | Offset/limit |
| `"page_number"` | Page number |
| `"cursor"` | Cursor-based |
| `"auto"` | Auto-detect (default) |

Or pass a dict with full paginator config (see dlt docs).

## Resource config

Each resource in the `resources` list supports:

```python
{
    "name": "repos",
    "endpoint": {
        "path": "/orgs/{org}/repos",
        "params": {"org": "anthropics", "per_page": 100},
        "incremental": {
            "start_param": "since",
            "cursor_path": "updated_at",
            "initial_value": "2024-01-01T00:00:00Z",
        },
    },
    "primary_key": "id",
    "write_disposition": "merge",
}
```

## Resource defaults

Apply defaults to all resources:

```python
github = RESTAPISource(
    name="github",
    base_url="https://api.github.com",
    auth=BearerTokenAuth(token=Env("GH_TOKEN")),
    resource_defaults={
        "primary_key": "id",
        "write_disposition": "merge",
    },
    resources=[
        {"name": "repos", "endpoint": {"path": "/orgs/anthropics/repos"}},
        {"name": "issues", "endpoint": {"path": "/repos/anthropics/sdk/issues"}},
    ],
)
```

## When to use RESTAPISource vs Source

| Use case | Recommendation |
|----------|---------------|
| Standard REST API with pagination | `RESTAPISource` — zero code needed |
| Custom extraction logic (filtering, transformations) | `Source` + `@source.resource()` |
| Need to call multiple APIs per resource | `Source` + `@source.resource()` |
| Complex authentication flows | `Source` + `@source.resource()` |

## Installation

```bash
pip install fastelt[rest_api]
```
