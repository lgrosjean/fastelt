# Config (Env, Secret)

::: fastelt.config

Configuration primitives for environment variable resolution.

```python
from fastelt.config import Env, Secret
```

## Env

Lazy reference to an environment variable. Can be used as a value or as a type annotation with `Annotated`.

### Constructor

```python
Env(var: str, default: str | None = None)
```

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `var` | `str` | required | Environment variable name |
| `default` | `str \| None` | `None` | Fallback value if env var is not set |

### Methods

#### `resolve()`

```python
env.resolve() -> str
```

Return the current value of the environment variable.

**Raises:** `EnvironmentError` if the variable is not set and no default was provided.

### Usage

```python
from fastelt.config import Env

# As a Source field value (resolved at construction time)
github = Source(name="github", token=Env("GH_TOKEN"))

# With a default fallback
github = Source(name="github", token=Env("GH_TOKEN", default="fallback"))

# As a Destination field value
from fastelt.destinations import BigQueryDestination
bq = BigQueryDestination(project_id=Env("GCP_PROJECT"))

# As an Annotated type hint (resolved at resource call time)
from typing import Annotated

@source.resource()
def repos(token: Annotated[str, Env("GH_TOKEN")]):
    ...
```

---

## Secret

Like `Env` but masks the value in logs and repr. Use for sensitive values (API keys, tokens, passwords).

Subclass of `Env` — same constructor and `resolve()` method.

```python
from fastelt.config import Secret

# repr shows: Secret('GH_TOKEN') instead of the actual value
github = Source(name="github", token=Secret("GH_TOKEN"))

# As an Annotated type hint
@source.resource()
def repos(token: Annotated[str, Secret("GH_TOKEN")]):
    ...
```
