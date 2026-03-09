# CLI

FastELT includes an optional CLI powered by [Typer](https://typer.tiangolo.com/).

## Installation

```bash
pip install fastelt[cli]
```

## App discovery

The CLI auto-discovers your `FastELT` app instance, similar to how `fastapi run` works:

1. Checks conventional files: `fastelt_app.py`, `main.py`, `app.py`
2. Searches package directories (folders with `__init__.py`)
3. Scans loose `.py` files in the current directory

The first file containing a `FastELT` instance is used.

Override with `--app`:

```bash
fastelt --app my_module:app run ...
fastelt --app my_package.pipelines:my_app run ...
```

## Commands

### `fastelt run`

Run the pipeline — extract from sources, load to a destination:

```bash
fastelt run <destination> [source]
```

**Arguments:**

| Argument | Required | Description |
|----------|----------|-------------|
| `destination` | Yes | Destination name (must be registered in the app) |
| `source` | No | Run only this source |

**Options:**

| Option | Short | Description |
|--------|-------|-------------|
| `--resource NAME` | `-r` | Run only these resources (repeatable) |
| `--app MODULE:ATTR` | | App import path |

**Examples:**

```bash
# Run all sources to the registered DuckDB destination
fastelt run duckdb

# Run only the "github" source
fastelt run duckdb github

# Run specific resources
fastelt run duckdb -r repos -r issues
```

### `fastelt list`

List registered sources, destinations, and their resources:

```bash
fastelt list
```

**Example output:**

```
Destinations:
  - duckdb
  - bigquery

Sources:
  github (3 resources)
    - repositories
    - issues
    - stargazers
  local (1 resources)
    - users
```

### `fastelt describe`

Show detailed information about a source, resource, or destination:

```bash
fastelt describe <name>
```

Use `source_name:resource_name` to describe a specific resource:

```bash
fastelt describe github
fastelt describe github:repositories
fastelt describe duckdb
```

**Example output:**

```
Resource: github:repositories
  Description: Fetch repositories from GitHub
  Tags: core, github
  Write disposition: merge
  Primary key: id
```

## Example workflow

Given a project with `fastelt_app.py`:

```python
from fastelt import FastELT, Source
from fastelt.config import Env
from fastelt.destinations import DuckDBDestination

github = Source(name="github", token=Env("GH_TOKEN"))

@github.resource(primary_key="id", write_disposition="merge")
def repos():
    ...

db = DuckDBDestination()
app = FastELT(pipeline_name="my_pipeline")
app.include_destination(db)
app.include_source(github)
```

```bash
# List available sources, destinations, and resources
fastelt list

# Inspect a resource
fastelt describe github:repos

# Run the pipeline
fastelt run duckdb

# Run only a specific source
fastelt run duckdb github
```
