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

Run the pipeline — extract from sources, load to destination:

```bash
fastelt run [OPTIONS]
```

**Options:**

| Option | Short | Description |
|--------|-------|-------------|
| `--destination DEST` | `-d` | dlt destination (duckdb, postgres, bigquery, ...) |
| `--dataset NAME` | | Dataset/schema name at the destination |
| `--source NAME` | `-s` | Run only this source |
| `--resource NAME` | `-r` | Run only these resources (repeatable) |
| `--app MODULE:ATTR` | | App import path |

**Examples:**

```bash
# Run all sources to DuckDB
fastelt run -d duckdb

# Run only the "github" source
fastelt run -d duckdb -s github

# Run specific resources
fastelt run -d duckdb -r repos -r issues
```

### `fastelt list`

List registered sources and their resources:

```bash
fastelt list [OPTIONS]
```

**Example output:**

```
Source: github (3 resources)
  - repositories
  - issues
  - stargazers
Source: local (1 resources)
  - users
```

### `fastelt describe`

Show detailed information about a source or resource:

```bash
fastelt describe <name> [OPTIONS]
```

Use `source_name:resource_name` to describe a specific resource:

```bash
fastelt describe github
fastelt describe github:repositories
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
from fastelt import FastELT, Source, Env

github = Source(name="github", token=Env("GH_TOKEN"))

@github.resource(primary_key="id", write_disposition="merge")
def repos():
    ...

app = FastELT(pipeline_name="my_pipeline", destination="duckdb")
app.include_source(github)
```

```bash
# List available sources and resources
fastelt list

# Inspect a resource
fastelt describe github:repos

# Run the pipeline
fastelt run

# Run to a different destination
fastelt run -d postgres
```
