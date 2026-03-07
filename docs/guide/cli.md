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

Run a pipeline from an extractor to a loader:

```bash
fastelt run <extractor> <loader> [OPTIONS]
```

**Options:**

| Option | Short | Description |
|--------|-------|-------------|
| `--extractor-config KEY=VALUE` | `-e` | Extractor config (repeatable) |
| `--loader-config KEY=VALUE` | `-l` | Loader config (repeatable) |
| `--app MODULE:ATTR` | | App import path |

**Example:**

```bash
fastelt run csv_users json_file \
    -e path=users.csv \
    -e delimiter=, \
    -l path=output.json
```

### `fastelt list`

List all registered extractors and loaders:

```bash
fastelt list [OPTIONS]
```

**Options:**

| Option | Short | Description |
|--------|-------|-------------|
| `--tag TAG` | `-t` | Filter extractors by tag |
| `--app MODULE:ATTR` | | App import path |

**Example output:**

```
Extractors:
  - repositories [core, github]
  - pull_requests [core, github]
  - legacy_users (deprecated) [users]
Loaders:
  - json_file
  - console
```

### `fastelt describe`

Show detailed information about a component:

```bash
fastelt describe <name> [OPTIONS]
```

**Example output:**

```
Extractor: repositories
Description: Fetch repositories from a GitHub org
Tags: core, github
Record type: Repository
Primary key: name
Config schema:
  min_stars: integer - Minimum star count
```

## Example workflow

Given a project with `fastelt_app.py`:

```python
from fastelt import FastELT, Source
from fastelt.extractors.csv import csv_extractor
from fastelt.loaders.json import json_loader
from pydantic import BaseModel

class User(BaseModel):
    name: str
    email: str
    age: int

app = FastELT()
app.include(csv_extractor(User))
app.include(json_loader(User))
```

```bash
# List available components
fastelt list

# Inspect config for CSV extractor
fastelt describe csv

# Run a pipeline
fastelt run csv json -e path=users.csv -l path=output.json
```
