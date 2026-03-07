# Built-in Plugins

FastELT ships with extractors and loaders for common file formats. They are factory functions that return a `PluginGroup`, parameterized by your record model.

## Usage pattern

```python
from fastelt import FastELT
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

app.run(
    extractor="csv",
    loader="json",
    extractor_config={"path": "users.csv"},
    loader_config={"path": "output.json"},
)
```

## Extractors

### CSV

```python
from fastelt.extractors.csv import csv_extractor

app.include(csv_extractor(User))
```

| Config | Type | Default | Description |
|--------|------|---------|-------------|
| `path` | `str` | required | Path to CSV file |
| `delimiter` | `str` | `","` | CSV delimiter |

### JSON

```python
from fastelt.extractors.json import json_extractor

app.include(json_extractor(User))
```

| Config | Type | Default | Description |
|--------|------|---------|-------------|
| `path` | `str` | required | Path to JSON file |

Handles both JSON arrays (`[{...}, {...}]`) and single objects (`{...}`).

### Parquet

```python
from fastelt.extractors.parquet import parquet_extractor

app.include(parquet_extractor(User))
```

| Config | Type | Default | Description |
|--------|------|---------|-------------|
| `path` | `str` | required | Path to Parquet file |

!!! note "Extra dependency"
    Requires `pip install fastelt[parquet]` (installs PyArrow).

## Loaders

### CSV

```python
from fastelt.loaders.csv import csv_loader

app.include(csv_loader(User))
```

| Config | Type | Default | Description |
|--------|------|---------|-------------|
| `path` | `str` | required | Output CSV file path |
| `delimiter` | `str` | `","` | CSV delimiter |

### JSON

```python
from fastelt.loaders.json import json_loader

app.include(json_loader(User))
```

| Config | Type | Default | Description |
|--------|------|---------|-------------|
| `path` | `str` | required | Output JSON file path |

### Parquet

```python
from fastelt.loaders.parquet import parquet_loader

app.include(parquet_loader(User))
```

| Config | Type | Default | Description |
|--------|------|---------|-------------|
| `path` | `str` | required | Output Parquet file path |

!!! note "Extra dependency"
    Requires `pip install fastelt[parquet]` (installs PyArrow).

## Combining built-in and custom

You can mix built-in plugins with custom extractors/loaders freely:

```python
app = FastELT()

# Built-in extractor
app.include(csv_extractor(User))

# Custom loader
@app.loader()
def upload_to_s3(records: Records[User], bucket: str = Field(...)) -> None:
    ...

app.run(extractor="csv", loader="upload_to_s3", ...)
```
