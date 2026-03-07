# Quickstart

This guide walks you through building your first FastELT pipeline.

## 1. Define your data model

Like FastAPI uses Pydantic models for request/response schemas, FastELT uses them for records:

```python
from pydantic import BaseModel

class User(BaseModel):
    name: str
    email: str
    age: int
```

## 2. Create an app and register an extractor

```python
from fastelt import FastELT
from pydantic import Field
from typing import Iterator

app = FastELT()

@app.extractor()
def csv_users(
    path: str = Field(..., description="Path to CSV file"),
    delimiter: str = Field(default=","),
) -> Iterator[User]:
    import csv
    with open(path) as f:
        for row in csv.DictReader(f, delimiter=delimiter):
            yield User(**row)
```

Config parameters are inferred from the function signature — just like FastAPI infers query parameters. Use `Field(...)` for required params and `Field(default=...)` for optional ones.

## 3. Register a loader

```python
from fastelt import Records

@app.loader()
def json_file(
    records: Records[User],
    path: str = Field(..., description="Output path"),
) -> None:
    import json
    with open(path, "w") as f:
        json.dump([r.model_dump() for r in records], f, indent=2)
```

The `Records[User]` parameter is optional — declare it only if your loader needs the extracted data. Think of it like FastAPI's `Request` object.

## 4. Run the pipeline

```python
app.run(
    extractor="csv_users",
    loader="json_file",
    extractor_config={"path": "users.csv"},
    loader_config={"path": "output.json"},
)
```

## 5. Or use the CLI

Create a file called `fastelt_app.py` (or any `.py` file — FastELT auto-discovers it):

```bash
fastelt run csv_users json_file \
    -e path=users.csv \
    -l path=output.json
```

## Using built-in plugins

Skip writing extractors/loaders for common formats:

```python
from fastelt.extractors.csv import csv_extractor
from fastelt.loaders.json import json_loader

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

## Next steps

- [Extractors](../guide/extractors.md) — streaming vs batch, metadata, decorator options
- [Loaders](../guide/loaders.md) — Records injection, loaders without records
- [Sources](../guide/sources.md) — group extractors with shared config (like APIRouter)
