# FastELT

**A FastAPI-inspired ELT pipeline library.**

FastELT brings FastAPI's developer experience to data pipelines: decorators, type inference, Pydantic v2 validation, and automatic config generation — all in a lightweight Python package.

## Installation

```bash
pip install fastelt
```

With optional extras:

```bash
pip install fastelt[cli]       # CLI support (Typer)
pip install fastelt[parquet]   # Parquet support (PyArrow)
```

## Quickstart

```python
from fastelt import FastELT, Records
from pydantic import BaseModel, Field
from typing import Iterator

app = FastELT()

class User(BaseModel):
    name: str
    email: str
    age: int

@app.extractor()
def csv_users(
    path: str = Field(..., description="Path to CSV file"),
    delimiter: str = Field(default=","),
) -> Iterator[User]:
    import csv
    with open(path) as f:
        for row in csv.DictReader(f, delimiter=delimiter):
            yield User(**row)

@app.loader()
def json_file(
    records: Records[User],
    path: str = Field(..., description="Output path"),
) -> None:
    import json
    with open(path, "w") as f:
        json.dump([r.model_dump() for r in records], f, indent=2)

app.run(
    extractor="csv_users",
    loader="json_file",
    extractor_config={"path": "users.csv"},
    loader_config={"path": "output.json"},
)
```

## Key Concepts

### Config inference

Config is inferred from function signatures — no separate config class needed. Use `Field(...)` for required params and `Field(default=...)` for optional ones, just like FastAPI query parameters.

### Records

`Records[T]` is an injectable container, like FastAPI's `Request`. Declare it in your loader only if you need the extracted data:

```python
@app.loader()
def my_loader(records: Records[User], path: str = Field(...)) -> None:
    for record in records:
        ...
```

### Streaming vs Batch

Use `Iterator[T]` for streaming (memory-efficient) or `list[T]` for batch:

```python
# Streaming
@app.extractor()
def stream(path: str = Field(...)) -> Iterator[User]:
    yield User(...)

# Batch
@app.extractor()
def batch(path: str = Field(...)) -> list[User]:
    return [User(...)]
```

### Sources

Group related extractors with shared config — like FastAPI's `APIRouter`:

```python
from fastelt import Source

github = Source(
    base_url="https://api.github.com",
    token="ghp_...",
    org="anthropics",
)

@github.entity(
    description="Fetch repositories",
    tags=["core"],
    primary_key="name",
)
def repositories(min_stars: int = Field(default=0)) -> Iterator[Repository]:
    ...

app = FastELT()
app.include_source(github)
```

### Built-in Plugins

Skip writing extractors/loaders for common formats:

```python
from fastelt.extractors.csv import csv_extractor
from fastelt.loaders.parquet import parquet_loader

app = FastELT()
app.include(csv_extractor(User))
app.include(parquet_loader(User))

app.run(extractor="csv", loader="parquet", ...)
```

Available: `csv`, `json`, `parquet` (extractors and loaders).

### CLI

```bash
pip install fastelt[cli]

fastelt run csv_users json_file -e path=users.csv -l path=output.json
fastelt list
fastelt describe csv_users
```

The CLI auto-discovers your `FastELT` app instance, like `fastapi run`.

## Why FastELT?

| Feature | FastELT | Meltano / Singer | dlt |
|---------|---------|-------------------|-----|
| Define pipelines | Python decorators | YAML / JSON config | Python decorators |
| Config schema | Inferred from signature | Manual definition | Manual / partial |
| Data validation | Pydantic v2 (rust-powered) | None built-in | Schema inference |
| Dependencies | 2 (`pydantic`, `loguru`) | 10+ | 5+ |
| Learning curve | Familiar if you know FastAPI | Tool-specific DSL | dlt-specific API |

## Documentation

Full docs: [fastelt.dev](https://fastelt.dev)

## Requirements

- Python >= 3.12
- Pydantic >= 2.0

## License

MIT
