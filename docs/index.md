# FastELT

**A FastAPI-inspired ELT pipeline library.**

FastELT brings FastAPI's developer experience to data pipelines: decorators, type inference, Pydantic v2 validation, and automatic config generation — all in a lightweight Python package.

```python
from fastelt import FastELT
from pydantic import BaseModel, Field
from typing import Iterator

app = FastELT()

class User(BaseModel):
    name: str
    email: str
    age: int

@app.extractor()
def csv_users(path: str = Field(...), delimiter: str = Field(default=",")) -> Iterator[User]:
    import csv
    with open(path) as f:
        for row in csv.DictReader(f, delimiter=delimiter):
            yield User(**row)

@app.loader()
def json_file(records: Records[User], path: str = Field(...)) -> None:
    import json
    with open(path, "w") as f:
        json.dump([r.model_dump() for r in records], f)

app.run(
    extractor="csv_users",
    loader="json_file",
    extractor_config={"path": "users.csv"},
    loader_config={"path": "output.json"},
)
```

## Why FastELT?

| Feature | FastELT | Meltano / Singer | dlt |
|---------|---------|-------------------|-----|
| Define pipelines | Python decorators | YAML / JSON config | Python decorators |
| Config schema | Inferred from signature | Manual definition | Manual / partial |
| Data validation | Pydantic v2 (rust-powered) | None built-in | Schema inference |
| Dependencies | 2 (`pydantic`, `loguru`) | 10+ | 5+ |
| Learning curve | Familiar if you know FastAPI | Tool-specific DSL | dlt-specific API |
| Extensibility | Plain Python functions | Plugin systems | Decorators + config |

[See the full comparison](getting-started/why-fastelt.md){ .md-button }

## Key Concepts

- **Extractors** produce records — generator functions that `yield` Pydantic models
- **Loaders** consume records — functions that receive a `Records[T]` container
- **Sources** group extractors with shared config — like FastAPI's `APIRouter`
- **Built-in plugins** for CSV, JSON, and Parquet — include them with one line

## Quick Links

- [Why FastELT?](getting-started/why-fastelt.md) — how it compares to Meltano, Singer, and dlt
- [Installation](getting-started/installation.md) — get up and running
- [Quickstart](getting-started/quickstart.md) — build your first pipeline in 5 minutes
- [User Guide](guide/extractors.md) — learn the core concepts
- [API Reference](api/app.md) — detailed API documentation
