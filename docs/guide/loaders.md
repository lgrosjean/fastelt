# Loaders

Loaders are functions that consume records. They are the "L" in ELT.

## Basic loader with Records

Declare a `Records[T]` parameter to receive extracted data — like FastAPI's `Request`:

```python
from fastelt import FastELT, Records
from pydantic import BaseModel, Field

app = FastELT()

class User(BaseModel):
    name: str
    email: str

@app.loader()
def json_file(
    records: Records[User],
    path: str = Field(..., description="Output file path"),
    indent: int = Field(default=2),
) -> None:
    import json
    with open(path, "w") as f:
        json.dump([r.model_dump() for r in records], f, indent=indent)
```

## Iterating vs collecting

`Records[T]` supports two consumption patterns:

### Iterate (streaming)

Process records one at a time — memory-efficient for large datasets:

```python
@app.loader()
def print_loader(records: Records[User]) -> None:
    for record in records:
        print(record.model_dump())
```

### Collect (batch)

Materialize all records into a list:

```python
@app.loader()
def csv_file(records: Records[User], path: str = Field(...)) -> None:
    all_records = records.collect()  # list[User]
    # write all at once...
```

!!! warning "Single consumption"
    `Records` can only be consumed once. Iterating or calling `.collect()` a second time raises `RuntimeError`.

## Loader without Records

Not every loader needs the extracted data. Omit the `Records` parameter entirely:

```python
@app.loader()
def notify(message: str = Field(default="Pipeline finished")) -> None:
    print(message)
```

This is useful for side-effect loaders like notifications, logging, or triggering downstream jobs.

```python
app.run(
    extractor="csv_users",
    loader="notify",
    extractor_config={"path": "users.csv"},
    loader_config={"message": "Done processing users!"},
)
```

## Config inference

Just like extractors, loader config is inferred from the function signature. The `Records` parameter is automatically excluded from the config model:

```python
@app.loader()
def my_loader(
    records: Records[User],              # injected by framework (not config)
    path: str = Field(...),               # required config
    overwrite: bool = Field(default=True), # optional config
) -> None:
    ...
```

Generated config model:

```python
class my_loader_Config(BaseModel):
    path: str
    overwrite: bool = True
```
