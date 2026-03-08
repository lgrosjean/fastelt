# Data Validation

FastELT supports Pydantic `response_model` for in-stream validation, type enforcement, column normalization, and data quality checks — like FastAPI's `response_model`.

## Basic usage

```python
from pydantic import BaseModel, field_validator
from fastelt import Source

class UserModel(BaseModel):
    name: str
    email: str
    age: int

    @field_validator("age")
    @classmethod
    def age_must_be_positive(cls, v):
        if v <= 0:
            raise ValueError(f"age must be > 0, got {v}")
        return v

source = Source(name="local")

@source.resource(response_model=UserModel, write_disposition="replace")
def users():
    yield {"name": "Alice", "email": "alice@example.com", "age": "30"}  # str → int coercion
```

Each yielded dict is validated through the Pydantic model before being passed to dlt. This gives you:

- **Type coercion**: CSV yields strings, but `age: int` coerces `"30"` to `30`
- **Data quality**: Field validators catch bad data at extraction time
- **Column normalization**: `alias_generator` renames columns (e.g. camelCase → snake_case)

## Column normalization with `alias_generator`

```python
from pydantic import BaseModel, ConfigDict
from pydantic.alias_generators import to_snake

class UserModel(BaseModel):
    model_config = ConfigDict(
        alias_generator=to_snake,
        populate_by_name=True,
    )

    first_name: str
    last_name: str
    email: str
```

This normalizes column names before loading — `firstName` becomes `first_name` at the destination.

## Extra columns: warnings and `frozen` mode

When a record contains keys not defined in the `response_model`:

- **Default**: A warning is emitted and the extra keys are dropped
- **`frozen=True`**: A `SchemaFrozenError` is raised, stopping the pipeline

```python
@source.resource(response_model=UserModel, frozen=True)
def users():
    # This will raise SchemaFrozenError because "unknown_field" is not in UserModel
    yield {"name": "Alice", "email": "alice@example.com", "age": 30, "unknown_field": "oops"}
```

Use `frozen=True` for strict schema enforcement — any column drift is caught immediately.

## `SchemaFrozenError`

```python
from fastelt import SchemaFrozenError

try:
    app.run()
except SchemaFrozenError as e:
    print(e)
    # Resource 'users' is frozen but received new columns: ['unknown_field'].
    # Update the response_model to accept them or remove frozen=True.
```

## With `@app.source`

The quick inline decorator also supports `response_model` and `frozen`:

```python
@app.source("users", primary_key="id", response_model=UserModel, frozen=True)
def users():
    yield {"id": 1, "name": "Alice", "email": "alice@example.com", "age": 30}
```
