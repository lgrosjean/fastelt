# Records

::: fastelt.types.Records

Injectable container for extracted records — like FastAPI's `Request` object.

```python
from fastelt import Records
```

## Usage

Declare `Records[T]` in your loader's signature to receive extracted data:

```python
@app.loader()
def my_loader(records: Records[User], path: str = Field(...)) -> None:
    for record in records:
        print(record)
```

## Constructor

```python
Records(data: Iterator[T])
```

!!! note
    You typically don't construct `Records` yourself — the framework creates it during pipeline execution.

## Methods

### `__iter__()`

Iterate over records one at a time (streaming):

```python
for record in records:
    process(record)
```

**Raises:** `RuntimeError` if records have already been consumed.

### `collect()`

```python
records.collect() -> list[T]
```

Consume all records into a list:

```python
all_users = records.collect()
print(f"Got {len(all_users)} users")
```

**Raises:** `RuntimeError` if records have already been consumed.

## Single consumption

`Records` can only be consumed once — either by iterating or by calling `.collect()`. This is by design: extractors may be streaming generators, so the data cannot be replayed.

```python
@app.loader()
def bad_loader(records: Records[User]) -> None:
    first_pass = list(records)   # OK
    second_pass = list(records)  # RuntimeError!
```

If you need multiple passes, call `.collect()` once and work with the list.
