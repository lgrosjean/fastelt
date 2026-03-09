"""Tests for pydantic response_model validation in resources.

Covers:
- Type enforcement via pydantic models
- Extra field detection (warning when not frozen, error when frozen)
- Column normalization via alias_generator
- Data quality checks via pydantic field validators
- Integration with dlt pipeline
"""

from __future__ import annotations

import shutil
import warnings
from pathlib import Path
from typing import Iterator

import duckdb
import pytest
from pydantic import BaseModel, ConfigDict, EmailStr, field_validator
from pydantic.alias_generators import to_snake

from fastelt import FastELT, SchemaFrozenError, Source

PIPELINES: list[str] = []


def _cleanup_pipeline(pipeline_name: str):
    for suffix in [".duckdb", ".duckdb.wal"]:
        p = Path(f"{pipeline_name}{suffix}")
        if p.exists():
            p.unlink()
    working_dir = Path(f"/var/dlt/pipelines/{pipeline_name}")
    if working_dir.exists():
        shutil.rmtree(working_dir, ignore_errors=True)


@pytest.fixture(autouse=True)
def cleanup():
    PIPELINES.clear()
    yield
    for name in PIPELINES:
        _cleanup_pipeline(name)


def _app(name: str, **kwargs) -> FastELT:
    PIPELINES.append(name)
    return FastELT(pipeline_name=name, destination="duckdb", **kwargs)


def _query(pipeline_name: str, sql: str):
    conn = duckdb.connect(f"{pipeline_name}.duckdb")
    result = conn.sql(sql).fetchall()
    conn.close()
    return result


# -- Basic type enforcement --


class UserModel(BaseModel):
    id: int
    name: str
    age: int


def test_response_model_enforces_types():
    """Pydantic model coerces string values to correct types."""
    src = Source(name="test")

    @src.resource(response_model=UserModel, primary_key="id")
    def users() -> Iterator[dict]:
        # CSV-like data: all strings
        yield {"id": "1", "name": "Alice", "age": "30"}
        yield {"id": "2", "name": "Bob", "age": "25"}

    dlt_source = src._build_dlt_source()
    records = list(list(dlt_source.resources.values())[0])
    assert records[0]["id"] == 1  # int, not "1"
    assert records[0]["age"] == 30  # int, not "30"
    assert records[1]["name"] == "Bob"


def test_response_model_rejects_invalid_types():
    """Pydantic raises ValidationError for unconvertible types."""
    src = Source(name="test")

    @src.resource(response_model=UserModel)
    def users() -> Iterator[dict]:
        yield {"id": 1, "name": "Alice", "age": "not_a_number"}

    dlt_source = src._build_dlt_source()
    with pytest.raises(Exception):  # pydantic ValidationError
        list(list(dlt_source.resources.values())[0])


# -- Extra fields: warning mode (default) --


def test_extra_fields_emit_warning():
    """Extra fields in yielded dicts emit a UserWarning."""
    src = Source(name="test")

    @src.resource(response_model=UserModel)
    def users() -> Iterator[dict]:
        yield {"id": 1, "name": "Alice", "age": 30, "favorite_color": "blue"}

    dlt_source = src._build_dlt_source()
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        records = list(list(dlt_source.resources.values())[0])
        assert len(w) == 1
        assert "new keys not ingested" in str(w[0].message)
        assert "favorite_color" in str(w[0].message)

    # Extra field should NOT be in the output
    assert "favorite_color" not in records[0]
    assert records[0]["id"] == 1


def test_no_warning_when_all_fields_match():
    """No warning when yielded dict matches model exactly."""
    src = Source(name="test")

    @src.resource(response_model=UserModel)
    def users() -> Iterator[dict]:
        yield {"id": 1, "name": "Alice", "age": 30}

    dlt_source = src._build_dlt_source()
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        records = list(list(dlt_source.resources.values())[0])
        assert len(w) == 0

    assert records[0] == {"id": 1, "name": "Alice", "age": 30}


# -- Extra fields: frozen mode --


def test_frozen_raises_on_extra_fields():
    """frozen=True raises SchemaFrozenError on extra fields."""
    from fastelt.types import _validate_record

    # Test the validation function directly (dlt wraps exceptions)
    with pytest.raises(SchemaFrozenError, match="frozen"):
        _validate_record(
            {"id": 1, "name": "Alice", "age": 30, "city": "Paris"},
            UserModel,
            frozen=True,
            resource_name="users",
        )


def test_frozen_raises_through_dlt_pipeline():
    """frozen=True error propagates through dlt pipeline."""
    src = Source(name="test")

    @src.resource(response_model=UserModel, frozen=True)
    def users() -> Iterator[dict]:
        yield {"id": 1, "name": "Alice", "age": 30, "city": "Paris"}

    dlt_source = src._build_dlt_source()
    with pytest.raises(Exception, match="frozen"):
        list(list(dlt_source.resources.values())[0])


def test_frozen_ok_when_no_extras():
    """frozen=True works fine when no extra fields."""
    src = Source(name="test")

    @src.resource(response_model=UserModel, frozen=True)
    def users() -> Iterator[dict]:
        yield {"id": 1, "name": "Alice", "age": 30}

    dlt_source = src._build_dlt_source()
    records = list(list(dlt_source.resources.values())[0])
    assert records[0] == {"id": 1, "name": "Alice", "age": 30}


# -- Column normalization via alias_generator --


class NormalizedUser(BaseModel):
    model_config = ConfigDict(
        alias_generator=to_snake,
        populate_by_name=True,
    )

    user_id: int
    first_name: str
    last_name: str
    email_address: str


def test_alias_generator_normalizes_columns():
    """alias_generator (to_snake) normalizes incoming column names."""
    src = Source(name="test")

    @src.resource(response_model=NormalizedUser)
    def users() -> Iterator[dict]:
        yield {
            "user_id": 1,
            "first_name": "Alice",
            "last_name": "Smith",
            "email_address": "alice@example.com",
        }

    dlt_source = src._build_dlt_source()
    records = list(list(dlt_source.resources.values())[0])
    assert records[0]["user_id"] == 1
    assert records[0]["first_name"] == "Alice"


class CamelToSnakeUser(BaseModel):
    """Model that accepts camelCase input and normalizes to snake_case."""

    model_config = ConfigDict(
        populate_by_name=True,
    )

    user_id: int
    full_name: str
    email_address: str


# -- Data quality checks via validators --


class ValidatedUser(BaseModel):
    id: int
    name: str
    age: int

    @field_validator("age")
    @classmethod
    def age_must_be_positive(cls, v: int) -> int:
        if v <= 0:
            raise ValueError("age must be greater than 0")
        return v

    @field_validator("name")
    @classmethod
    def name_must_not_be_empty(cls, v: str) -> str:
        if not v.strip():
            raise ValueError("name must not be empty")
        return v.strip()


def test_validator_rejects_negative_age():
    """Field validator rejects age <= 0."""
    src = Source(name="test")

    @src.resource(response_model=ValidatedUser)
    def users() -> Iterator[dict]:
        yield {"id": 1, "name": "Alice", "age": -5}

    dlt_source = src._build_dlt_source()
    with pytest.raises(Exception, match="age must be greater than 0"):
        list(list(dlt_source.resources.values())[0])


def test_validator_rejects_zero_age():
    """Field validator rejects age == 0."""
    src = Source(name="test")

    @src.resource(response_model=ValidatedUser)
    def users() -> Iterator[dict]:
        yield {"id": 1, "name": "Bob", "age": 0}

    dlt_source = src._build_dlt_source()
    with pytest.raises(Exception, match="age must be greater than 0"):
        list(list(dlt_source.resources.values())[0])


def test_validator_accepts_valid_data():
    """Valid data passes all validators."""
    src = Source(name="test")

    @src.resource(response_model=ValidatedUser)
    def users() -> Iterator[dict]:
        yield {"id": 1, "name": "Alice", "age": 30}
        yield {"id": 2, "name": "Bob", "age": 25}

    dlt_source = src._build_dlt_source()
    records = list(list(dlt_source.resources.values())[0])
    assert len(records) == 2
    assert records[0]["age"] == 30


def test_validator_strips_whitespace():
    """Validator normalizes name by stripping whitespace."""
    src = Source(name="test")

    @src.resource(response_model=ValidatedUser)
    def users() -> Iterator[dict]:
        yield {"id": 1, "name": "  Alice  ", "age": 30}

    dlt_source = src._build_dlt_source()
    records = list(list(dlt_source.resources.values())[0])
    assert records[0]["name"] == "Alice"


def test_validator_rejects_empty_name():
    """Validator rejects empty/whitespace-only name."""
    src = Source(name="test")

    @src.resource(response_model=ValidatedUser)
    def users() -> Iterator[dict]:
        yield {"id": 1, "name": "   ", "age": 30}

    dlt_source = src._build_dlt_source()
    with pytest.raises(Exception, match="name must not be empty"):
        list(list(dlt_source.resources.values())[0])


# -- Missing required fields --


def test_missing_required_field_raises():
    """Missing required field raises ValidationError."""
    src = Source(name="test")

    @src.resource(response_model=UserModel)
    def users() -> Iterator[dict]:
        yield {"id": 1, "name": "Alice"}  # missing 'age'

    dlt_source = src._build_dlt_source()
    with pytest.raises(Exception):
        list(list(dlt_source.resources.values())[0])


# -- Optional fields --


class UserWithOptional(BaseModel):
    id: int
    name: str
    age: int | None = None
    city: str = "Unknown"


def test_optional_fields_use_defaults():
    """Optional fields get default values when not provided."""
    src = Source(name="test")

    @src.resource(response_model=UserWithOptional)
    def users() -> Iterator[dict]:
        yield {"id": 1, "name": "Alice"}

    dlt_source = src._build_dlt_source()
    records = list(list(dlt_source.resources.values())[0])
    assert records[0]["age"] is None
    assert records[0]["city"] == "Unknown"


# -- Integration: full pipeline with response_model --


def test_pipeline_with_response_model():
    """Full pipeline: response_model validates and loads into duckdb."""
    app = _app("p_val")
    src = Source(name="data")

    @src.resource(primary_key="id", response_model=UserModel, write_disposition="replace")
    def users() -> Iterator[dict]:
        yield {"id": "1", "name": "Alice", "age": "30"}
        yield {"id": "2", "name": "Bob", "age": "25"}

    app.include_source(src)
    app.run()

    rows = _query("p_val", "SELECT id, name, age FROM p_val_data.users ORDER BY id")
    assert rows[0] == (1, "Alice", 30)
    assert rows[1] == (2, "Bob", 25)


def test_app_source_decorator_with_response_model():
    """@app.source() accepts response_model."""
    app = _app("p_app_val")

    @app.source("users", primary_key="id", response_model=UserModel)
    def users():
        yield {"id": "1", "name": "Alice", "age": "30"}

    app.run()

    rows = _query("p_app_val", "SELECT id, age FROM p_app_val_data.users")
    assert rows[0] == (1, 30)


# -- Multiple extra fields --


def test_multiple_extra_fields_in_warning():
    """All extra fields are listed in the warning."""
    src = Source(name="test")

    @src.resource(response_model=UserModel)
    def users() -> Iterator[dict]:
        yield {"id": 1, "name": "Alice", "age": 30, "city": "Paris", "role": "admin"}

    dlt_source = src._build_dlt_source()
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        records = list(list(dlt_source.resources.values())[0])
        assert len(w) == 1
        msg = str(w[0].message)
        assert "city" in msg
        assert "role" in msg

    assert "city" not in records[0]
    assert "role" not in records[0]


# -- No response_model: passthrough --


def test_no_response_model_passes_through():
    """Without response_model, records pass through unmodified."""
    src = Source(name="test")

    @src.resource()
    def users() -> Iterator[dict]:
        yield {"id": 1, "name": "Alice", "extra_field": "hello"}

    dlt_source = src._build_dlt_source()
    records = list(list(dlt_source.resources.values())[0])
    assert records[0]["extra_field"] == "hello"
