"""Example: Simple fastELT app — CSV data into DuckDB via dlt.

Demonstrates:
- Pydantic response_model for type enforcement (CSV yields strings → int)
- alias_generator for column normalization
- Field validators for data quality checks (age > 0, email format)
- Extra field detection with warnings

Usage:  python example/fastelt_app.py
"""

import csv

from pydantic import BaseModel, ConfigDict, EmailStr, field_validator
from pydantic.alias_generators import to_snake

from fastelt import FastELT, Source
from fastelt.destinations import DuckDBDestination


# --- Pydantic model for data validation ---


class UserModel(BaseModel):
    """Pydantic model enforcing types, column normalization, and data quality."""

    model_config = ConfigDict(
        alias_generator=to_snake,
        populate_by_name=True,
    )

    name: str
    email: str
    age: int
    city: str

    @field_validator("age")
    @classmethod
    def age_must_be_positive(cls, v: int) -> int:
        if v <= 0:
            raise ValueError(f"age must be > 0, got {v}")
        return v

    @field_validator("email")
    @classmethod
    def email_must_contain_at(cls, v: str) -> str:
        if "@" not in v:
            raise ValueError(f"invalid email: {v!r}")
        return v.lower()

    @field_validator("name")
    @classmethod
    def name_must_not_be_empty(cls, v: str) -> str:
        if not v.strip():
            raise ValueError("name must not be empty")
        return v.strip()


# --- Source with validated resource ---

local_data = Source(name="local")


@local_data.resource(
    primary_key="name",
    write_disposition="replace",
    response_model=UserModel,
)
def users():
    """Extract users from CSV — pydantic coerces age from str to int."""
    with open("example/users.csv") as f:
        for row in csv.DictReader(f):
            yield row


# Wire up
db = DuckDBDestination()

app = FastELT(pipeline_name="local_pipeline")
app.include_destination(db)
app.include_source(local_data)

if __name__ == "__main__":
    info = app.run(destination=db)
    print(f"Done! {info}")
    print("Data loaded into local_pipeline.duckdb")
