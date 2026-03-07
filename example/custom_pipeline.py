"""Example: custom extractor and loader using @decorators.

Demonstrates both styles:
- A loader WITH Records[T] injection (json_users)
- A loader WITHOUT Records (log_done) — just receives config
"""

import csv
import json
from typing import Iterator

from pydantic import BaseModel, Field

from fastelt import FastELT, Records

app = FastELT()


class User(BaseModel):
    name: str
    email: str
    age: int
    city: str


@app.extractor("csv_users")
def extract_users(
    path: str = Field(..., description="Path to CSV file"),
    delimiter: str = Field(default=",", description="CSV delimiter"),
) -> Iterator[User]:
    with open(path) as f:
        for row in csv.DictReader(f, delimiter=delimiter):
            yield User(**row)


@app.loader("json_users")
def load_users(
    records: Records[User],
    path: str = Field(..., description="Output JSON file path"),
    indent: int = Field(default=2, description="JSON indentation"),
) -> None:
    with open(path, "w") as f:
        json.dump([r.model_dump() for r in records], f, indent=indent)


@app.loader("log_done")
def log_done(message: str = Field(default="Pipeline finished")) -> None:
    print(message)


if __name__ == "__main__":
    # With Records injection
    app.run(
        extractor="csv_users",
        loader="json_users",
        extractor_config={"path": "example/users.csv"},
        loader_config={"path": "example/users.json"},
    )
    print("Done! Written to example/users.json")

    # Without Records — loader just runs with its config
    app.run(
        extractor="csv_users",
        loader="log_done",
        extractor_config={"path": "example/users.csv"},
        loader_config={"message": "CSV pipeline completed!"},
    )
