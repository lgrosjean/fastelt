import json
from typing import Iterator

import pytest
from pydantic import BaseModel, Field, ValidationError

from fastelt import FastELT, Records


class User(BaseModel):
    name: str
    email: str
    age: int


def test_full_pipeline_csv_to_json(tmp_path):
    """Integration test: CSV extract -> JSON load."""
    csv_file = tmp_path / "users.csv"
    csv_file.write_text(
        "name,email,age\nAlice,alice@example.com,30\nBob,bob@example.com,25\n"
    )
    json_file = tmp_path / "users.json"

    app = FastELT()

    @app.extractor("csv_users")
    def extract_users(
        path: str = Field(..., description="Path to CSV"),
        delimiter: str = Field(default=","),
    ) -> Iterator[User]:
        import csv

        with open(path) as f:
            for row in csv.DictReader(f, delimiter=delimiter):
                yield User(**row)

    @app.loader("json_users")
    def load_users(
        records: Records[User],
        path: str = Field(..., description="Output path"),
    ) -> None:
        with open(path, "w") as f:
            json.dump([r.model_dump() for r in records], f)

    app.run(
        extractor="csv_users",
        loader="json_users",
        extractor_config={"path": str(csv_file)},
        loader_config={"path": str(json_file)},
    )

    data = json.loads(json_file.read_text())
    assert len(data) == 2
    assert data[0]["name"] == "Alice"
    assert data[0]["age"] == 30
    assert data[1]["name"] == "Bob"


def test_pipeline_with_validation_error():
    """Test that type validation catches bad records."""
    app = FastELT()

    class Other(BaseModel):
        x: int

    @app.extractor("bad_ext")
    def bad_extractor() -> Iterator[User]:
        yield Other(x=1)

    @app.loader("sink")
    def sink(records: Records[User]) -> None:
        records.collect()

    with pytest.raises(TypeError, match="Expected User"):
        app.run(extractor="bad_ext", loader="sink")


def test_pipeline_config_validation():
    """Config validation rejects missing required fields."""
    app = FastELT()

    @app.extractor("ext")
    def ext(path: str = Field(...)) -> Iterator[User]:
        yield User(name="a", email="b", age=1)

    @app.loader("ldr")
    def ldr(records: Records[User]) -> None:
        records.collect()

    with pytest.raises(ValidationError):
        app.run(
            extractor="ext",
            loader="ldr",
            extractor_config={},  # missing required 'path'
        )


def test_pipeline_no_validation():
    """Pipeline runs without record validation when disabled."""
    app = FastELT()

    @app.extractor("ext")
    def ext() -> Iterator[User]:
        yield User(name="a", email="b", age=1)

    collected = []

    @app.loader("ldr")
    def ldr(records: Records[User]) -> None:
        collected.extend(records)

    app.run(extractor="ext", loader="ldr", validate_records=False)
    assert len(collected) == 1


def test_loader_without_records():
    """Loader that doesn't need records still works."""
    app = FastELT()
    side_effects = []

    @app.extractor("ext")
    def ext() -> Iterator[User]:
        yield User(name="a", email="b", age=1)

    @app.loader("notify")
    def notify(message: str = Field(default="done")) -> None:
        side_effects.append(message)

    app.run(extractor="ext", loader="notify")
    assert side_effects == ["done"]


def test_builtin_csv_to_json_via_include(tmp_path):
    """Integration test using built-in plugins via include."""
    csv_file = tmp_path / "data.csv"
    csv_file.write_text("name,email,age\nEve,eve@test.com,28\n")
    json_file = tmp_path / "data.json"

    from fastelt.extractors.csv import csv_extractor
    from fastelt.loaders.json import json_loader

    app = FastELT()
    app.include(csv_extractor(User))
    app.include(json_loader(User))

    app.run(
        extractor="csv",
        loader="json",
        extractor_config={"path": str(csv_file)},
        loader_config={"path": str(json_file)},
    )

    data = json.loads(json_file.read_text())
    assert len(data) == 1
    assert data[0]["name"] == "Eve"
    assert data[0]["age"] == 28


def test_records_collect():
    """Records.collect() returns a list."""
    app = FastELT()

    @app.extractor("ext")
    def ext() -> Iterator[User]:
        yield User(name="a", email="b", age=1)
        yield User(name="c", email="d", age=2)

    result = []

    @app.loader("ldr")
    def ldr(records: Records[User]) -> None:
        result.extend(records.collect())

    app.run(extractor="ext", loader="ldr")
    assert len(result) == 2
    assert result[0].name == "a"


def test_records_consumed_twice_raises():
    """Records can only be iterated once."""
    records = Records(iter([User(name="a", email="b", age=1)]))
    list(records)
    with pytest.raises(RuntimeError, match="already been consumed"):
        list(records)


def test_batch_extractor_pipeline():
    """Batch extractor (return list) works end-to-end."""
    app = FastELT()

    @app.extractor()
    def batch_users() -> list[User]:
        return [
            User(name="a", email="a@test.com", age=1),
            User(name="b", email="b@test.com", age=2),
        ]

    collected = []

    @app.loader()
    def sink(records: Records[User]) -> None:
        collected.extend(records.collect())

    app.run(extractor="batch_users", loader="sink")
    assert len(collected) == 2
    assert collected[0].name == "a"


def test_batch_extractor_with_validation():
    """Batch extractor records are validated too."""
    app = FastELT()

    class Other(BaseModel):
        x: int

    @app.extractor()
    def bad_batch() -> list[User]:
        return [Other(x=1)]  # type: ignore

    @app.loader()
    def sink(records: Records[User]) -> None:
        records.collect()

    with pytest.raises(TypeError, match="Expected User"):
        app.run(extractor="bad_batch", loader="sink")
