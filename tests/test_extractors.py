import json

from pydantic import BaseModel

from fastelt.extractors.csv import csv_extractor
from fastelt.extractors.json import json_extractor


class User(BaseModel):
    name: str
    email: str
    age: int


def test_csv_extractor(tmp_path):
    csv_file = tmp_path / "users.csv"
    csv_file.write_text("name,email,age\nAlice,alice@example.com,30\nBob,bob@example.com,25\n")

    plugin = csv_extractor(User)
    reg = plugin.extractors["csv"]
    config = reg.config_model(path=str(csv_file))
    records = list(reg.func(**config.model_dump()))

    assert len(records) == 2
    assert records[0].name == "Alice"
    assert records[0].age == 30
    assert records[1].name == "Bob"


def test_json_extractor(tmp_path):
    json_file = tmp_path / "users.json"
    data = [
        {"name": "Alice", "email": "alice@example.com", "age": 30},
        {"name": "Bob", "email": "bob@example.com", "age": 25},
    ]
    json_file.write_text(json.dumps(data))

    plugin = json_extractor(User)
    reg = plugin.extractors["json"]
    config = reg.config_model(path=str(json_file))
    records = list(reg.func(**config.model_dump()))

    assert len(records) == 2
    assert records[0].name == "Alice"
    assert records[1].email == "bob@example.com"
