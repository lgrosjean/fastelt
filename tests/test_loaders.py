import csv
import json

from pydantic import BaseModel

from fastelt.loaders.csv import csv_loader
from fastelt.loaders.json import json_loader
from fastelt.types import Records


class User(BaseModel):
    name: str
    email: str
    age: int


def _sample_users() -> list[User]:
    return [
        User(name="Alice", email="alice@example.com", age=30),
        User(name="Bob", email="bob@example.com", age=25),
    ]


def test_json_loader(tmp_path):
    out = tmp_path / "out.json"
    plugin = json_loader(User)
    reg = plugin.loaders["json"]
    config = reg.config_model(path=str(out))
    kwargs = config.model_dump()
    kwargs[reg.records_param] = Records(iter(_sample_users()))
    reg.func(**kwargs)

    data = json.loads(out.read_text())
    assert len(data) == 2
    assert data[0]["name"] == "Alice"


def test_csv_loader(tmp_path):
    out = tmp_path / "out.csv"
    plugin = csv_loader(User)
    reg = plugin.loaders["csv"]
    config = reg.config_model(path=str(out))
    kwargs = config.model_dump()
    kwargs[reg.records_param] = Records(iter(_sample_users()))
    reg.func(**kwargs)

    with open(out) as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    assert len(rows) == 2
    assert rows[0]["name"] == "Alice"
    assert rows[1]["age"] == "25"
