"""Tests for incremental loading and state persistence."""

import json
from datetime import datetime
from typing import Iterator

import pytest
from pydantic import BaseModel, Field

from fastelt import FastELT, Incremental, Records, Source, WriteDisposition


class Event(BaseModel):
    id: int
    name: str
    updated_at: datetime


class Item(BaseModel):
    id: int
    value: str


# -- Incremental class unit tests --


def test_incremental_last_value_returns_initial_on_first_run():
    inc = Incremental[int]("id", initial_value=0)
    assert inc.last_value == 0


def test_incremental_last_value_returns_loaded_state():
    inc = Incremental[int]("id", initial_value=0)
    clone = inc._clone()
    clone._last_value = 42
    assert clone.last_value == 42


def test_incremental_track_computes_max():
    inc = Incremental[int]("id", initial_value=0)
    clone = inc._clone()
    clone._track(10)
    clone._track(5)
    clone._track(20)
    assert clone.end_value == 20


def test_incremental_track_with_min():
    inc = Incremental[int]("id", initial_value=100, last_value_func=min)
    clone = inc._clone()
    clone._track(10)
    clone._track(5)
    clone._track(20)
    assert clone.end_value == 5


def test_incremental_track_ignores_none():
    inc = Incremental[int]("id", initial_value=0)
    clone = inc._clone()
    clone._track(None)
    assert clone.end_value is None
    clone._track(5)
    clone._track(None)
    assert clone.end_value == 5


def test_incremental_serialize_deserialize_int():
    inc = Incremental[int]("id", initial_value=0)
    assert inc._serialize(42) == 42
    assert inc._deserialize(42) == 42


def test_incremental_serialize_deserialize_datetime():
    inc = Incremental[datetime]("updated_at", initial_value=datetime.min)
    dt = datetime(2024, 6, 15, 12, 30, 0)
    serialized = inc._serialize(dt)
    assert serialized == {"__type__": "datetime", "v": "2024-06-15T12:30:00"}
    deserialized = inc._deserialize(serialized)
    assert deserialized == dt


def test_incremental_clone_resets_state():
    inc = Incremental[int]("id", initial_value=0)
    inc._last_value = 10
    inc._end_value = 20
    clone = inc._clone()
    assert clone._last_value is None
    assert clone._end_value is None
    assert clone.cursor_path == "id"
    assert clone.initial_value == 0


# -- Pipeline integration tests --


def test_incremental_pipeline_first_run(tmp_path):
    """First run: no state, uses initial_value."""
    state_dir = str(tmp_path / "state")
    app = FastELT(state_dir=state_dir)

    seen_last_value = []

    @app.extractor()
    def events(
        since_id: Incremental[int] = Incremental("id", initial_value=0),
    ) -> Iterator[Event]:
        seen_last_value.append(since_id.last_value)
        yield Event(id=1, name="a", updated_at=datetime(2024, 1, 1))
        yield Event(id=3, name="c", updated_at=datetime(2024, 3, 1))
        yield Event(id=2, name="b", updated_at=datetime(2024, 2, 1))

    collected = []

    @app.loader()
    def sink(records: Records[Event]) -> None:
        collected.extend(records.collect())

    app.run(extractor="events", loader="sink")

    assert seen_last_value == [0]
    assert len(collected) == 3

    # State should be persisted
    state_file = tmp_path / "state" / "events.json"
    assert state_file.exists()
    state = json.loads(state_file.read_text())
    assert state["since_id"] == 3  # max(1, 3, 2)


def test_incremental_pipeline_second_run(tmp_path):
    """Second run: loads persisted state."""
    state_dir = str(tmp_path / "state")
    app = FastELT(state_dir=state_dir)

    seen_last_value = []

    @app.extractor()
    def events(
        since_id: Incremental[int] = Incremental("id", initial_value=0),
    ) -> Iterator[Event]:
        seen_last_value.append(since_id.last_value)
        yield Event(id=10, name="d", updated_at=datetime(2024, 4, 1))

    @app.loader()
    def sink(records: Records[Event]) -> None:
        records.collect()

    # Pre-seed state
    (tmp_path / "state").mkdir(parents=True)
    (tmp_path / "state" / "events.json").write_text(json.dumps({"since_id": 5}))

    app.run(extractor="events", loader="sink")

    assert seen_last_value == [5]  # loaded from state


def test_incremental_datetime_cursor(tmp_path):
    """Test datetime cursor serialization round-trip."""
    state_dir = str(tmp_path / "state")
    app = FastELT(state_dir=state_dir)

    @app.extractor()
    def events(
        since: Incremental[datetime] = Incremental(
            "updated_at", initial_value=datetime(2000, 1, 1)
        ),
    ) -> Iterator[Event]:
        yield Event(id=1, name="a", updated_at=datetime(2024, 6, 15))
        yield Event(id=2, name="b", updated_at=datetime(2024, 8, 20))

    @app.loader()
    def sink(records: Records[Event]) -> None:
        records.collect()

    app.run(extractor="events", loader="sink")

    # Verify state was saved correctly
    state = json.loads((tmp_path / "state" / "events.json").read_text())
    assert state["since"]["__type__"] == "datetime"
    assert state["since"]["v"] == "2024-08-20T00:00:00"

    # Second run should deserialize correctly
    seen = []

    @app.extractor("events2")
    def events2(
        since: Incremental[datetime] = Incremental(
            "updated_at", initial_value=datetime(2000, 1, 1)
        ),
    ) -> Iterator[Event]:
        seen.append(since.last_value)
        yield Event(id=3, name="c", updated_at=datetime(2024, 9, 1))

    # Copy state for events2
    (tmp_path / "state" / "events2.json").write_text(
        (tmp_path / "state" / "events.json").read_text()
    )
    app.run(extractor="events2", loader="sink")
    assert seen == [datetime(2024, 8, 20)]


def test_incremental_excluded_from_config():
    """Incremental params should not appear in the config model."""
    app = FastELT()

    @app.extractor()
    def events(
        since_id: Incremental[int] = Incremental("id", initial_value=0),
        limit: int = Field(default=100),
    ) -> Iterator[Event]:
        yield Event(id=1, name="a", updated_at=datetime(2024, 1, 1))

    reg = app.get_extractor("events")
    assert "since_id" not in reg.config_model.model_fields
    assert "limit" in reg.config_model.model_fields


def test_write_disposition_on_extractor():
    app = FastELT()

    @app.extractor(write_disposition="merge", primary_key="id")
    def events() -> Iterator[Event]:
        yield Event(id=1, name="a", updated_at=datetime(2024, 1, 1))

    reg = app.get_extractor("events")
    assert reg.write_disposition == WriteDisposition.MERGE


def test_write_disposition_default_is_append():
    app = FastELT()

    @app.extractor()
    def events() -> Iterator[Event]:
        yield Event(id=1, name="a", updated_at=datetime(2024, 1, 1))

    reg = app.get_extractor("events")
    assert reg.write_disposition == WriteDisposition.APPEND


def test_incremental_with_dict_records(tmp_path):
    """Incremental tracking works when extractor yields dicts."""
    state_dir = str(tmp_path / "state")
    app = FastELT(state_dir=state_dir)

    @app.extractor()
    def items(
        since_id: Incremental[int] = Incremental("id", initial_value=0),
    ) -> Iterator[Item]:
        yield {"id": 5, "value": "x"}
        yield {"id": 10, "value": "y"}
        yield {"id": 7, "value": "z"}

    @app.loader()
    def sink(records: Records[Item]) -> None:
        records.collect()

    app.run(extractor="items", loader="sink")

    state = json.loads((tmp_path / "state" / "items.json").read_text())
    assert state["since_id"] == 10


def test_extractor_with_no_incremental_no_state(tmp_path):
    """Non-incremental extractors should not create state files."""
    state_dir = str(tmp_path / "state")
    app = FastELT(state_dir=state_dir)

    @app.extractor()
    def simple() -> Iterator[Item]:
        yield Item(id=1, value="a")

    @app.loader()
    def sink(records: Records[Item]) -> None:
        records.collect()

    app.run(extractor="simple", loader="sink")
    assert not (tmp_path / "state").exists()
