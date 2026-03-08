"""Tests for FastELT app — the top-level orchestrator wrapping dlt."""

from __future__ import annotations

import shutil
from pathlib import Path
from typing import Iterator

import dlt
import duckdb
import pytest

from fastelt import FastELT, Source

PIPELINES = []


def _cleanup_pipeline(pipeline_name: str):
    """Clean up dlt pipeline artifacts."""
    for suffix in [".duckdb", ".duckdb.wal"]:
        p = Path(f"{pipeline_name}{suffix}")
        if p.exists():
            p.unlink()
    # Clean dlt working dir
    working_dir = Path(f"/var/dlt/pipelines/{pipeline_name}")
    if working_dir.exists():
        shutil.rmtree(working_dir, ignore_errors=True)


@pytest.fixture(autouse=True)
def cleanup():
    """Clean up after each test."""
    PIPELINES.clear()
    yield
    for name in PIPELINES:
        _cleanup_pipeline(name)


def _app(name: str, **kwargs) -> FastELT:
    """Create an app and register it for cleanup."""
    PIPELINES.append(name)
    return FastELT(pipeline_name=name, destination="duckdb", **kwargs)


def _query(pipeline_name: str, sql: str):
    """Run a query against the pipeline's duckdb."""
    conn = duckdb.connect(f"{pipeline_name}.duckdb")
    result = conn.sql(sql).fetchall()
    conn.close()
    return result


# -- Source registration --


def test_include_source():
    app = _app("p1")
    src = Source(name="test")

    @src.resource()
    def items():
        yield {"id": 1}

    app.include_source(src)
    assert "test" in app.list_sources()
    assert app.list_resources() == {"test": ["items"]}


def test_include_source_custom_name():
    app = _app("p2")
    src = Source(name="original")

    @src.resource()
    def items():
        yield {"id": 1}

    app.include_source(src, name="custom")
    assert "custom" in app.list_sources()


def test_include_source_subclass_name():
    class GitHubSource(Source):
        token: str

    app = _app("p3")
    gh = GitHubSource(token="abc")

    @gh.resource()
    def repos():
        yield {"id": 1}

    app.include_source(gh)
    assert "githubsource" in app.list_sources()


# -- Pipeline execution --


def test_run_basic_pipeline():
    """Full pipeline: source -> duckdb destination."""
    app = _app("p_basic")
    src = Source(name="mydata")

    @src.resource(primary_key="id")
    def users():
        yield {"id": 1, "name": "Alice"}
        yield {"id": 2, "name": "Bob"}

    app.include_source(src)
    app.run()

    rows = _query("p_basic", "SELECT id, name FROM p_basic_data.users ORDER BY id")
    assert len(rows) == 2
    assert rows[0] == (1, "Alice")
    assert rows[1] == (2, "Bob")


def test_run_multiple_sources():
    """Multiple sources run sequentially."""
    app = _app("p_multi")

    src_a = Source(name="source_a")

    @src_a.resource(primary_key="id")
    def items_a():
        yield {"id": 1, "val": "a"}

    src_b = Source(name="source_b")

    @src_b.resource(primary_key="id")
    def items_b():
        yield {"id": 10, "val": "b"}

    app.include_source(src_a)
    app.include_source(src_b)

    result = app.run()
    assert isinstance(result, list)
    assert len(result) == 2


def test_run_selective_source():
    """Run only a specific source."""
    app = _app("p_sel_src")

    src_a = Source(name="alpha")

    @src_a.resource()
    def alpha_data():
        yield {"id": 1}

    src_b = Source(name="beta")

    @src_b.resource()
    def beta_data():
        yield {"id": 2}

    app.include_source(src_a)
    app.include_source(src_b)

    # Only run alpha
    app.run(source="alpha")

    tables = _query(
        "p_sel_src",
        "SELECT table_name FROM information_schema.tables WHERE table_schema = 'p_sel_src_data'",
    )
    table_names = [t[0] for t in tables]
    assert "alpha_data" in table_names
    assert "beta_data" not in table_names


def test_run_selective_resources():
    """Run only specific resources from a source."""
    app = _app("p_sel_res")
    src = Source(name="mydata")

    @src.resource(primary_key="id")
    def users():
        yield {"id": 1, "name": "Alice"}

    @src.resource(primary_key="id")
    def orders():
        yield {"id": 100, "amount": 42}

    app.include_source(src)
    app.run(resources=["users"])

    tables = _query(
        "p_sel_res",
        "SELECT table_name FROM information_schema.tables WHERE table_schema = 'p_sel_res_data'",
    )
    table_names = [t[0] for t in tables]
    assert "users" in table_names


# -- Error handling --


def test_run_no_destination_raises():
    app = FastELT(pipeline_name="p_err")
    src = Source(name="test")

    @src.resource()
    def items():
        yield {"id": 1}

    app.include_source(src)
    with pytest.raises(ValueError, match="No destination"):
        app.run()


def test_run_unknown_source_raises():
    app = _app("p_err2")
    src = Source(name="real")

    @src.resource()
    def items():
        yield {"id": 1}

    app.include_source(src)
    with pytest.raises(KeyError, match="fake"):
        app.run(source="fake")


def test_run_no_sources_raises():
    app = _app("p_err3")
    with pytest.raises(ValueError, match="No sources"):
        app.run()


# -- Write dispositions --


def test_write_disposition_merge():
    """Merge write disposition does upsert."""
    app = _app("p_merge")
    src = Source(name="data")

    @src.resource(primary_key="id", write_disposition="merge")
    def users():
        yield {"id": 1, "name": "Alice", "age": 30}
        yield {"id": 2, "name": "Bob", "age": 25}

    app.include_source(src)
    app.run()

    # Second run with updated data
    src2 = Source(name="data")

    @src2.resource(primary_key="id", write_disposition="merge")
    def users():
        yield {"id": 1, "name": "Alice", "age": 31}  # updated
        yield {"id": 3, "name": "Charlie", "age": 20}  # new

    app2 = _app("p_merge")
    app2.include_source(src2)
    app2.run()

    rows = _query("p_merge", "SELECT id, name, age FROM p_merge_data.users ORDER BY id")
    assert len(rows) == 3
    assert rows[0] == (1, "Alice", 31)
    assert rows[1] == (2, "Bob", 25)
    assert rows[2] == (3, "Charlie", 20)


# -- Incremental loading --


def test_incremental_loading():
    """dlt.sources.incremental tracks cursor between runs."""
    app = _app("p_incr")
    src = Source(name="events")

    @src.resource(primary_key="id", write_disposition="append")
    def events(updated_at=dlt.sources.incremental("updated_at", initial_value="2024-01-01")):
        yield {"id": 1, "name": "a", "updated_at": "2024-06-01"}
        yield {"id": 2, "name": "b", "updated_at": "2024-07-01"}

    app.include_source(src)
    app.run()

    rows = _query("p_incr", "SELECT id FROM p_incr_data.events ORDER BY id")
    assert len(rows) == 2


# -- Source injection --


def test_source_with_injection():
    """Source injects itself into resource functions."""

    class MySource(Source):
        token: str
        org: str

    src = MySource(token="abc", org="test_org")

    @src.resource()
    def repos(source: MySource):
        yield {"id": 1, "org": source.org}

    app = _app("p_inject")
    app.include_source(src, name="my")
    app.run()

    rows = _query("p_inject", "SELECT org FROM p_inject_data.repos")
    assert rows[0][0] == "test_org"


# -- Introspection --


def test_get_source():
    app = _app("p_intro")
    src = Source(name="mydata")

    @src.resource()
    def items():
        yield {"id": 1}

    app.include_source(src)
    retrieved = app.get_source("mydata")
    assert retrieved is src


def test_get_source_not_found():
    app = _app("p_intro2")
    with pytest.raises(KeyError, match="nope"):
        app.get_source("nope")


def test_list_resources_by_source():
    app = _app("p_intro3")

    src = Source(name="mydata")

    @src.resource()
    def users():
        yield {"id": 1}

    @src.resource()
    def orders():
        yield {"id": 100}

    app.include_source(src)
    result = app.list_resources("mydata")
    assert result == {"mydata": ["users", "orders"]}


def test_destination_override_at_run():
    """Destination can be overridden per-run."""
    app = FastELT(pipeline_name="p_override")
    src = Source(name="test")

    @src.resource()
    def items():
        yield {"id": 1}

    app.include_source(src)
    PIPELINES.append("p_override")

    # Override destination at run time
    app.run(destination="duckdb")
    rows = _query("p_override", "SELECT id FROM p_override_data.items")
    assert len(rows) == 1
