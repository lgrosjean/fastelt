"""Tests for FastELT app — the top-level orchestrator wrapping dlt."""

from __future__ import annotations

import shutil
from pathlib import Path
from typing import Annotated, Iterator

import dlt
import duckdb
import pytest

from fastelt import FastELT, Source
from fastelt.destinations import DuckDBDestination
from fastelt.sources import Incremental

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


db = DuckDBDestination()


def _app(name: str) -> FastELT:
    """Create an app with a duckdb destination and register it for cleanup."""
    PIPELINES.append(name)
    app = FastELT(pipeline_name=name)
    app.include_destination(db)
    return app


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


# -- Destination registration --


def test_include_destination():
    app = FastELT(pipeline_name="p_dest")
    dest = DuckDBDestination()
    app.include_destination(dest)
    assert "duckdb" in app.list_destinations()


def test_destination_name_from_class():
    """Name is auto-derived from class name."""
    from fastelt.destinations import BigQueryDestination

    bq = BigQueryDestination(project_id="test")
    assert bq.name == "bigquery"

    ddb = DuckDBDestination()
    assert ddb.name == "duckdb"


def test_get_destination():
    app = FastELT(pipeline_name="p_dest2")
    dest = DuckDBDestination()
    app.include_destination(dest)
    assert app.get_destination("duckdb") is dest


def test_get_destination_not_found():
    app = FastELT(pipeline_name="p_dest3")
    with pytest.raises(KeyError, match="nope"):
        app.get_destination("nope")


def test_run_unknown_destination_raises():
    app = FastELT(pipeline_name="p_dest4")
    src = Source(name="test")

    @src.resource()
    def items():
        yield {"id": 1}

    app.include_source(src)
    with pytest.raises(KeyError, match="fake"):
        app.run(destination="fake")


def test_run_with_destination_object():
    """app.run(destination=obj) auto-registers and runs."""
    PIPELINES.append("p_dst_obj")
    app = FastELT(pipeline_name="p_dst_obj")

    @app.source("items")
    def items():
        yield {"id": 1}

    dest = DuckDBDestination()
    app.run(destination=dest)

    rows = _query("p_dst_obj", "SELECT id FROM p_dst_obj_data.items")
    assert len(rows) == 1


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
    app.run(destination=db)

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

    result = app.run(destination=db)
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
    app.run(destination=db, source="alpha")

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
    app.run(destination=db, resources=["users"])

    tables = _query(
        "p_sel_res",
        "SELECT table_name FROM information_schema.tables WHERE table_schema = 'p_sel_res_data'",
    )
    table_names = [t[0] for t in tables]
    assert "users" in table_names


# -- Error handling --


def test_run_unknown_source_raises():
    app = _app("p_err2")
    src = Source(name="real")

    @src.resource()
    def items():
        yield {"id": 1}

    app.include_source(src)
    with pytest.raises(KeyError, match="fake"):
        app.run(destination=db, source="fake")


def test_run_no_sources_raises():
    app = _app("p_err3")
    with pytest.raises(ValueError, match="No sources"):
        app.run(destination=db)


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
    app.run(destination=db)

    # Second run with updated data
    src2 = Source(name="data")

    @src2.resource(primary_key="id", write_disposition="merge")
    def users():
        yield {"id": 1, "name": "Alice", "age": 31}  # updated
        yield {"id": 3, "name": "Charlie", "age": 20}  # new

    app2 = _app("p_merge")
    app2.include_source(src2)
    app2.run(destination=db)

    rows = _query("p_merge", "SELECT id, name, age FROM p_merge_data.users ORDER BY id")
    assert len(rows) == 3
    assert rows[0] == (1, "Alice", 31)
    assert rows[1] == (2, "Bob", 25)
    assert rows[2] == (3, "Charlie", 20)


# -- Incremental loading --


def test_incremental_loading():
    """Annotated[str, Incremental(...)] tracks cursor between runs."""
    app = _app("p_incr")
    src = Source(name="events")

    @src.resource(primary_key="id", write_disposition="append")
    def events(updated_at: Annotated[str, Incremental(initial_value="2024-01-01")]):
        yield {"id": 1, "name": "a", "updated_at": "2024-06-01"}
        yield {"id": 2, "name": "b", "updated_at": "2024-07-01"}

    app.include_source(src)
    app.run(destination=db)

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
    app.run(destination=db)

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


# -- @app.source() decorator --


def test_app_source_decorator():
    """@app.source() registers a single-resource source."""
    app = _app("p_app_src")

    @app.source("users", primary_key="id")
    def users():
        yield {"id": 1, "name": "Alice"}
        yield {"id": 2, "name": "Bob"}

    assert "users" in app.list_sources()
    app.run(destination=db)

    rows = _query("p_app_src", "SELECT name FROM p_app_src_data.users ORDER BY id")
    assert [r[0] for r in rows] == ["Alice", "Bob"]


def test_app_source_decorator_default_name():
    """@app.source() uses function name when no name given."""
    app = _app("p_app_name")

    @app.source()
    def events():
        yield {"id": 1, "type": "click"}

    assert "events" in app.list_sources()


def test_app_source_decorator_write_disposition():
    """@app.source() passes write_disposition to dlt."""
    app = _app("p_app_wd")

    @app.source("items", write_disposition="replace")
    def items():
        yield {"id": 1, "val": "a"}

    app.run(destination=db)
    app.run(destination=db)  # second run should replace, not append

    rows = _query("p_app_wd", "SELECT COUNT(*) FROM p_app_wd_data.items")
    assert rows[0][0] == 1


# -- Destination with extra fields --


def test_bigquery_destination_fields():
    """BigQueryDestination has typed fields."""
    from fastelt.destinations import BigQueryDestination

    bq = BigQueryDestination(
        project_id="my-project",
        location="EU",
        dataset_name="analytics",
    )
    assert bq.name == "bigquery"
    assert bq.destination_type == "bigquery"
    assert bq.dataset_name == "analytics"
    assert bq.project_id == "my-project"
    assert bq.location == "EU"


def test_destination_dataset_name_used():
    """Destination's dataset_name is used by pipeline."""
    app = FastELT(pipeline_name="p_ds")
    PIPELINES.append("p_ds")
    dest = DuckDBDestination(dataset_name="custom_ds")

    @app.source("items")
    def items():
        yield {"id": 1}

    app.run(destination=dest)

    rows = _query("p_ds", "SELECT id FROM custom_ds.items")
    assert len(rows) == 1


# -- @app.destination() decorator --


def test_app_destination_decorator():
    """@app.destination() registers a custom sink function."""
    PIPELINES.append("p_custom_dest")
    app = FastELT(pipeline_name="p_custom_dest")
    received = []

    @app.destination(batch_size=10)
    def my_sink(items, table):
        for item in items:
            received.append({"table": table["name"], **item})

    @app.source("users")
    def users():
        yield {"id": 1, "name": "Alice"}
        yield {"id": 2, "name": "Bob"}

    assert "my_sink" in app.list_destinations()
    app.run(destination=my_sink)

    assert len(received) == 2
    assert received[0]["table"] == "users"
    assert received[0]["name"] == "Alice"
    assert received[1]["name"] == "Bob"


def test_app_destination_decorator_name():
    """@app.destination() uses function name as destination name."""
    app = FastELT(pipeline_name="p_dest_name")

    @app.destination()
    def warehouse(items, table):
        pass

    assert warehouse.name == "warehouse"
    assert "warehouse" in app.list_destinations()
