"""Tests for source-based pipeline routing."""

from typing import Iterator

import pytest
from pydantic import BaseModel, Field

from fastelt import FastELT, Records, Source


class Repo(BaseModel):
    name: str
    stars: int


class Issue(BaseModel):
    title: str
    state: str


class MlModel(BaseModel):
    name: str
    version: int


def _make_github_app():
    """Helper: create an app with a github source that has two entities."""
    github = Source(base_url="https://api.github.com", org="test")

    @github.entity()
    def repositories() -> Iterator[Repo]:
        yield Repo(name="fastelt", stars=100)
        yield Repo(name="sdk", stars=50)

    @github.entity()
    def issues(state: str = Field(default="open")) -> Iterator[Issue]:
        yield Issue(title="bug", state=state)
        yield Issue(title="feature", state=state)

    app = FastELT()
    app.include_source(github, name="github")
    return app


def test_run_single_entity_by_name():
    """Plain extractor name still works."""
    app = _make_github_app()
    collected = []

    @app.loader()
    def sink(records: Records[Repo]) -> None:
        collected.extend(records.collect())

    app.run(extractor="repositories", loader="sink")
    assert len(collected) == 2
    assert collected[0].name == "fastelt"


def test_run_source_colon_entity():
    """source:entity runs a single entity from a source."""
    app = _make_github_app()
    collected = []

    @app.loader()
    def sink(records: Records[Issue]) -> None:
        collected.extend(records.collect())

    app.run(extractor="github:issues", loader="sink")
    assert len(collected) == 2
    assert collected[0].title == "bug"


def test_run_source_runs_all_entities():
    """Just a source name runs ALL its entities sequentially."""
    app = _make_github_app()
    all_records = []

    @app.loader()
    def sink(records: Records[BaseModel]) -> None:
        all_records.extend(records.collect())

    app.run(extractor="github", loader="sink")
    # Both repositories and issues entities should have run
    assert len(all_records) == 4


def test_source_colon_entity_wrong_source_raises():
    """source:entity fails if entity doesn't belong to the named source."""
    app = _make_github_app()

    # Add a second source with different entity
    mlflow = Source(tracking_uri="http://localhost")

    @mlflow.entity()
    def models() -> Iterator[MlModel]:
        yield MlModel(name="model", version=1)

    app.include_source(mlflow, name="mlflow")

    @app.loader()
    def sink(records: Records[MlModel]) -> None:
        records.collect()

    with pytest.raises(KeyError, match="does not belong to source"):
        app.run(extractor="mlflow:repositories", loader="sink")


def test_unknown_source_raises():
    app = _make_github_app()

    @app.loader()
    def sink(records: Records[Repo]) -> None:
        records.collect()

    with pytest.raises(KeyError, match="Extractor 'nonexistent' not registered"):
        app.run(extractor="nonexistent", loader="sink")


def test_source_colon_unknown_entity_raises():
    app = _make_github_app()

    @app.loader()
    def sink(records: Records[Repo]) -> None:
        records.collect()

    with pytest.raises(KeyError, match="not found"):
        app.run(extractor="github:nonexistent", loader="sink")


def test_list_sources():
    app = _make_github_app()
    assert app.list_sources() == ["github"]


def test_get_source_entities():
    app = _make_github_app()
    entities = app.get_source_entities("github")
    assert set(entities) == {"repositories", "issues"}


def test_extractor_has_source_name():
    app = _make_github_app()
    reg = app.get_extractor("repositories")
    assert reg.source_name == "github"


def test_standalone_extractor_has_no_source_name():
    app = FastELT()

    @app.extractor()
    def standalone() -> Iterator[Repo]:
        yield Repo(name="x", stars=1)

    reg = app.get_extractor("standalone")
    assert reg.source_name is None


def test_include_source_default_name():
    """Source name defaults to class name lowercased."""
    class GitHubSource(Source):
        token: str

    gh = GitHubSource(token="abc")

    @gh.entity()
    def repos() -> Iterator[Repo]:
        yield Repo(name="x", stars=1)

    app = FastELT()
    app.include_source(gh)
    assert "githubsource" in app.list_sources()


def test_include_source_custom_name():
    github = Source(token="abc")

    @github.entity()
    def repos() -> Iterator[Repo]:
        yield Repo(name="x", stars=1)

    app = FastELT()
    app.include_source(github, name="gh")
    assert "gh" in app.list_sources()
    assert app.get_source_entities("gh") == ["repos"]
