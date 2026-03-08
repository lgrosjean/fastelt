"""Tests for Source — config container and resource registration."""

from __future__ import annotations

import os
from typing import Iterator

import pytest

from fastelt import Env, Source


class GitHubSource(Source):
    base_url: str = "https://api.github.com"
    token: str
    org: str = "test"


def test_source_programmatic():
    """Source(...) with dynamic fields."""
    src = Source(name="gh", base_url="https://api.github.com", token="abc")
    assert src.base_url == "https://api.github.com"
    assert src.token == "abc"
    assert src.name == "gh"


def test_source_subclass():
    """Source subclass with typed fields."""
    gh = GitHubSource(token="abc", org="anthropics")
    assert gh.base_url == "https://api.github.com"
    assert gh.token == "abc"
    assert gh.org == "anthropics"


def test_source_env_resolution(monkeypatch):
    """Env values are resolved at Source construction time."""
    monkeypatch.setenv("TEST_TOKEN", "secret123")
    src = Source(name="test", token=Env("TEST_TOKEN"))
    assert src.token == "secret123"


def test_source_env_missing_raises():
    """Missing env var without default raises EnvironmentError."""
    with pytest.raises(EnvironmentError, match="NOT_SET_VAR"):
        Source(name="test", token=Env("NOT_SET_VAR"))


def test_source_env_default():
    """Env with default uses default when var is missing."""
    src = Source(name="test", token=Env("MISSING_VAR", default="fallback"))
    assert src.token == "fallback"


def test_source_resource_decorator():
    """@source.resource() registers a resource."""
    src = Source(name="test", base_url="https://example.com")

    @src.resource(primary_key="id", write_disposition="merge")
    def items():
        yield {"id": 1, "name": "a"}

    assert "items" in src.list_resources()
    meta = src.get_resource_meta("items")
    assert meta.primary_key == "id"
    assert meta.write_disposition == "merge"


def test_source_resource_custom_name():
    """Resource can have a custom name."""
    src = Source(name="test")

    @src.resource("custom_name")
    def items():
        yield {"id": 1}

    assert "custom_name" in src.list_resources()


def test_source_multiple_resources():
    """A source can have multiple resources."""
    src = Source(name="test")

    @src.resource()
    def repos():
        yield {"id": 1}

    @src.resource()
    def issues():
        yield {"id": 10}

    assert set(src.list_resources()) == {"repos", "issues"}


def test_source_resource_metadata():
    """Resource decorator passes metadata."""
    src = Source(name="test")

    @src.resource(
        description="Fetch repos",
        tags=["github", "core"],
        deprecated=True,
        primary_key="id",
        write_disposition="merge",
        merge_key="repo_id",
    )
    def repos():
        yield {"id": 1}

    meta = src.get_resource_meta("repos")
    assert meta.description == "Fetch repos"
    assert meta.tags == ["github", "core"]
    assert meta.deprecated is True
    assert meta.primary_key == "id"
    assert meta.write_disposition == "merge"
    assert meta.merge_key == "repo_id"


def test_source_resource_default_write_disposition():
    src = Source(name="test")

    @src.resource()
    def items():
        yield {"id": 1}

    meta = src.get_resource_meta("items")
    assert meta.write_disposition == "append"


def test_source_bind_source_explicit_type():
    """Source injection via explicit type annotation."""
    gh = GitHubSource(token="abc", org="anthropics")

    @gh.resource()
    def repos(source: GitHubSource):
        yield {"id": 1, "org": source.org}

    # _bind_source should inject the source
    bound = gh._bind_source(repos)
    records = list(bound())
    assert records[0]["org"] == "anthropics"


def test_source_bind_source_convention():
    """Source injection via convention (no annotation, no default)."""
    gh = GitHubSource(token="abc", org="test_org")

    @gh.resource()
    def repos(source):
        yield {"id": 1, "org": source.org}

    bound = gh._bind_source(repos)
    records = list(bound())
    assert records[0]["org"] == "test_org"


def test_source_closure_access():
    """Resources can access source via closure (no injection needed)."""
    gh = Source(name="test", org="closure_org")

    @gh.resource()
    def repos():
        yield {"id": 1, "org": gh.org}

    records = list(repos())
    assert records[0]["org"] == "closure_org"
