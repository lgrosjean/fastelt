"""Tests for Source — config container and resource registration."""

from __future__ import annotations

import os
from typing import Annotated, Iterator

import pytest

from fastelt import Env, Secret, Source


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


# -- Secret type --


def test_secret_resolves_env(monkeypatch):
    """Secret resolves like Env."""
    monkeypatch.setenv("MY_SECRET", "s3cret")
    s = Secret("MY_SECRET")
    assert s.resolve() == "s3cret"


def test_secret_masked_repr():
    """Secret repr does not show default value."""
    s = Secret("API_KEY", default="fallback")
    assert repr(s) == "Secret('API_KEY')"
    assert "fallback" not in repr(s)


def test_secret_is_env():
    """Secret is a subclass of Env."""
    assert issubclass(Secret, Env)
    s = Secret("X")
    assert isinstance(s, Env)


def test_secret_in_source(monkeypatch):
    """Secret works as a Source field value."""
    monkeypatch.setenv("TOKEN", "abc123")
    src = Source(name="test", token=Secret("TOKEN"))
    assert src.token == "abc123"


# -- Annotated[str, Env(...)] parameter injection --


def test_annotated_env_in_resource(monkeypatch):
    """Annotated[str, Env(...)] is resolved in resource functions."""
    monkeypatch.setenv("API_TOKEN", "resolved_token")
    src = Source(name="test")

    @src.resource()
    def items(token: Annotated[str, Env("API_TOKEN")]):
        yield {"id": 1, "token": token}

    # Build and extract the dlt source to trigger resolution
    dlt_source = src._build_dlt_source()
    records = list(list(dlt_source.resources.values())[0])
    assert records[0]["token"] == "resolved_token"


def test_annotated_secret_in_resource(monkeypatch):
    """Annotated[str, Secret(...)] is resolved in resource functions."""
    monkeypatch.setenv("DB_PASS", "hunter2")
    src = Source(name="test")

    @src.resource()
    def items(password: Annotated[str, Secret("DB_PASS")]):
        yield {"id": 1, "pw": password}

    dlt_source = src._build_dlt_source()
    records = list(list(dlt_source.resources.values())[0])
    assert records[0]["pw"] == "hunter2"


def test_annotated_env_missing_raises():
    """Missing env var in Annotated raises EnvironmentError at resource build time."""
    src = Source(name="test")

    @src.resource()
    def items(token: Annotated[str, Env("DEFINITELY_NOT_SET_12345")]):
        yield {"id": 1}

    with pytest.raises(EnvironmentError, match="DEFINITELY_NOT_SET_12345"):
        dlt_source = src._build_dlt_source()
        list(list(dlt_source.resources.values())[0])


def test_annotated_env_with_default():
    """Annotated[str, Env("X", default="y")] uses default."""
    src = Source(name="test")

    @src.resource()
    def items(val: Annotated[str, Env("NOPE_NOT_SET", default="fallback")]):
        yield {"id": 1, "val": val}

    dlt_source = src._build_dlt_source()
    records = list(list(dlt_source.resources.values())[0])
    assert records[0]["val"] == "fallback"


# -- Plain str param → auto env var resolution --


def test_str_param_resolves_from_env(monkeypatch):
    """Plain `str` param auto-resolves from UPPERCASED env var."""
    monkeypatch.setenv("GH_TOKEN", "from_env")
    src = Source(name="test")

    @src.resource()
    def repos(gh_token: str):
        yield {"id": 1, "token": gh_token}

    dlt_source = src._build_dlt_source()
    records = list(list(dlt_source.resources.values())[0])
    assert records[0]["token"] == "from_env"


def test_str_param_with_default_uses_env(monkeypatch):
    """When env var is set, it takes priority over default."""
    monkeypatch.setenv("API_KEY", "env_value")
    src = Source(name="test")

    @src.resource()
    def items(api_key: str = "default_key"):
        yield {"id": 1, "key": api_key}

    dlt_source = src._build_dlt_source()
    records = list(list(dlt_source.resources.values())[0])
    assert records[0]["key"] == "env_value"


def test_str_param_with_default_falls_back():
    """When env var is missing, falls back to default value."""
    src = Source(name="test")

    @src.resource()
    def items(api_key: str = "fallback"):
        yield {"id": 1, "key": api_key}

    dlt_source = src._build_dlt_source()
    records = list(list(dlt_source.resources.values())[0])
    assert records[0]["key"] == "fallback"


def test_str_param_no_default_missing_raises():
    """Plain `str` param without default raises when env var is missing."""
    src = Source(name="test")

    @src.resource()
    def items(missing_var: str):
        yield {"id": 1}

    with pytest.raises(EnvironmentError, match="MISSING_VAR"):
        dlt_source = src._build_dlt_source()
        list(list(dlt_source.resources.values())[0])


def test_non_str_params_left_alone():
    """Non-str params (int, incremental, etc.) are NOT resolved from env."""
    src = Source(name="test")

    @src.resource()
    def items(limit: int = 10):
        yield {"id": 1, "limit": limit}

    # Should work without any env var — `int` params are passed through
    dlt_source = src._build_dlt_source()
    records = list(list(dlt_source.resources.values())[0])
    assert records[0]["limit"] == 10
