"""RESTAPISource — declarative REST API extraction wrapping dlt's rest_api.

Define API endpoints as config, and fastELT + dlt handle pagination,
authentication, incremental loading, and schema inference automatically.

Usage::

    from fastelt import FastELT
    from fastelt.config import Env
    from fastelt.rest_api import RESTAPISource

    github = RESTAPISource(
        name="github",
        base_url="https://api.github.com",
        auth={
            "type": "bearer",
            "token": Env("GH_TOKEN"),
        },
        paginator="header_link",
        resources=[
            {
                "name": "repos",
                "endpoint": {
                    "path": "/orgs/{org}/repos",
                    "params": {"org": "anthropics", "per_page": 100},
                },
                "primary_key": "id",
                "write_disposition": "merge",
            },
            {
                "name": "issues",
                "endpoint": {
                    "path": "/repos/{org}/{repo}/issues",
                    "params": {
                        "org": "anthropics",
                        "repo": "anthropic-sdk-python",
                        "state": "open",
                    },
                },
                "primary_key": "id",
                "write_disposition": "append",
            },
        ],
    )

    app = FastELT(pipeline_name="github_pipeline")
    app.include_source(github)
    app.run(destination="duckdb")
"""

from __future__ import annotations

from typing import Any

import dlt
from loguru import logger

from fastelt._utils import resolve_env_values
from fastelt.types import Source


class RESTAPISource(Source):
    """Declarative REST API source wrapping ``dlt.sources.rest_api``.

    Maps directly to dlt's ``rest_api_source`` config, with fastELT's ``Env``
    support for secrets.  All dlt rest_api features are supported: pagination,
    auth, incremental loading, parent-child relationships, etc.

    Parameters
    ----------
    name:
        Source name (used as dlt source name and for referencing).
    base_url:
        Base URL for all endpoints.
    resources:
        List of resource configs.  Each is a dict matching dlt's
        ``EndpointResource`` schema.  At minimum needs ``name`` and
        ``endpoint`` (string path or dict with ``path`` + ``params``).
    headers:
        Default headers for all requests.
    auth:
        Authentication config.  Supports dlt auth types::

            # Bearer token
            {"type": "bearer", "token": Env("GH_TOKEN")}

            # API key
            {"type": "api_key", "name": "X-API-Key", "api_key": Env("KEY")}

            # HTTP Basic
            {"type": "http_basic", "username": "user", "password": Env("PASS")}

        Or a string shorthand: ``"bearer"``
    paginator:
        Default paginator for all endpoints.  String shorthand::

            "header_link"   — GitHub-style Link header
            "json_link"     — JSON response with next link
            "offset"        — offset/limit
            "page_number"   — page number
            "cursor"        — cursor-based
            "auto"          — auto-detect (default)

        Or a dict with paginator config.
    resource_defaults:
        Default config applied to all resources (e.g. default write_disposition).
    """

    name: str
    base_url: str
    resources: list[dict[str, Any]]
    headers: dict[str, str] = {}
    auth: dict[str, Any] | str | Any | None = None
    paginator: dict[str, Any] | str | None = None
    resource_defaults: dict[str, Any] | None = None

    def _build_dlt_source(
        self,
        resource_names: list[str] | None = None,
    ) -> dlt.sources.DltSource:
        """Build a dlt source from the REST API config."""
        try:
            from dlt.sources.rest_api import rest_api_source
        except ImportError as e:
            raise ImportError(
                "dlt[rest_api] is required for RESTAPISource. "
                "Install with: pip install fastelt[rest_api]"
            ) from e

        # Build dlt rest_api config — resolve Env values throughout
        client_config: dict[str, Any] = {
            "base_url": self.base_url,
        }
        if self.headers:
            client_config["headers"] = resolve_env_values(self.headers)
        if self.auth:
            client_config["auth"] = resolve_env_values(self.auth)
        if self.paginator:
            client_config["paginator"] = self.paginator

        # Filter resources if selective run
        resources_config = list(self.resources)
        if resource_names:
            resources_config = [
                r for r in resources_config if r.get("name") in resource_names
            ]

        # Resolve any Env values in resource configs
        resources_config = resolve_env_values(resources_config)

        config: dict[str, Any] = {
            "client": client_config,
            "resources": resources_config,
        }
        if self.resource_defaults:
            config["resource_defaults"] = resolve_env_values(self.resource_defaults)

        logger.debug(
            "Building REST API source '{}' with {} resources",
            self.name,
            len(resources_config),
        )

        return rest_api_source(config, name=self.name)

    def list_resources(self) -> list[str]:
        """Return resource names."""
        return [r["name"] for r in self.resources if "name" in r]
