"""RESTAPISource — declarative REST API extraction wrapping dlt's rest_api.

Define API endpoints as config, and fastELT + dlt handle pagination,
authentication, incremental loading, and schema inference automatically.

Usage::

    from fastelt import FastELT
    from fastelt.rest_api import RESTAPISource

    github_api = RESTAPISource(
        name="github",
        base_url="https://api.github.com",
        headers={"Authorization": f"Bearer {token}"},
        resources=[
            {
                "name": "repos",
                "endpoint": "/orgs/anthropics/repos",
                "primary_key": "id",
                "write_disposition": "merge",
            },
            {
                "name": "issues",
                "endpoint": "/repos/anthropics/fastelt/issues",
                "primary_key": "id",
            },
        ],
    )

    app = FastELT(pipeline_name="github")
    app.include_rest_api(github_api)
    app.run(destination="duckdb")
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import dlt
from loguru import logger


@dataclass
class RESTAPISource:
    """Declarative REST API source wrapping ``dlt.sources.rest_api``.

    Parameters
    ----------
    name:
        Source name (used as dlt source name and for referencing).
    base_url:
        Base URL for all endpoints.
    resources:
        List of resource configs. Each is a dict with at minimum
        ``name`` and ``endpoint``.  Supports all dlt rest_api
        resource config fields.
    headers:
        Default headers for all requests.
    auth:
        Authentication config (dlt auth dict or object).
    paginator:
        Default paginator config for all endpoints.
    """

    name: str
    base_url: str
    resources: list[dict[str, Any]]
    headers: dict[str, str] = field(default_factory=dict)
    auth: dict[str, Any] | Any | None = None
    paginator: dict[str, Any] | str | None = None

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

        # Build dlt rest_api config
        client_config: dict[str, Any] = {
            "base_url": self.base_url,
        }
        if self.headers:
            client_config["headers"] = self.headers
        if self.auth:
            client_config["auth"] = self.auth
        if self.paginator:
            client_config["paginator"] = self.paginator

        # Filter resources if selective run
        resources_config = self.resources
        if resource_names:
            resources_config = [
                r for r in resources_config if r.get("name") in resource_names
            ]

        config: dict[str, Any] = {
            "client": client_config,
            "resources": resources_config,
        }

        logger.debug("Building REST API source '{}' with {} resources",
                      self.name, len(resources_config))

        return rest_api_source(config, name=self.name)

    def list_resources(self) -> list[str]:
        """Return resource names."""
        return [r["name"] for r in self.resources if "name" in r]
