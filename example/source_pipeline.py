"""Example: Source-based pipeline — like FastAPI's APIRouter wrapping dlt.

A GitHub source with shared config (base_url, token), exposing
multiple resources that get auto-loaded into DuckDB via dlt.

Requires: pip install httpx
Usage:    GH_TOKEN=ghp_... python example/source_pipeline.py
"""

from collections.abc import Iterator

import dlt
import httpx

from fastelt import Env, FastELT, Source

# --- Shared source config (no class needed) ---

github = Source(
    name="github",
    base_url="https://api.github.com",
    token=Env("GH_TOKEN"),
    org="anthropics",
)


# --- Resources: just use `github` from scope, like any Python closure ---


@github.resource(
    description="Fetch repositories from a GitHub org",
    tags=["core", "github"],
    primary_key="id",
    write_disposition="merge",
)
def repositories(
    updated_at=dlt.sources.incremental("updated_at", initial_value="2020-01-01"),
    min_stars: int = 0,
) -> Iterator[dict]:
    headers = {"Authorization": f"Bearer {github.token}"}
    url = f"{github.base_url}/orgs/{github.org}/repos"
    page = 1
    while True:
        resp = httpx.get(
            url,
            headers=headers,
            params={"per_page": 100, "page": page, "sort": "updated"},
        )
        resp.raise_for_status()
        data = resp.json()
        if not data:
            break
        for item in data:
            if item.get("stargazers_count", 0) >= min_stars:
                yield item
        page += 1


@github.resource(
    description="Fetch pull requests for a repository",
    tags=["core", "github"],
    primary_key="id",
    write_disposition="append",
)
def pull_requests(
    repo: str = "anthropic-sdk-python",
    state: str = "open",
) -> Iterator[dict]:
    headers = {"Authorization": f"Bearer {github.token}"}
    url = f"{github.base_url}/repos/{github.org}/{repo}/pulls"
    page = 1
    while True:
        resp = httpx.get(
            url,
            headers=headers,
            params={"state": state, "per_page": 100, "page": page},
        )
        resp.raise_for_status()
        data = resp.json()
        if not data:
            break
        yield from data
        page += 1


# --- App: wire source to dlt pipeline ---

app = FastELT(
    pipeline_name="github_pipeline",
    destination="duckdb",
    dataset_name="raw_github",
)
app.include_source(github)


if __name__ == "__main__":
    # Run all resources to DuckDB
    info = app.run()
    print(f"Pipeline completed: {info}")

    # Or run just one resource:
    # info = app.run(resources=["repositories"])
