"""Example: GitHub source with 2 resources → BigQuery.

Demonstrates:
- Source with shared config (token, org)
- Two resources: repositories and issues
- BigQueryDestination with project/location
- Incremental loading on issues

Usage:
    GH_TOKEN=ghp_... \
    GOOGLE_APPLICATION_CREDENTIALS=/path/to/sa.json \
    python example/github_bigquery.py
"""

from collections.abc import Iterator
from typing import Annotated

import httpx

from fastelt import FastELT, Source
from fastelt.config import Env, Secret
from fastelt.destinations import BigQueryDestination
from fastelt.sources import Incremental

# --- Source ---

github = Source(
    name="github",
    base_url="https://api.github.com",
    token=Env("GH_TOKEN"),
    org="anthropics",
)


@github.resource(primary_key="id", write_disposition="merge")
def repositories(github: Source) -> Iterator[dict]:
    """Fetch all repositories for the org."""
    headers = {"Authorization": f"Bearer {github.token}"}
    page = 1
    while True:
        resp = httpx.get(
            f"{github.base_url}/orgs/{github.org}/repos",
            headers=headers,
            params={"per_page": 100, "page": page, "sort": "updated"},
        )
        resp.raise_for_status()
        data = resp.json()
        if not data:
            break
        yield from data
        page += 1


@github.resource(primary_key="id", write_disposition="merge")
def issues(
    github: Source,
    updated_at: Annotated[str, Incremental(initial_value="2024-01-01T00:00:00Z")],
) -> Iterator[dict]:
    """Fetch issues incrementally by updated_at."""
    headers = {"Authorization": f"Bearer {github.token}"}
    page = 1
    while True:
        resp = httpx.get(
            f"{github.base_url}/repos/{github.org}/anthropic-sdk-python/issues",
            headers=headers,
            params={
                "state": "all",
                "per_page": 100,
                "page": page,
                "since": updated_at.last_value,
            },
        )
        resp.raise_for_status()
        data = resp.json()
        if not data:
            break
        yield from data
        page += 1


# --- Destination ---

bq = BigQueryDestination(
    project_id="my-gcp-project",
    location="EU",
    dataset_name="raw_github",
    credentials=Secret("GOOGLE_APPLICATION_CREDENTIALS"),
)

# --- App ---

app = FastELT(pipeline_name="github_to_bq")
app.include_source(github)
app.include_destination(bq)

if __name__ == "__main__":
    info = app.run(destination=bq)
    print(f"Pipeline completed: {info}")
