"""Example: Custom Source with @resource decorators + Incremental.

For APIs that need custom extraction logic (pagination, transformation,
business logic), use Source + @resource decorators.

For standard REST APIs, use RESTAPISource instead (see github_rest_api.py).

Requires: pip install httpx
Usage:    GH_TOKEN=ghp_... python example/source_pipeline.py
"""

from collections.abc import Iterator
from typing import Annotated

import httpx

from fastelt import FastELT, Source
from fastelt.config import Env
from fastelt.destinations import DuckDBDestination
from fastelt.sources import Incremental

# --- Custom source with shared config ---

github = Source(
    name="github",
    base_url="https://api.github.com",
    token=Env("GH_TOKEN"),
    org="anthropics",
)


@github.resource(
    primary_key="id",
    write_disposition="merge",
    description="Fetch repositories with custom star filtering",
)
def repositories(
    updated_at: Annotated[str, Incremental(initial_value="2020-01-01")],
    min_stars: int = 0,
) -> Iterator[dict]:
    """Custom logic: filter by star count (not possible with RESTAPISource)."""
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


# --- App ---

db = DuckDBDestination(dataset_name="raw_github")

app = FastELT(pipeline_name="github_custom")
app.include_destination(db)
app.include_source(github)

if __name__ == "__main__":
    info = app.run(destination=db)
    print(f"Pipeline completed: {info}")
