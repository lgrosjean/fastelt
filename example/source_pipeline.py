"""Example: Source-based pipeline — like FastAPI's APIRouter.

A GitHub source with shared config (base_url, token), exposing
multiple entities (repositories, pull_requests) that get the
source injected automatically.

Requires: pip install httpx
Usage:    GH_TOKEN=ghp_... python example/source_pipeline.py
"""

import json
from collections.abc import Iterator

import httpx
from pydantic import BaseModel, Field

from fastelt import Env, FastELT, Records, Source

# --- Shared source config (no class needed) ---

github = Source(
    base_url="https://api.github.com",
    token=Env("GH_TOKEN"),
    org="anthropics",
)


# --- Record schemas ---


class Repository(BaseModel):
    name: str
    stars: int
    language: str | None


class PullRequest(BaseModel):
    repo: str
    title: str
    author: str
    state: str


# --- Entities: just use `github` from scope, like any Python closure ---


@github.entity(
    description="Fetch repositories from a GitHub org",
    tags=["core", "github"],
    primary_key="name",
)
def repositories(min_stars: int = Field(default=0)) -> Iterator[Repository]:
    headers = {"Authorization": f"Bearer {github.token}"}
    url = f"{github.base_url}/orgs/{github.org}/repos"
    page = 1
    while True:
        resp = httpx.get(url, headers=headers, params={"per_page": 100, "page": page})
        resp.raise_for_status()
        data = resp.json()
        if not data:
            break
        for item in data:
            if item.get("stargazers_count", 0) >= min_stars:
                yield {
                    "name": item["name"],
                    "stars": item["stargazers_count"],
                    "language": item.get("language"),
                }
        page += 1


@github.entity(
    description="Fetch pull requests for a repository",
    tags=["core", "github"],
    primary_key=["repo", "title"],
)
def pull_requests(
    repo: str = Field(...), state: str = Field(default="open")
) -> Iterator[PullRequest]:
    headers = {"Authorization": f"Bearer {github.token}"}
    url = f"{github.base_url}/repos/{github.org}/{repo}/pulls"
    page = 1
    while True:
        resp = httpx.get(
            url, headers=headers, params={"state": state, "per_page": 100, "page": page}
        )
        resp.raise_for_status()
        data = resp.json()
        if not data:
            break
        for item in data:
            yield {
                "repo": repo,
                "title": item["title"],
                "author": item["user"]["login"],
                "state": item["state"],
            }
        page += 1


# --- App ---

app = FastELT()
app.include_source(github)


@app.loader()
def json_file(records: Records[BaseModel], path: str = Field(...)) -> None:
    with open(path, "w") as f:
        json.dump([r.model_dump() for r in records], f, indent=2)


@app.loader()
def console(records: Records[BaseModel]) -> None:
    for r in records:
        print(f"  {r.model_dump()}")


if __name__ == "__main__":
    print("=== Repositories (min 1000 stars) ===")
    app.run(
        extractor="repositories",
        loader="console",
        extractor_config={"min_stars": 1000},
    )

    print("\n=== Open pull requests for anthropic-sdk-python ===")
    app.run(
        extractor="pull_requests",
        loader="console",
        extractor_config={"repo": "anthropic-sdk-python", "state": "open"},
    )

    print("\n=== Dump repos to JSON ===")
    app.run(
        extractor="repositories",
        loader="json_file",
        loader_config={"path": "example/repos.json"},
    )
    print("Written to example/repos.json")
