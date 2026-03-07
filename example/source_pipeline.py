"""Example: Source-based pipeline — like FastAPI's APIRouter.

A GitHub source with shared config (base_url, token), exposing
multiple entities (repositories, pull_requests) that get the
source injected automatically.
"""

import json
from collections.abc import Iterator

from pydantic import BaseModel, Field

from fastelt import FastELT, Records, Source

# --- Shared source config (no class needed) ---

github = Source(
    base_url="https://api.github.com",
    token="ghp_fake_token",
    org="anthropics",
)


# --- Record schemas ---


class Repository(BaseModel):
    name: str
    stars: int
    language: str


class PullRequest(BaseModel):
    repo: str
    title: str
    author: str
    status: str


# --- Entities: just use `github` from scope, like any Python closure ---


@github.entity(
    description="Fetch repositories from a GitHub org",
    tags=["core", "github"],
    primary_key="name",
)
def repositories(min_stars: int = Field(default=0)) -> Iterator[Repository]:
    # In real code: requests.get(f"{github.base_url}/orgs/{github.org}/repos", headers=...)
    fake_data = [
        Repository(name="claude-code", stars=12000, language="TypeScript"),
        Repository(name="anthropic-sdk-python", stars=3400, language="Python"),
        Repository(name="courses", stars=800, language="Jupyter Notebook"),
    ]
    for repo in fake_data:
        if repo.stars >= min_stars:
            yield repo


@github.entity(
    description="Fetch pull requests for a repository",
    tags=["core", "github"],
    primary_key=["repo", "title"],
)
def pull_requests(
    repo: str = Field(...), state: str = Field(default="open")
) -> Iterator[PullRequest]:
    # In real code: requests.get(f"{github.base_url}/repos/{github.org}/{repo}/pulls", ...)
    fake_data = [
        PullRequest(
            repo=repo, title="Add streaming support", author="alice", status="open"
        ),
        PullRequest(
            repo=repo, title="Fix typo in README", author="bob", status="merged"
        ),
    ]
    for pr in fake_data:
        if state == "all" or pr.status == state:
            yield pr


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

    print("\n=== All pull requests for claude-code ===")
    app.run(
        extractor="pull_requests",
        loader="console",
        extractor_config={"repo": "claude-code", "state": "all"},
    )

    print("\n=== Dump repos to JSON ===")
    app.run(
        extractor="repositories",
        loader="json_file",
        loader_config={"path": "example/repos.json"},
    )
    print("Written to example/repos.json")
