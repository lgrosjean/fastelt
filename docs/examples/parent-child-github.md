# Parent-Child Resources: GitHub API

This example shows how to build a GitHub pipeline where resources depend on each other: **users produce repos, repos produce commits**. FastELT auto-detects these dependencies from type annotations and wires them as dlt transformers.

## About this example

You will learn how to:

- Use **return type annotations** (`-> list[User]`) to declare what a resource produces
- Use **parameter type annotations** (`user: User`) to declare parent dependencies
- Chain resources across **multiple levels** (users → repos → commits)
- Combine parent-child chaining with **env resolution**
- Use **`get_resource_tree()`** to inspect the dependency graph
- Run **selective resources** that auto-include their parents

## Full example

```python
"""GitHub users → repos → commits pipeline with parent-child chaining."""

from __future__ import annotations

import httpx
from pydantic import BaseModel

from fastelt import FastELT, Source
from fastelt.config import Env, Secret
from fastelt.destinations import DuckDBDestination


# ---------------------------------------------------------------------------
# 1. Pydantic models — define the shape of each resource's output
# ---------------------------------------------------------------------------


class User(BaseModel):
    id: int
    login: str


class Repo(BaseModel):
    id: int
    name: str
    full_name: str
    owner_login: str
    stargazers_count: int = 0


class Commit(BaseModel):
    sha: str
    repo_full_name: str
    message: str
    author_login: str | None = None


# ---------------------------------------------------------------------------
# 2. Source — shared config for the GitHub API
# ---------------------------------------------------------------------------


class GitHubSource(Source):
    base_url: str = "https://api.github.com"
    token: str
    org: str

    @property
    def headers(self) -> dict[str, str]:
        return {
            "Authorization": f"Bearer {self.token}",
            "Accept": "application/vnd.github+json",
        }


github = GitHubSource(
    token=Secret("GH_TOKEN"),
    org=Env("GH_ORG", default="anthropics"),
)


# ---------------------------------------------------------------------------
# 3. Resources — chained via type annotations
# ---------------------------------------------------------------------------


@github.resource(primary_key="id", write_disposition="merge")
def users() -> list[User]:
    """Fetch organization members."""
    resp = httpx.get(
        f"{github.base_url}/orgs/{github.org}/members",
        headers=github.headers,
    )
    resp.raise_for_status()
    for u in resp.json():
        yield {"id": u["id"], "login": u["login"]}


@github.resource(primary_key="id", write_disposition="merge")
def repos(user: User) -> list[Repo]:
    """Fetch repos for each user. Automatically receives User items from `users`."""
    resp = httpx.get(
        f"{github.base_url}/users/{user.login}/repos",
        headers=github.headers,
        params={"per_page": 100, "sort": "updated"},
    )
    resp.raise_for_status()
    for r in resp.json():
        yield {
            "id": r["id"],
            "name": r["name"],
            "full_name": r["full_name"],
            "owner_login": user.login,
            "stargazers_count": r.get("stargazers_count", 0),
        }


@github.resource(primary_key="sha", write_disposition="append")
def commits(repo: Repo) -> list[Commit]:
    """Fetch recent commits for each repo. Automatically receives Repo items from `repos`."""
    resp = httpx.get(
        f"{github.base_url}/repos/{repo.full_name}/commits",
        headers=github.headers,
        params={"per_page": 30},
    )
    resp.raise_for_status()
    for c in resp.json():
        yield {
            "sha": c["sha"],
            "repo_full_name": repo.full_name,
            "message": c["commit"]["message"],
            "author_login": (c.get("author") or {}).get("login"),
        }


# ---------------------------------------------------------------------------
# 4. App — wire everything together
# ---------------------------------------------------------------------------


db = DuckDBDestination(database="github.duckdb")

app = FastELT(pipeline_name="github_pipeline")
app.include_destination(db)
app.include_source(github)

if __name__ == "__main__":
    # Inspect the dependency graph
    print(github.get_resource_tree())
    # {'users': ['repos'], 'repos': ['commits']}

    app.run(destination=db)
```

## How it works

### The dependency chain

```
users() -> list[User]       ← root resource (dlt.resource)
    │
    ▼
repos(user: User)           ← child of users (dlt.transformer)
    │
    ▼
commits(repo: Repo)         ← child of repos (dlt.transformer)
```

FastELT builds this chain automatically at `_build_dlt_source()` time:

1. **`users`** has return type `-> list[User]` → registered as the producer of `User`
2. **`repos`** has parameter `user: User` → `User` is found in the type registry → `repos` becomes a transformer consuming from `users`
3. **`commits`** has parameter `repo: Repo` → same process, wired to `repos`

### What the child function receives

Each item from the parent is **validated through the Pydantic model** before being passed to the child. Your function receives a proper model instance:

```python
@github.resource(primary_key="id")
def repos(user: User) -> list[Repo]:
    # user is a User instance — use dot access
    print(user.id)      # 12345
    print(user.login)   # "alice"
    ...
```

If the parent yields `{"id": "not_an_int", "login": "alice"}`, Pydantic will raise a `ValidationError` before the child ever sees the data.

> **Note:** The source config (`github`) is accessed via closure here. If you prefer explicit injection (e.g., for testability), you can add a `source: GitHubSource` parameter — see the [source injection docs](../guide/sources.md#source-injection).

### Auto `response_model`

The return annotation `-> list[User]` automatically sets `response_model=User` on the resource. This means output records are also validated through the model. You don't need to specify `response_model` separately:

```python
# These are equivalent:
@source.resource(primary_key="id")
def users() -> list[User]:
    ...

@source.resource(primary_key="id", response_model=User)
def users():
    ...
```

If you need a different validation model for the output (e.g., stricter than the type used for chaining), pass an explicit `response_model` — it takes precedence.

## Selective runs

When you run a child resource selectively, its parents are auto-included:

```python
# Only load repos (users runs automatically as the parent)
app.run(destination=db, resources=["repos"])

# Only load commits (repos and users both run automatically)
app.run(destination=db, resources=["commits"])
```

## Introspection

Use the built-in helpers to inspect the dependency graph:

```python
# Direct children of a resource
github.get_children("users")
# ["repos"]

github.get_children("repos")
# ["commits"]

github.get_children("commits")
# []

# Full tree
github.get_resource_tree()
# {"users": ["repos"], "repos": ["commits"]}
```

## Simpler variant (without API calls)

For testing or local development, here's a self-contained version that doesn't require network access:

```python
from pydantic import BaseModel
from fastelt import FastELT, Source
from fastelt.destinations import DuckDBDestination


class User(BaseModel):
    id: int
    name: str


class Repo(BaseModel):
    id: int
    user_id: int
    name: str


class Commit(BaseModel):
    id: int
    repo_id: int
    message: str


github = Source(name="github")


@github.resource(primary_key="id")
def users() -> list[User]:
    yield {"id": 1, "name": "Alice"}
    yield {"id": 2, "name": "Bob"}


@github.resource(primary_key="id")
def repos(user: User) -> list[Repo]:
    yield {"id": 100 + user.id, "user_id": user.id, "name": f"repo-{user.name}"}


@github.resource(primary_key="id")
def commits(repo: Repo) -> list[Commit]:
    yield {"id": 1000 + repo.id, "repo_id": repo.id, "message": f"init {repo.name}"}


db = DuckDBDestination(database="demo.duckdb")
app = FastELT(pipeline_name="demo")
app.include_destination(db)
app.include_source(github)

if __name__ == "__main__":
    print(github.get_resource_tree())
    # {"users": ["repos"], "repos": ["commits"]}
    app.run(destination=db)
```

After running, query with DuckDB:

```sql
SELECT c.id, c.message, r.name as repo_name, u.name as user_name
FROM demo_data.commits c
JOIN demo_data.repos r ON c.repo_id = r.id
JOIN demo_data.users u ON r.user_id = u.id;
```

| id   | message         | repo_name  | user_name |
|------|-----------------|------------|-----------|
| 1101 | init repo-Alice | repo-Alice | Alice     |
| 1102 | init repo-Bob   | repo-Bob   | Bob       |

## Rules to remember

1. **One producer per type** — two resources returning `-> list[User]` raises `ValueError`
2. **One parent per child** — a resource can only depend on one parent type
3. **Type matching is exact** — `User` and `UserDetail` are different types, even if structurally identical
4. **Decorator order doesn't matter** — dependencies resolve at build time, not registration time
5. **All features compose** — combine parent-child with `Env`, `Secret`, `Incremental`, source injection, and `response_model`
