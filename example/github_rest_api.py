"""Example: GitHub REST API source — declarative, zero-code extraction.

Uses RESTAPISource to define GitHub API endpoints as config.
dlt handles pagination, incremental loading, and schema inference.

Usage:  GH_TOKEN=ghp_... python example/github_rest_api.py
"""

from fastelt import FastELT
from fastelt.config import Env
from fastelt.destinations import DuckDBDestination
from fastelt.rest_api import RESTAPISource

# --- GitHub API source (declarative — no code needed) ---

github = RESTAPISource(
    name="github",
    base_url="https://api.github.com",
    auth={
        "type": "bearer",
        "token": Env("GH_TOKEN"),
    },
    # GitHub uses Link header pagination
    paginator="header_link",
    # Default config for all resources
    resource_defaults={
        "primary_key": "id",
        "write_disposition": "merge",
    },
    resources=[
        # --- Repositories ---
        {
            "name": "repos",
            "endpoint": {
                "path": "/orgs/{org}/repos",
                "params": {
                    "org": "anthropics",
                    "per_page": 100,
                    "sort": "updated",
                },
            },
        },
        # --- Issues (with incremental loading) ---
        {
            "name": "issues",
            "endpoint": {
                "path": "/repos/{org}/{repo}/issues",
                "params": {
                    "org": "anthropics",
                    "repo": "anthropic-sdk-python",
                    "state": "all",
                    "per_page": 100,
                },
                "incremental": {
                    "start_param": "since",
                    "cursor_path": "updated_at",
                    "initial_value": "2024-01-01T00:00:00Z",
                },
            },
            "write_disposition": "merge",
        },
        # --- Pull requests ---
        {
            "name": "pull_requests",
            "endpoint": {
                "path": "/repos/{org}/{repo}/pulls",
                "params": {
                    "org": "anthropics",
                    "repo": "anthropic-sdk-python",
                    "state": "all",
                    "per_page": 100,
                },
            },
            "write_disposition": "append",
        },
        # --- Stargazers (child of repos) ---
        {
            "name": "stargazers",
            "endpoint": {
                "path": "/repos/{org}/{repo}/stargazers",
                "params": {
                    "org": "anthropics",
                    "repo": "anthropic-sdk-python",
                    "per_page": 100,
                },
            },
            "write_disposition": "replace",
        },
    ],
)

# --- App ---

db = DuckDBDestination(dataset_name="raw_github")

app = FastELT(pipeline_name="github_pipeline")
app.include_destination(db)
app.include_source(github)

if __name__ == "__main__":
    # Run all resources
    info = app.run(destination=db)
    print(f"Pipeline completed: {info}")

    # Or run just repos:
    # info = app.run(destination=db, resources=["repos"])
