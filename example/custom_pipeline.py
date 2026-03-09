"""Example: Custom resource with dlt incremental loading.

Demonstrates:
- Source with closure-based config access
- dlt.sources.incremental for cursor tracking
- merge write disposition for upserts
"""

from collections.abc import Iterator

import dlt

from fastelt import FastELT, Source

# --- Source with config ---
api = Source(name="myapi", base_url="https://api.example.com")


@api.resource(primary_key="id", write_disposition="merge")
def events(
    updated_at=dlt.sources.incremental("updated_at", initial_value="2024-01-01"),
) -> Iterator[dict]:
    """Fetch events, incrementally by updated_at."""
    # In real life, use updated_at.last_value to filter API calls
    print(f"Fetching events since {updated_at.last_value}")
    yield {"id": 1, "name": "signup", "updated_at": "2024-06-15T10:00:00"}
    yield {"id": 2, "name": "purchase", "updated_at": "2024-07-20T14:30:00"}


@api.resource(primary_key="id", write_disposition="append")
def logs() -> Iterator[dict]:
    """Append-only log stream."""
    yield {"id": 100, "level": "info", "message": "Server started"}
    yield {"id": 101, "level": "error", "message": "Connection timeout"}


# --- App ---
app = FastELT(pipeline_name="custom_pipeline", destination="duckdb")
app.include_source(api)

if __name__ == "__main__":
    # Run all resources
    info = app.run()
    print(f"Pipeline completed: {info}")

    # Run again — incremental will track cursor
    info = app.run()
    print(f"Second run: {info}")
