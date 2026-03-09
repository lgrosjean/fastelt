"""DuckDB destination."""

from __future__ import annotations

from fastelt.destinations.base import Destination


class DuckDBDestination(Destination):
    """DuckDB destination (local file-based analytics database).

    Usage::

        from fastelt.destinations import DuckDBDestination

        db = DuckDBDestination(database="my_pipeline.duckdb")

        app.include_destination(db)
        app.run(destination=db)
    """

    destination_type: str = "duckdb"
    database: str | None = None
