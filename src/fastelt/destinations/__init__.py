"""Destination configuration for fastELT.

A ``Destination`` is a declarative config object describing where data is loaded.
Subclass it to create typed destinations, or use the built-in ones.

Usage::

    from fastelt.destinations import BigQueryDestination

    bq = BigQueryDestination(
        project_id="my-project",
        location="EU",
        dataset_name="analytics",
        credentials=Secret("BQ_CREDENTIALS"),
    )

    app.include_destination(bq)
    app.run(destination=bq)
"""

from fastelt.destinations.base import CustomDestination, Destination
from fastelt.destinations.bigquery import BigQueryDestination
from fastelt.destinations.duckdb import DuckDBDestination
from fastelt.destinations.filesystem import FileSystemDestination

__all__ = [
    "BigQueryDestination",
    "CustomDestination",
    "Destination",
    "DuckDBDestination",
    "FileSystemDestination",
]
