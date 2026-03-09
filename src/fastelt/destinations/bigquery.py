"""BigQuery destination."""

from __future__ import annotations

from fastelt.destinations.base import Destination


class BigQueryDestination(Destination):
    """Google BigQuery destination.

    Usage::

        from fastelt.destinations import BigQueryDestination
        from fastelt.config import Secret

        bq = BigQueryDestination(
            project_id="my-project",
            location="EU",
            dataset_name="analytics",
            credentials=Secret("GOOGLE_APPLICATION_CREDENTIALS"),
        )

        app.include_destination(bq)
        app.run(destination=bq)
    """

    destination_type: str = "bigquery"
    project_id: str | None = None
    location: str = "US"
    credentials: str | None = None
