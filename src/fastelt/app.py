"""FastELT application — the top-level orchestrator wrapping dlt.

Like ``FastAPI()`` wraps Starlette, ``FastELT()`` wraps ``dlt.pipeline()``.
"""

from __future__ import annotations

from typing import Any

import dlt
from loguru import logger

from fastelt.types import Source


class FastELT:
    """Central application object — like ``FastAPI()`` but for ELT pipelines.

    Usage::

        app = FastELT(pipeline_name="my_pipeline")

        github = Source(name="github", base_url="...", token=Env("GH_TOKEN"))

        @github.resource(primary_key="id", write_disposition="merge")
        def repositories():
            yield from fetch_repos()

        app.include_source(github)
        app.run(destination="duckdb", dataset_name="raw_data")
    """

    def __init__(
        self,
        pipeline_name: str = "fastelt",
        *,
        destination: str | None = None,
        dataset_name: str | None = None,
    ) -> None:
        self._pipeline_name = pipeline_name
        self._default_destination = destination
        self._default_dataset_name = dataset_name
        self._sources: dict[str, Source] = {}
        logger.debug("FastELT app '{}' initialized", pipeline_name)

    def include_source(self, source: Source, name: str | None = None) -> None:
        """Register a source with its resources.

        Parameters
        ----------
        source:
            A ``Source`` instance (or subclass: ``RESTAPISource``,
            ``LocalFileSystemSource``, etc.).
        name:
            Optional name override.
        """
        source_name = (
            name
            or getattr(source, "name", None)
            or source._source_name
            or type(source).__name__.lower()
        )
        source._source_name = source_name
        self._sources[source_name] = source
        logger.info(
            "Included source '{}' ({} resources)",
            source_name,
            len(source.list_resources()),
        )

    def run(
        self,
        *,
        source: str | None = None,
        resources: list[str] | None = None,
        destination: str | None = None,
        dataset_name: str | None = None,
        write_disposition: str | None = None,
        **pipeline_kwargs: Any,
    ) -> Any:
        """Run the pipeline — extract from sources, load to destination via dlt.

        Parameters
        ----------
        source:
            Run only this source (by name).  If omitted, runs all sources.
        resources:
            Run only these resources within the selected source(s).
        destination:
            dlt destination (e.g. ``"duckdb"``, ``"postgres"``, ``"bigquery"``).
            Overrides the app default.
        dataset_name:
            Dataset/schema name at the destination.
        write_disposition:
            Override write disposition for all resources in this run.
        **pipeline_kwargs:
            Extra kwargs forwarded to ``dlt.pipeline()``.
        """
        dest = destination or self._default_destination
        if dest is None:
            raise ValueError(
                "No destination specified. Pass destination= to run() "
                "or set it in FastELT(destination=...)"
            )

        ds_name = dataset_name or self._default_dataset_name or f"{self._pipeline_name}_data"

        # Create dlt pipeline
        pipeline = dlt.pipeline(
            pipeline_name=self._pipeline_name,
            destination=dest,
            dataset_name=ds_name,
            **pipeline_kwargs,
        )

        # Determine which sources to run
        if source:
            if source not in self._sources:
                raise KeyError(f"Source '{source}' not registered")
            sources_to_run = {source: self._sources[source]}
        else:
            sources_to_run = self._sources

        if not sources_to_run:
            raise ValueError("No sources registered. Use app.include_source() first.")

        # Run each source
        all_info = []
        for src_name, src_obj in sources_to_run.items():
            logger.info("Running source '{}' -> '{}'", src_name, dest)
            dlt_source = src_obj._build_dlt_source(resource_names=resources)

            run_kwargs: dict[str, Any] = {}
            if write_disposition:
                run_kwargs["write_disposition"] = write_disposition

            info = pipeline.run(dlt_source, **run_kwargs)
            all_info.append(info)
            logger.info("Source '{}' completed: {}", src_name, info)

        return all_info[0] if len(all_info) == 1 else all_info

    # -- Introspection --

    def list_sources(self) -> list[str]:
        """Return registered source names."""
        return list(self._sources.keys())

    def list_resources(self, source: str | None = None) -> dict[str, list[str]]:
        """Return resources grouped by source.

        Parameters
        ----------
        source:
            If provided, only list resources for this source.
        """
        if source:
            if source not in self._sources:
                raise KeyError(f"Source '{source}' not registered")
            return {source: self._sources[source].list_resources()}
        return {
            name: src.list_resources() for name, src in self._sources.items()
        }

    def get_source(self, name: str) -> Source:
        """Get a registered source by name."""
        if name not in self._sources:
            raise KeyError(f"Source '{name}' not registered")
        return self._sources[name]
