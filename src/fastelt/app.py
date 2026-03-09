"""FastELT application — the top-level orchestrator wrapping dlt.

Like ``FastAPI()`` wraps Starlette, ``FastELT()`` wraps ``dlt.pipeline()``.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

import dlt
from loguru import logger
from pydantic import BaseModel

from fastelt.destinations import CustomDestination, Destination, FileSystemDestination
from fastelt.types import Source


class FastELT:
    """Central application object — like ``FastAPI()`` but for ELT pipelines.

    Usage::

        app = FastELT(pipeline_name="my_pipeline")

        # Register destinations
        app.include_destination(Destination(name="lake", destination_type="duckdb"))

        # Register sources
        @app.source("users", primary_key="id")
        def users():
            yield {"id": 1, "name": "Alice"}

        # Run: destination required, source optional
        app.run(destination="lake")
        app.run(destination="lake", source="users")
    """

    def __init__(
        self,
        pipeline_name: str = "fastelt",
        destination: Destination | str | None = None,
    ) -> None:
        self._pipeline_name = pipeline_name
        self._sources: dict[str, Source] = {}
        self._destinations: dict[str, Destination] = {}
        self._default_destination: Destination | str = destination or FileSystemDestination()
        logger.debug("FastELT app '{}' initialized", pipeline_name)

    def source(
        self,
        name: str | None = None,
        *,
        primary_key: str | list[str] | None = None,
        write_disposition: str = "append",
        merge_key: str | list[str] | None = None,
        table_name: str | None = None,
        response_model: type[BaseModel] | None = None,
        frozen: bool = False,
    ) -> Callable[..., Any]:
        """Register a resource as its own source — like ``@app.get`` in FastAPI.

        Quick way to add a single-resource source without creating a ``Source``
        object first.  For multi-resource sources, use ``Source`` +
        ``@source.resource()`` + ``app.include_source()``.
        """

        def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
            source_name = name or func.__name__
            src = Source()
            src.resource(
                name=source_name,
                primary_key=primary_key,
                write_disposition=write_disposition,
                merge_key=merge_key,
                table_name=table_name,
                response_model=response_model,
                frozen=frozen,
            )(func)
            self.include_source(src, name=source_name)
            return func

        return decorator

    def include_source(self, source: Source, name: str | None = None) -> None:
        """Register a source with its resources."""
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

    def destination(
        self,
        *,
        batch_size: int = 10,
        loader_file_format: str | None = None,
        naming_convention: str = "direct",
        skip_dlt_columns_and_tables: bool = True,
        max_table_nesting: int = 0,
        dataset_name: str | None = None,
    ) -> Callable[..., Any]:
        """Register a custom sink function as a destination — like ``@app.get`` in FastAPI.

        Wraps ``@dlt.destination`` with decorator DX. The function name becomes
        the destination name.

        Usage::

            @app.destination(batch_size=100)
            def my_sink(items, table):
                for item in items:
                    print(f"{table['name']}: {item}")

            app.run(destination=my_sink)

        Parameters
        ----------
        batch_size:
            Number of items per call. Set to 0 to receive file paths instead.
        loader_file_format:
            Format of load files (``"typed-jsonl"``, ``"parquet"``, etc.).
        naming_convention:
            How to normalize table/column names.
        skip_dlt_columns_and_tables:
            Exclude internal dlt tables/columns.
        max_table_nesting:
            How deep to flatten nested fields (0 = no nesting).
        dataset_name:
            Dataset/schema name at the destination.
        """

        def decorator(func: Callable[..., Any]) -> CustomDestination:
            dest = CustomDestination(
                _func=func,
                batch_size=batch_size,
                loader_file_format=loader_file_format,
                naming_convention=naming_convention,
                skip_dlt_columns_and_tables=skip_dlt_columns_and_tables,
                max_table_nesting=max_table_nesting,
                dataset_name=dataset_name,
            )
            self.include_destination(dest)
            return dest

        return decorator

    def include_destination(self, destination: Destination) -> None:
        """Register a destination."""
        self._destinations[destination.name] = destination
        logger.info(
            "Included destination '{}' ({})",
            destination.name,
            destination.destination_type,
        )

    def _resolve_destination(self, destination: Destination | str) -> Destination:
        """Resolve a destination argument to a Destination object."""
        if isinstance(destination, Destination):
            # Auto-register if not already registered
            if destination.name not in self._destinations:
                self.include_destination(destination)
            return destination

        if destination not in self._destinations:
            raise KeyError(
                f"Destination '{destination}' not registered. "
                f"Available: {list(self._destinations.keys())}"
            )
        return self._destinations[destination]

    def run(
        self,
        *,
        destination: Destination | str | None = None,
        source: str | None = None,
        resources: list[str] | None = None,
        dataset_name: str | None = None,
        write_disposition: str | None = None,
        **pipeline_kwargs: Any,
    ) -> Any:
        """Run the pipeline — extract from sources, load to destination via dlt.

        Parameters
        ----------
        destination:
            A ``Destination`` object or the name of a registered destination.
        source:
            Run only this source (by name). If omitted, runs all sources.
        resources:
            Run only these resources within the selected source(s).
        dataset_name:
            Dataset/schema name override (otherwise uses destination's setting).
        write_disposition:
            Override write disposition for all resources in this run.
        **pipeline_kwargs:
            Extra kwargs forwarded to ``dlt.pipeline()``.
        """
        dest_obj = self._resolve_destination(destination or self._default_destination)
        dest_name = dest_obj.name
        dlt_kwargs = dest_obj._to_dlt_kwargs()

        loader_file_format = dlt_kwargs.pop("loader_file_format", None)

        # dataset_name: explicit arg > destination config > default
        ds_name = (
            dataset_name
            or dlt_kwargs.pop("dataset_name", None)
            or f"{self._pipeline_name}_data"
        )

        # Unique pipeline name per destination for state isolation
        if len(self._destinations) > 1:
            pipe_name = f"{self._pipeline_name}__{dest_name}"
        else:
            pipe_name = self._pipeline_name

        pipeline = dlt.pipeline(
            pipeline_name=pipe_name,
            dataset_name=ds_name,
            **dlt_kwargs,
            **pipeline_kwargs,
        )

        # Determine which sources to run
        if source:
            if source not in self._sources:
                raise KeyError(
                    f"Source '{source}' not registered. "
                    f"Available: {list(self._sources.keys())}"
                )
            sources_to_run = {source: self._sources[source]}
        else:
            sources_to_run = self._sources

        if not sources_to_run:
            raise ValueError("No sources registered. Use app.include_source() first.")

        # Run each source
        all_info = []
        for src_name, src_obj in sources_to_run.items():
            logger.info("Running source '{}' -> '{}'", src_name, dest_name)
            dlt_source = src_obj._build_dlt_source(resource_names=resources)

            run_kwargs: dict[str, Any] = {}
            if write_disposition:
                run_kwargs["write_disposition"] = write_disposition
            if loader_file_format:
                run_kwargs["loader_file_format"] = loader_file_format

            info = pipeline.run(dlt_source, **run_kwargs)
            all_info.append(info)
            logger.info("Source '{}' completed: {}", src_name, info)

        return all_info[0] if len(all_info) == 1 else all_info

    # -- Introspection --

    def list_sources(self) -> list[str]:
        """Return registered source names."""
        return list(self._sources.keys())

    def list_destinations(self) -> list[str]:
        """Return registered destination names."""
        return list(self._destinations.keys())

    def list_resources(self, source: str | None = None) -> dict[str, list[str]]:
        """Return resources grouped by source."""
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

    def get_destination(self, name: str) -> Destination:
        """Get a registered destination by name."""
        if name not in self._destinations:
            raise KeyError(f"Destination '{name}' not registered")
        return self._destinations[name]
