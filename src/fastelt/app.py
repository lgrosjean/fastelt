from __future__ import annotations

from collections.abc import Callable
from typing import Any, TypeVar

from loguru import logger

from fastelt.extractor import create_extractor_registration
from fastelt.loader import create_loader_registration
from fastelt.pipeline import run_pipeline
from fastelt.types import (
    ExtractorRegistration,
    LoaderRegistration,
    PluginGroup,
    Source,
    WriteDisposition,
)

F = TypeVar("F", bound=Callable[..., Any])


class FastELT:
    def __init__(self, *, state_dir: str | None = None) -> None:
        self._extractors: dict[str, ExtractorRegistration] = {}
        self._loaders: dict[str, LoaderRegistration] = {}
        self._sources: dict[str, Source] = {}
        self._source_entities: dict[str, list[str]] = {}  # source_name -> [entity_keys]
        self._state_dir = state_dir
        logger.debug("FastELT app initialized")

    def extractor(
        self,
        name: str | None = None,
        *,
        description: str | None = None,
        tags: list[str] | None = None,
        deprecated: bool = False,
        primary_key: str | list[str] | None = None,
        write_disposition: str | WriteDisposition = WriteDisposition.APPEND,
    ) -> Callable[[F], F]:
        """Decorator to register an extractor function."""
        def decorator(func: F) -> F:
            key = name or func.__name__
            reg = create_extractor_registration(
                key,
                func,
                description=description,
                tags=tags,
                deprecated=deprecated,
                primary_key=primary_key,
                write_disposition=write_disposition,
            )
            self._extractors[key] = reg
            logger.info("Registered extractor '{}'", key)
            return func
        return decorator

    def loader(self, name: str | None = None) -> Callable[[F], F]:
        """Decorator to register a loader function."""
        def decorator(func: F) -> F:
            key = name or func.__name__
            reg = create_loader_registration(key, func)
            self._loaders[key] = reg
            logger.info("Registered loader '{}'", key)
            return func
        return decorator

    def include(self, plugin: PluginGroup) -> None:
        """Include extractors/loaders from a PluginGroup."""
        self._extractors.update(plugin.extractors)
        self._loaders.update(plugin.loaders)
        logger.info(
            "Included plugin group ({} extractors, {} loaders)",
            len(plugin.extractors),
            len(plugin.loaders),
        )

    def include_source(self, source: Source, name: str | None = None) -> None:
        """Include all entities from a Source.

        Parameters
        ----------
        source:
            The Source instance whose entities should be registered.
        name:
            Optional name for the source.  Used to reference it as
            ``source:entity`` in ``run()``.  If omitted, uses the
            Source class name (lowercased).
        """
        source_name = name or type(source).__name__.lower()
        source._source_name = source_name

        plugin = source._build_plugin_group()
        self._extractors.update(plugin.extractors)
        self._loaders.update(plugin.loaders)
        self._sources[source_name] = source
        self._source_entities[source_name] = list(plugin.extractors.keys())
        logger.info(
            "Included source '{}' ({} extractors, {} loaders)",
            source_name,
            len(plugin.extractors),
            len(plugin.loaders),
        )

    def _resolve_extractors(self, target: str) -> list[ExtractorRegistration]:
        """Resolve a target string to extractor registration(s).

        Supports three forms:
        - ``"extractor_name"`` — a single extractor
        - ``"source_name"`` — all entities from a source
        - ``"source_name:entity_name"`` — a single entity from a source
        """
        # source:entity form
        if ":" in target:
            source_name, entity_name = target.split(":", 1)
            if source_name not in self._sources:
                raise KeyError(f"Source '{source_name}' not registered")
            if entity_name not in self._extractors:
                raise KeyError(
                    f"Entity '{entity_name}' not found in source '{source_name}'"
                )
            reg = self._extractors[entity_name]
            if reg.source_name != source_name:
                raise KeyError(
                    f"Entity '{entity_name}' does not belong to source '{source_name}'"
                )
            return [reg]

        # Check if it's a source name → return all its entities
        if target in self._sources:
            entity_keys = self._source_entities.get(target, [])
            if not entity_keys:
                raise KeyError(f"Source '{target}' has no entities")
            return [self._extractors[k] for k in entity_keys]

        # Plain extractor name
        reg = self._extractors.get(target)
        if reg is None:
            raise KeyError(f"Extractor '{target}' not registered")
        return [reg]

    def run(
        self,
        extractor: str,
        loader: str,
        extractor_config: dict[str, Any] | None = None,
        loader_config: dict[str, Any] | None = None,
        *,
        validate_records: bool = True,
        state_dir: str | None = None,
    ) -> None:
        """Run a pipeline from the named extractor/source to the named loader.

        The *extractor* argument accepts three forms:

        - ``"extractor_name"`` — run a single extractor
        - ``"source_name"`` — run **all** entities from a source
        - ``"source_name:entity_name"`` — run one entity from a source
        """
        ext_regs = self._resolve_extractors(extractor)

        ldr_reg = self._loaders.get(loader)
        if ldr_reg is None:
            raise KeyError(f"Loader '{loader}' not registered")

        resolved_state_dir = state_dir or self._state_dir

        for ext_reg in ext_regs:
            logger.info("Running pipeline: '{}' -> '{}'", ext_reg.name, loader)
            run_pipeline(
                ext_reg,
                ldr_reg,
                extractor_config=extractor_config,
                loader_config=loader_config,
                validate_records=validate_records,
                state_dir=resolved_state_dir,
            )
            logger.info("Pipeline '{}' -> '{}' completed", ext_reg.name, loader)

    def list_extractors(self) -> list[str]:
        return list(self._extractors.keys())

    def list_loaders(self) -> list[str]:
        return list(self._loaders.keys())

    def list_sources(self) -> list[str]:
        return list(self._sources.keys())

    def get_extractor(self, name: str) -> ExtractorRegistration:
        return self._extractors[name]

    def get_loader(self, name: str) -> LoaderRegistration:
        return self._loaders[name]

    def get_source_entities(self, source_name: str) -> list[str]:
        """Return entity names belonging to a source."""
        if source_name not in self._sources:
            raise KeyError(f"Source '{source_name}' not registered")
        return list(self._source_entities.get(source_name, []))
