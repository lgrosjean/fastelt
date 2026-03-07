from __future__ import annotations

from collections.abc import Callable
from typing import Any, TypeVar

from loguru import logger

from fastelt.extractor import create_extractor_registration
from fastelt.loader import create_loader_registration
from fastelt.pipeline import run_pipeline
from fastelt.types import ExtractorRegistration, LoaderRegistration, PluginGroup, Source

F = TypeVar("F", bound=Callable[..., Any])


class FastELT:
    def __init__(self) -> None:
        self._extractors: dict[str, ExtractorRegistration] = {}
        self._loaders: dict[str, LoaderRegistration] = {}
        logger.debug("FastELT app initialized")

    def extractor(
        self,
        name: str | None = None,
        *,
        description: str | None = None,
        tags: list[str] | None = None,
        deprecated: bool = False,
        primary_key: str | list[str] | None = None,
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

    def include_source(self, source: Source) -> None:
        """Include all entities from a Source."""
        plugin = source._build_plugin_group()
        self._extractors.update(plugin.extractors)
        self._loaders.update(plugin.loaders)
        logger.info(
            "Included source ({} extractors, {} loaders)",
            len(plugin.extractors),
            len(plugin.loaders),
        )

    def run(
        self,
        extractor: str,
        loader: str,
        extractor_config: dict[str, Any] | None = None,
        loader_config: dict[str, Any] | None = None,
        *,
        validate_records: bool = True,
    ) -> None:
        """Run a pipeline from the named extractor to the named loader."""
        ext_reg = self._extractors.get(extractor)
        if ext_reg is None:
            raise KeyError(f"Extractor '{extractor}' not registered")

        ldr_reg = self._loaders.get(loader)
        if ldr_reg is None:
            raise KeyError(f"Loader '{loader}' not registered")

        logger.info("Running pipeline: '{}' -> '{}'", extractor, loader)
        run_pipeline(
            ext_reg,
            ldr_reg,
            extractor_config=extractor_config,
            loader_config=loader_config,
            validate_records=validate_records,
        )
        logger.info("Pipeline '{}' -> '{}' completed", extractor, loader)

    def list_extractors(self) -> list[str]:
        return list(self._extractors.keys())

    def list_loaders(self) -> list[str]:
        return list(self._loaders.keys())

    def get_extractor(self, name: str) -> ExtractorRegistration:
        return self._extractors[name]

    def get_loader(self, name: str) -> LoaderRegistration:
        return self._loaders[name]
