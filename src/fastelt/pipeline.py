from __future__ import annotations

from collections.abc import Iterator

from loguru import logger
from pydantic import BaseModel

from fastelt.types import ExtractorRegistration, LoaderRegistration, Records


def run_pipeline(
    extractor_reg: ExtractorRegistration,
    loader_reg: LoaderRegistration,
    extractor_config: dict[str, object] | None = None,
    loader_config: dict[str, object] | None = None,
    *,
    validate_records: bool = True,
) -> None:
    """Run a pipeline connecting an extractor to a loader."""
    if extractor_reg.deprecated:
        logger.warning("Extractor '{}' is deprecated", extractor_reg.name)

    logger.debug("Validating extractor config for '{}'", extractor_reg.name)
    ext_conf = extractor_reg.config_model(**(extractor_config or {}))
    logger.debug("Validating loader config for '{}'", loader_reg.name)
    ldr_conf = loader_reg.config_model(**(loader_config or {}))

    logger.debug("Extracting records from '{}'", extractor_reg.name)
    raw_records = extractor_reg.func(**ext_conf.model_dump())

    if validate_records:
        logger.debug("Record validation enabled for type '{}'", extractor_reg.record_type.__name__)
        raw_records = _validated_iterator(raw_records, extractor_reg.record_type)

    loader_kwargs = ldr_conf.model_dump()
    if loader_reg.records_param is not None:
        loader_kwargs[loader_reg.records_param] = Records(raw_records)

    logger.debug("Loading records into '{}'", loader_reg.name)
    loader_reg.func(**loader_kwargs)


def _validated_iterator(
    records: Iterator[BaseModel], record_type: type[BaseModel]
) -> Iterator[BaseModel]:
    """Wrap an iterator to validate each record against the expected type."""
    for record in records:
        if not isinstance(record, record_type):
            raise TypeError(
                f"Expected {record_type.__name__}, got {type(record).__name__}"
            )
        yield record
