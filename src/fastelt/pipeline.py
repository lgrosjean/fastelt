from __future__ import annotations

from collections.abc import Iterator
from typing import Any

from loguru import logger
from pydantic import BaseModel

from fastelt.incremental import Incremental
from fastelt.state import StateStore
from fastelt.types import ExtractorRegistration, LoaderRegistration, Records


def run_pipeline(
    extractor_reg: ExtractorRegistration,
    loader_reg: LoaderRegistration,
    extractor_config: dict[str, object] | None = None,
    loader_config: dict[str, object] | None = None,
    *,
    validate_records: bool = True,
    state_dir: str | None = None,
) -> None:
    """Run a pipeline connecting an extractor to a loader."""
    if extractor_reg.deprecated:
        logger.warning("Extractor '{}' is deprecated", extractor_reg.name)

    logger.debug("Validating extractor config for '{}'", extractor_reg.name)
    ext_conf = extractor_reg.config_model(**(extractor_config or {}))
    logger.debug("Validating loader config for '{}'", loader_reg.name)
    ldr_conf = loader_reg.config_model(**(loader_config or {}))

    # -- Prepare extractor kwargs (config + injected incrementals) --
    ext_kwargs: dict[str, Any] = ext_conf.model_dump()

    # Load state and inject Incremental instances
    store: StateStore | None = None
    live_incrementals: dict[str, Incremental] = {}
    if extractor_reg.incremental_params:
        store = StateStore(state_dir or ".fastelt/state")
        state = store.load(extractor_reg.name)

        for param_name, template in extractor_reg.incremental_params.items():
            inc = template._clone()
            if param_name in state:
                inc._last_value = inc._deserialize(state[param_name])
            live_incrementals[param_name] = inc
            ext_kwargs[param_name] = inc

    logger.debug("Extracting records from '{}'", extractor_reg.name)
    raw_records = extractor_reg.func(**ext_kwargs)

    # Wrap iterator to track cursor values from yielded records
    if live_incrementals:
        raw_records = _tracking_iterator(
            raw_records, live_incrementals, extractor_reg.record_type
        )

    if validate_records:
        logger.debug(
            "Record validation enabled for type '{}'",
            extractor_reg.record_type.__name__,
        )
        raw_records = _validated_iterator(raw_records, extractor_reg.record_type)

    loader_kwargs = ldr_conf.model_dump()
    if loader_reg.records_param is not None:
        loader_kwargs[loader_reg.records_param] = Records(raw_records)

    logger.debug("Loading records into '{}'", loader_reg.name)
    loader_reg.func(**loader_kwargs)

    # Persist incremental state after successful load
    if store is not None and live_incrementals:
        new_state: dict[str, Any] = {}
        for param_name, inc in live_incrementals.items():
            value = inc.end_value if inc.end_value is not None else inc.last_value
            if value is not None:
                new_state[param_name] = inc._serialize(value)
        if new_state:
            store.save(extractor_reg.name, new_state)


def _tracking_iterator(
    records: Iterator[BaseModel],
    incrementals: dict[str, Incremental],
    record_type: type[BaseModel],
) -> Iterator[BaseModel]:
    """Wrap an iterator to track cursor field values for each incremental."""
    for record in records:
        for inc in incrementals.values():
            # Extract cursor value from the record
            value = _get_cursor_value(record, inc.cursor_path)
            inc._track(value)
        yield record


def _get_cursor_value(record: Any, cursor_path: str) -> Any:
    """Extract a cursor field value from a record (dict or model)."""
    if isinstance(record, dict):
        return record.get(cursor_path)
    return getattr(record, cursor_path, None)


def _validated_iterator(
    records: Iterator[BaseModel], record_type: type[BaseModel]
) -> Iterator[BaseModel]:
    """Wrap an iterator to validate/coerce each record against the expected type.

    Like FastAPI's response_model, raw dicts (or any mapping) are automatically
    parsed into the target Pydantic model.  Already-typed instances pass through.
    """
    for record in records:
        if isinstance(record, record_type):
            yield record
        elif isinstance(record, dict):
            yield record_type.model_validate(record)
        else:
            yield record_type.model_validate(record, from_attributes=True)
