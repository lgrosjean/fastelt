from __future__ import annotations

from typing import Any, Callable

from fastelt._utils import build_config_model, get_incremental_params, get_record_type
from fastelt.types import ExtractorRegistration, WriteDisposition


def create_extractor_registration(
    name: str,
    func: Callable[..., Any],
    *,
    description: str | None = None,
    tags: list[str] | None = None,
    deprecated: bool = False,
    primary_key: str | list[str] | None = None,
    write_disposition: str | WriteDisposition = WriteDisposition.APPEND,
    source_name: str | None = None,
) -> ExtractorRegistration:
    """Validate and create an ExtractorRegistration from a decorated function.

    Supports two patterns:
    - yield (stream): def extract(...) -> Iterator[T]  — lazy, memory-efficient
    - return (batch): def extract(...) -> list[T]      — all records at once
    """
    record_type = get_record_type(func)
    incremental = get_incremental_params(func)

    # Exclude incremental params from config model (they're injected by pipeline)
    exclude = set(incremental.keys())
    config_model = build_config_model(func, exclude=exclude)

    return ExtractorRegistration(
        name=name,
        func=func,
        config_model=config_model,
        record_type=record_type,
        description=description or func.__doc__,
        tags=tags or [],
        deprecated=deprecated,
        primary_key=primary_key,
        write_disposition=WriteDisposition(write_disposition),
        incremental_params=incremental,
        source_name=source_name,
    )
