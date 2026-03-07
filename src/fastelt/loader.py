from __future__ import annotations

from typing import Any, Callable

from fastelt._utils import build_config_model, get_records_param
from fastelt.types import LoaderRegistration


def create_loader_registration(
    name: str, func: Callable[..., Any]
) -> LoaderRegistration:
    """Validate and create a LoaderRegistration from a decorated function."""
    result = get_records_param(func)

    if result is not None:
        records_param_name, record_type = result
        config_model = build_config_model(func, exclude={records_param_name})
    else:
        records_param_name = None
        record_type = None
        config_model = build_config_model(func)

    return LoaderRegistration(
        name=name,
        func=func,
        config_model=config_model,
        record_type=record_type,
        records_param=records_param_name,
    )
