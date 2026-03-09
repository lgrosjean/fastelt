"""Internal utilities for fastELT."""

from __future__ import annotations

import inspect
from typing import Any

from pydantic import BaseModel, create_model


def resolve_env_values(obj: Any) -> Any:
    """Recursively resolve :class:`~fastelt.types.Env` instances in nested structures.

    Works on dicts, lists, and scalar values.  Non-Env values are returned as-is.
    """
    # Import here to avoid circular import (types.py imports nothing from _utils)
    from fastelt.config import Env

    if isinstance(obj, Env):
        return obj.resolve()
    if isinstance(obj, dict):
        return {k: resolve_env_values(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [resolve_env_values(v) for v in obj]
    return obj


def build_config_model(fn: Any, *, exclude: set[str] | None = None) -> type[BaseModel]:
    """Build a Pydantic config model from a function's signature.

    Like FastAPI infers query params from function args, this infers config
    fields from parameters — including Field() defaults.
    """
    exclude = exclude or set()
    sig = inspect.signature(fn)
    hints = inspect.get_annotations(fn, eval_str=True)
    fields: dict[str, Any] = {}

    for name, param in sig.parameters.items():
        if name in exclude:
            continue
        annotation = hints.get(name, Any)
        default = ... if param.default is inspect.Parameter.empty else param.default
        fields[name] = (annotation, default)

    return create_model(f"{fn.__name__}_Config", **fields)
