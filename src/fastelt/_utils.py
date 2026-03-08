"""Internal utilities for fastELT."""

import inspect
from typing import Any

from pydantic import BaseModel, create_model


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
