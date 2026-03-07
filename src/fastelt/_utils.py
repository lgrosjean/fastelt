import collections.abc
import inspect
from typing import Any, get_args, get_origin

from pydantic import BaseModel, create_model

from fastelt.incremental import Incremental
from fastelt.types import Records


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


def get_incremental_params(fn: Any) -> dict[str, Incremental]:
    """Find parameters typed as ``Incremental[T]`` with an ``Incremental`` default.

    Returns a dict mapping parameter names to their Incremental instances.
    """
    hints = inspect.get_annotations(fn, eval_str=True)
    sig = inspect.signature(fn)
    result: dict[str, Incremental] = {}

    for name, param in sig.parameters.items():
        hint = hints.get(name)
        if hint is None:
            continue
        origin = get_origin(hint)
        if origin is Incremental and isinstance(param.default, Incremental):
            result[name] = param.default

    return result


def get_record_type(fn: Any) -> type[BaseModel]:
    """Extract T from Iterator[T] (stream) or list[T] (batch) return annotation."""
    hints = inspect.get_annotations(fn, eval_str=True)
    ret = hints.get("return")
    if ret is None:
        raise TypeError(f"Function {fn.__name__} must have a return type annotation")

    origin = get_origin(ret)
    if origin in (collections.abc.Iterator, list):
        args = get_args(ret)
        if args and _is_basemodel_subclass(args[0]):
            return args[0]

    raise TypeError(
        f"Function {fn.__name__} must return Iterator[T] or list[T] "
        f"where T is a BaseModel subclass, got {ret}"
    )


def get_records_param(fn: Any) -> tuple[str, type[BaseModel]] | None:
    """Find a parameter typed as Records[T] (for loaders).

    Returns (param_name, record_type) or None if the loader doesn't
    request records injection.
    """
    hints = inspect.get_annotations(fn, eval_str=True)
    sig = inspect.signature(fn)

    for name in sig.parameters:
        hint = hints.get(name)
        if hint is None:
            continue
        origin = get_origin(hint)
        if origin is Records:
            args = get_args(hint)
            if args and _is_basemodel_subclass(args[0]):
                return (name, args[0])

    return None


def _is_basemodel_subclass(tp: Any) -> bool:
    try:
        return isinstance(tp, type) and issubclass(tp, BaseModel)
    except TypeError:
        return False
