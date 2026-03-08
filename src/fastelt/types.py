"""Core types for fastELT — a FastAPI-inspired wrapper around dlt.

Like FastAPI wraps Starlette, fastELT wraps dlt with a decorator-driven DX:

- ``Source``     → config container that produces ``dlt.source`` objects
- ``resource()`` → decorator that creates ``dlt.resource`` entries
- ``Env``        → lazy environment variable resolution (like FastAPI's ``Query``)
- ``Secret``     → like ``Env`` but masked in logs/repr
"""

from __future__ import annotations

import functools
import inspect
import os
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Annotated, Any, get_args, get_origin

import dlt
from loguru import logger
from pydantic import BaseModel, ConfigDict, PrivateAttr, create_model

_UNSET = object()


class Env:
    """Lazy reference to an environment variable.

    Can be used as a value or as a type annotation with ``Annotated``.

    Usage::

        from fastelt import Env, Source

        # As a value (resolved at Source construction):
        github = Source(
            name="github",
            token=Env("GH_TOKEN"),
        )

        # As an Annotated type hint (resolved at resource call time):
        @source.resource()
        def repos(token: Annotated[str, Env("GH_TOKEN")]):
            ...
    """

    __slots__ = ("_var", "_default")

    def __init__(self, var: str, default: str = _UNSET) -> None:  # type: ignore[assignment]
        self._var = var
        self._default = default

    def resolve(self) -> str:
        """Return the current value of the environment variable."""
        value = os.environ.get(self._var, self._default)
        if value is _UNSET:
            raise EnvironmentError(
                f"Environment variable '{self._var}' is not set and no default was provided"
            )
        return value  # type: ignore[return-value]

    def __repr__(self) -> str:
        if self._default is _UNSET:
            return f"Env({self._var!r})"
        return f"Env({self._var!r}, default={self._default!r})"


class Secret(Env):
    """Like ``Env`` but masks the value in logs and repr.

    Use for sensitive values (API keys, tokens, passwords).

    Usage::

        from fastelt import Secret, Source

        # As a value:
        github = Source(name="github", token=Secret("GH_TOKEN"))

        # As an Annotated type hint:
        @source.resource()
        def repos(token: Annotated[str, Secret("GH_TOKEN")]):
            ...
    """

    def __repr__(self) -> str:
        return f"Secret({self._var!r})"


def _resolve_annotated_params(func: Callable[..., Any]) -> Callable[..., Any]:
    """Wrap a function to auto-resolve ``Annotated[str, Env(...)]`` parameters.

    Like FastAPI resolves ``Query()``, ``Path()`` etc. from type annotations,
    this resolves ``Env`` and ``Secret`` annotations before calling the function.
    """
    try:
        hints = inspect.get_annotations(func, eval_str=True)
    except NameError:
        hints = inspect.get_annotations(func, eval_str=False)

    sig = inspect.signature(func)

    # Find params with Annotated[T, Env(...)] or Annotated[T, Secret(...)]
    env_params: dict[str, Env] = {}
    for param_name, param in sig.parameters.items():
        hint = hints.get(param_name)
        if hint is None or get_origin(hint) is not Annotated:
            continue
        args = get_args(hint)
        for arg in args[1:]:
            if isinstance(arg, Env):
                env_params[param_name] = arg
                break

    if not env_params:
        return func

    # Strip Env-injected params from the visible signature
    remaining_params = [
        p for p in sig.parameters.values() if p.name not in env_params
    ]

    @functools.wraps(func)
    def wrapper(**kwargs: Any) -> Any:
        for param_name, env in env_params.items():
            if param_name not in kwargs:
                kwargs[param_name] = env.resolve()
        return func(**kwargs)

    wrapper.__signature__ = sig.replace(parameters=remaining_params)  # type: ignore[attr-defined]
    wrapper.__annotations__ = {
        k: v for k, v in hints.items() if k not in env_params
    }
    return wrapper


@dataclass
class _ResourceMeta:
    """Internal metadata for a registered resource."""

    func: Callable[..., Any]
    name: str
    description: str | None = None
    tags: list[str] = field(default_factory=list)
    deprecated: bool = False
    primary_key: str | list[str] | None = None
    write_disposition: str = "append"
    merge_key: str | list[str] | None = None
    table_name: str | None = None
    selected: bool = True


class Source(BaseModel):
    """Shared config for a group of related resources — like FastAPI's APIRouter.

    Create programmatically or via subclass::

        # Programmatic — types inferred from values:
        github = Source(
            name="github",
            base_url="https://api.github.com",
            token=Env("GH_TOKEN"),
            org="anthropics",
        )

        # Class-based — for complex schemas:
        class GitHubSource(Source):
            base_url: str = "https://api.github.com"
            token: str
            org: str

    Then register resources (like dlt, but with FastAPI-style decorators)::

        @github.resource(primary_key="id", write_disposition="merge")
        def repositories(
            updated_at=dlt.sources.incremental("updated_at"),
        ) -> Iterator[dict]:
            ...

        app.include_source(github)
        app.run(destination="duckdb")
    """

    model_config = ConfigDict(arbitrary_types_allowed=True)

    _resources: dict[str, _ResourceMeta] = PrivateAttr(default_factory=dict)
    _source_name: str | None = PrivateAttr(default=None)

    def __init__(self, **kwargs: Any) -> None:
        # Resolve Env values before Pydantic validation
        resolved = {
            k: v.resolve() if isinstance(v, Env) else v
            for k, v in kwargs.items()
        }
        super().__init__(**resolved)

    def __new__(cls, **kwargs: Any) -> Source:
        if cls is Source:
            # Resolve any Env instances to their environment variable values
            resolved = {
                k: v.resolve() if isinstance(v, Env) else v
                for k, v in kwargs.items()
            }
            # Direct Source(...) call — dynamically create a typed subclass
            # 'name' becomes a regular model field alongside base_url, token, etc.
            fields = {k: (type(v), v) for k, v in resolved.items()}
            dynamic_cls = create_model("Source", __base__=Source, **fields)  # type: ignore[call-overload]
            return dynamic_cls(**resolved)
        return super().__new__(cls)

    def resource(
        self,
        name: str | None = None,
        *,
        description: str | None = None,
        tags: list[str] | None = None,
        deprecated: bool = False,
        primary_key: str | list[str] | None = None,
        write_disposition: str = "append",
        merge_key: str | list[str] | None = None,
        table_name: str | None = None,
        selected: bool = True,
    ) -> Callable[..., Any]:
        """Decorator to register a resource on this source.

        Mirrors ``dlt.resource()`` parameters with FastAPI-style decorator DX.

        Parameters
        ----------
        name:
            Resource name. Defaults to function name.
        primary_key:
            Column(s) used as primary key (for merge).
        write_disposition:
            How data is written: ``"append"``, ``"replace"``, or ``"merge"``.
        merge_key:
            Column(s) used to match records for merge.
        table_name:
            Destination table name. Defaults to resource name.
        selected:
            Whether this resource runs by default.
        """

        def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
            key = name or func.__name__
            self._resources[key] = _ResourceMeta(
                func=func,
                name=key,
                description=description or func.__doc__,
                tags=tags or [],
                deprecated=deprecated,
                primary_key=primary_key,
                write_disposition=write_disposition,
                merge_key=merge_key,
                table_name=table_name or key,
                selected=selected,
            )
            return func

        return decorator

    def _build_dlt_source(
        self,
        resource_names: list[str] | None = None,
    ) -> dlt.sources.DltSource:
        """Convert this Source into a ``dlt.source`` with ``dlt.resource`` entries.

        Parameters
        ----------
        resource_names:
            If provided, only include these resources (selective run).
        """
        source_instance = self
        resource_metas = dict(self._resources)
        source_name = self._source_name or type(self).__name__.lower()

        # Build dlt resources from registered functions
        dlt_resources: list[Any] = []
        for key, meta in resource_metas.items():
            if resource_names and key not in resource_names:
                continue
            if not meta.selected and not (resource_names and key in resource_names):
                continue

            if meta.deprecated:
                logger.warning("Resource '{}' is deprecated", key)

            # Resolve Annotated[str, Env(...)] params, then bind source
            bound = _resolve_annotated_params(meta.func)
            bound = self._bind_source(bound)

            # Build dlt.resource kwargs
            res_kwargs: dict[str, Any] = {
                "name": meta.table_name or key,
                "primary_key": meta.primary_key,
                "write_disposition": meta.write_disposition,
            }
            if meta.merge_key:
                res_kwargs["merge_key"] = meta.merge_key

            dlt_res = dlt.resource(bound, **res_kwargs)
            dlt_resources.append(dlt_res)

        # Create dlt source wrapping these resources
        @dlt.source(name=source_name)
        def _make_source() -> Any:
            return dlt_resources

        return _make_source()

    def _bind_source(self, func: Callable[..., Any]) -> Callable[..., Any]:
        """Wrap function to inject this source instance if it declares a Source param.

        Detection (in order):
        1. Parameter annotated as a Source subclass (explicit)
        2. Parameter with no annotation and no default (convention)
        """
        try:
            hints = inspect.get_annotations(func, eval_str=True)
        except NameError:
            # Annotation references a locally-defined class that can't be resolved
            hints = inspect.get_annotations(func, eval_str=False)
        sig = inspect.signature(func)

        source_param: str | None = None
        for param_name, param in sig.parameters.items():
            hint = hints.get(param_name)
            # Explicit: annotated as Source subclass
            if hint is not None and isinstance(hint, type) and issubclass(hint, Source):
                source_param = param_name
                break
            # String annotation containing "Source" — treat as source param
            if isinstance(hint, str) and "Source" in hint:
                source_param = param_name
                break
            # Convention: no annotation, no default
            if hint is None and param.default is inspect.Parameter.empty:
                source_param = param_name
                break

        if source_param is None:
            return func

        source_instance = self
        source_key = source_param

        @functools.wraps(func)
        def bound(**kwargs: Any) -> Any:
            kwargs[source_key] = source_instance
            yield from func(**kwargs)

        # Fix signature and annotations to exclude the source param
        new_params = [p for p in sig.parameters.values() if p.name != source_key]
        bound.__signature__ = sig.replace(parameters=new_params)  # type: ignore[attr-defined]
        bound.__annotations__ = {k: v for k, v in hints.items() if k != source_key}

        return bound

    def list_resources(self) -> list[str]:
        """Return names of registered resources."""
        return list(self._resources.keys())

    def get_resource_meta(self, name: str) -> _ResourceMeta:
        """Get metadata for a resource."""
        return self._resources[name]
