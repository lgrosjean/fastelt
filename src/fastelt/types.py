from __future__ import annotations

import functools
import inspect
import os
from collections.abc import Callable, Iterator
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Generic, TypeVar

from pydantic import BaseModel, PrivateAttr, create_model

T = TypeVar("T", bound=BaseModel)


class WriteDisposition(str, Enum):
    """How extracted records should be written to the destination."""

    APPEND = "append"
    REPLACE = "replace"
    MERGE = "merge"

_UNSET = object()


class Env:
    """Lazy reference to an environment variable.

    Resolves the value when ``resolve()`` is called (typically at Source
    construction time), not at import time.  This gives three advantages
    over a bare ``os.getenv``:

    * **Lazy** — the var is read when the Source is used, not when the
      module is imported.
    * **Fail-fast** — raises immediately with a clear message if the var
      is missing and no *default* was provided.
    * **Declarative** — the Source definition shows *which* values come
      from the environment.

    Usage::

        from fastelt import Env, Source

        github = Source(
            base_url="https://api.github.com",
            token=Env("GH_TOKEN"),
            org="anthropics",
        )
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


class Records(Generic[T]):
    """Injectable container for extracted records.

    Like FastAPI's Request object — declare it in your loader signature
    only if you need access to the extracted data.

    Usage:
        @app.loader("my_loader")
        def load(records: Records[User], path: str = Field(...)) -> None:
            for record in records:
                ...
    """

    def __init__(self, data: Iterator[T]) -> None:
        self._data = data
        self._consumed = False

    def __iter__(self) -> Iterator[T]:
        if self._consumed:
            raise RuntimeError("Records have already been consumed")
        self._consumed = True
        yield from self._data

    def collect(self) -> list[T]:
        """Consume all records into a list."""
        return list(self)


@dataclass
class ExtractorRegistration:
    name: str
    func: Callable[..., Any]
    config_model: type[BaseModel]
    record_type: type[BaseModel]
    description: str | None = None
    tags: list[str] = field(default_factory=list)
    deprecated: bool = False
    primary_key: str | list[str] | None = None
    write_disposition: WriteDisposition = WriteDisposition.APPEND
    incremental_params: dict[str, Any] = field(default_factory=dict)
    source_name: str | None = None


@dataclass
class LoaderRegistration:
    name: str
    func: Callable[..., Any]
    config_model: type[BaseModel]
    record_type: type[BaseModel] | None
    records_param: str | None


@dataclass
class PluginGroup:
    extractors: dict[str, ExtractorRegistration] = field(default_factory=dict)
    loaders: dict[str, LoaderRegistration] = field(default_factory=dict)


@dataclass
class _EntityMeta:
    func: Callable[..., Any]
    description: str | None = None
    tags: list[str] = field(default_factory=list)
    deprecated: bool = False
    primary_key: str | list[str] | None = None
    write_disposition: WriteDisposition = WriteDisposition.APPEND


class Source(BaseModel):
    """Shared config for a group of related extractors — like FastAPI's APIRouter.

    Create programmatically or via subclass:

        # Programmatic — types inferred from values:
        github = Source(
            base_url="https://api.github.com",
            token="ghp_...",
            org="anthropics",
        )

        # Class-based — for complex schemas (validators, descriptions, etc.):
        class MlflowSource(Source):
            tracking_uri: str
            token: str = ""

        mlflow = MlflowSource(tracking_uri="http://localhost:5000")

    Then register entities:

        @github.entity()
        def repositories(min_stars: int = Field(default=0)) -> Iterator[Repository]:
            print(github.org)  # access source via closure
            ...

        app.include_source(github)
    """

    _entities: dict[str, _EntityMeta] = PrivateAttr(default_factory=dict)
    _source_name: str | None = PrivateAttr(default=None)

    def __new__(cls, **kwargs: Any) -> Source:
        if cls is Source:
            # Resolve any Env instances to their environment variable values
            resolved = {
                k: v.resolve() if isinstance(v, Env) else v
                for k, v in kwargs.items()
            }
            # Direct Source(...) call — dynamically create a typed subclass
            fields = {k: (type(v), v) for k, v in resolved.items()}
            dynamic_cls = create_model("Source", __base__=Source, **fields)  # type: ignore[call-overload]
            return dynamic_cls(**resolved)
        return super().__new__(cls)

    def entity(
        self,
        name: str | None = None,
        *,
        description: str | None = None,
        tags: list[str] | None = None,
        deprecated: bool = False,
        primary_key: str | list[str] | None = None,
        write_disposition: str | WriteDisposition = WriteDisposition.APPEND,
    ) -> Callable[..., Any]:
        """Decorator to register an entity extractor on this source."""

        def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
            key = name or func.__name__  # type: ignore[attr-defined]
            self._entities[key] = _EntityMeta(
                func=func,
                description=description,
                tags=tags or [],
                deprecated=deprecated,
                primary_key=primary_key,
                write_disposition=WriteDisposition(write_disposition),
            )
            return func

        return decorator

    def _build_plugin_group(self) -> PluginGroup:
        """Build registrations — called lazily by app.include()."""
        from fastelt.extractor import create_extractor_registration

        extractors: dict[str, ExtractorRegistration] = {}
        for key, meta in self._entities.items():
            bound = self._bind_source(meta.func)
            extractors[key] = create_extractor_registration(
                key,
                bound,
                description=meta.description,
                tags=meta.tags,
                deprecated=meta.deprecated,
                primary_key=meta.primary_key,
                write_disposition=meta.write_disposition,
                source_name=self._source_name,
            )
        return PluginGroup(extractors=extractors)

    def _bind_source(self, func: Callable[..., Any]) -> Callable[..., Any]:
        """Wrap function to inject this source instance if it declares a Source param.

        Detection (in order):
        1. Parameter annotated as a Source subclass (explicit)
        2. Parameter with no annotation and no default (convention)
        """
        hints = inspect.get_annotations(func, eval_str=True)
        sig = inspect.signature(func)

        source_param: str | None = None
        for param_name, param in sig.parameters.items():
            hint = hints.get(param_name)
            # Explicit: annotated as Source subclass
            if hint is not None and isinstance(hint, type) and issubclass(hint, Source):
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
