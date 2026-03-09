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
import warnings
from collections.abc import Callable, Generator, Iterable, Iterator
from dataclasses import dataclass, field
from typing import Annotated, Any, get_args, get_origin

import dlt
from loguru import logger
from pydantic import BaseModel, ConfigDict, PrivateAttr, ValidationError, create_model

from fastelt.config import Env, Secret


class SchemaFrozenError(Exception):
    """Raised when extra columns are detected on a frozen resource.

    A frozen resource (``@source.resource(frozen=True)``) enforces a strict
    schema — any columns not defined in the ``response_model`` are rejected.
    """


def _get_model_known_keys(model_cls: type[BaseModel]) -> set[str]:
    """Return all keys a model recognizes (field names + aliases)."""
    known: set[str] = set()
    for field_name, field_info in model_cls.model_fields.items():
        known.add(field_name)
        if field_info.alias:
            known.add(field_info.alias)
        if field_info.validation_alias:
            # validation_alias can be str or AliasPath/AliasChoices
            alias = field_info.validation_alias
            if isinstance(alias, str):
                known.add(alias)
    # Also account for alias_generator: generate alias for each field name
    config = model_cls.model_config
    alias_gen = config.get("alias_generator")
    if alias_gen:
        for field_name in model_cls.model_fields:
            try:
                known.add(alias_gen(field_name))
            except Exception:
                pass
    return known


def _validate_record(
    record: dict[str, Any],
    model_cls: type[BaseModel],
    frozen: bool,
    resource_name: str,
) -> dict[str, Any]:
    """Validate a single record dict through a pydantic model.

    - Enforces types and runs field/model validators (data quality).
    - Normalizes column names via alias_generator.
    - Extra keys: warns (default) or raises SchemaFrozenError (frozen=True).
    """
    known_keys = _get_model_known_keys(model_cls)
    extra_keys = set(record.keys()) - known_keys

    if extra_keys:
        if frozen:
            raise SchemaFrozenError(
                f"Resource '{resource_name}' is frozen but received new columns: "
                f"{sorted(extra_keys)}. Update the response_model to accept them "
                f"or remove frozen=True."
            )
        else:
            warnings.warn(
                f"Resource '{resource_name}': new keys not ingested: "
                f"{sorted(extra_keys)}. Add them to the response_model to ingest.",
                UserWarning,
                stacklevel=2,
            )

    # Filter to known keys before validation so pydantic doesn't choke on extras
    filtered = {k: v for k, v in record.items() if k in known_keys}
    validated = model_cls.model_validate(filtered)
    return validated.model_dump(by_alias=False)


def _extract_inner_type(annotation: Any) -> type[BaseModel] | None:
    """Extract BaseModel subclass from return type annotations.

    Handles: list[User], Iterator[User], Generator[User, ...], Iterable[User], bare User.
    Returns None for non-BaseModel types (list[dict], str, etc.).
    """
    origin = get_origin(annotation)
    if origin in (list, Iterator, Generator, Iterable):
        args = get_args(annotation)
        if args:
            candidate = args[0]
            if isinstance(candidate, type) and issubclass(candidate, BaseModel):
                return candidate
    elif isinstance(annotation, type) and issubclass(annotation, BaseModel):
        return annotation
    return None


def _wrap_with_validation(
    gen_func: Callable[..., Any],
    model_cls: type[BaseModel],
    frozen: bool,
    resource_name: str,
) -> Callable[..., Any]:
    """Wrap a generator function to validate each yielded record."""

    @functools.wraps(gen_func)
    def wrapper(*args: Any, **kwargs: Any) -> Iterator[dict[str, Any]]:
        for record in gen_func(*args, **kwargs):
            if isinstance(record, dict):
                yield _validate_record(record, model_cls, frozen, resource_name)
            else:
                yield record

    # Preserve __signature__ through the wrapping chain so dlt sees the
    # correct (stripped) signature instead of following __wrapped__.
    if hasattr(gen_func, "__signature__"):
        wrapper.__signature__ = gen_func.__signature__  # type: ignore[attr-defined]
    # Remove __wrapped__ to prevent dlt/inspect from unwrapping to the original
    if hasattr(wrapper, "__wrapped__"):
        del wrapper.__wrapped__

    return wrapper


def _resolve_env_params(func: Callable[..., Any]) -> Callable[..., Any]:
    """Auto-resolve environment variables and incremental cursors from parameters.

    Resolution rules (in priority order):

    1. ``Annotated[str, Env("CUSTOM_VAR")]`` → resolve from ``CUSTOM_VAR``
    2. ``Annotated[str, Secret("CUSTOM_VAR")]`` → resolve from ``CUSTOM_VAR`` (masked)
    3. ``Annotated[str, Incremental(...)]`` → inject ``dlt.sources.incremental``
    4. ``param_name: str`` → resolve from ``PARAM_NAME`` (uppercased)
    5. ``param_name: str = "default"`` → resolve from ``PARAM_NAME``, fallback to ``"default"``

    Like FastAPI auto-resolves ``Query()``, ``Path()`` from type hints,
    fastELT auto-resolves env vars and incremental cursors from annotations.
    """
    from fastelt.sources.types import Incremental

    try:
        hints = inspect.get_annotations(func, eval_str=True)
    except NameError:
        hints = inspect.get_annotations(func, eval_str=False)

    sig = inspect.signature(func)

    # Collect params to resolve
    env_params: dict[str, Env] = {}
    incremental_params: dict[str, Incremental] = {}

    for param_name, param in sig.parameters.items():
        hint = hints.get(param_name)
        if hint is None:
            continue

        # Priority 1: Annotated[str, Env/Secret/Incremental]
        if get_origin(hint) is Annotated:
            args = get_args(hint)
            for arg in args[1:]:
                if isinstance(arg, Incremental):
                    incremental_params[param_name] = arg
                    break
                if isinstance(arg, Env):
                    env_params[param_name] = arg
                    break
            continue

        # Priority 2: plain `str` annotation → auto-resolve from UPPERCASED name
        if hint is str:
            var_name = param_name.upper()
            if param.default is not inspect.Parameter.empty:
                env_params[param_name] = Env(var_name, default=param.default)
            else:
                env_params[param_name] = Env(var_name)

    if not env_params and not incremental_params:
        return func

    # Build final signature:
    # - env params: stripped (injected at call time)
    # - incremental params: kept with dlt.sources.incremental as default (dlt handles them)
    final_params = []
    for p in sig.parameters.values():
        if p.name in env_params:
            continue  # stripped — resolved at call time
        if p.name in incremental_params:
            inc = incremental_params[p.name]
            final_params.append(
                p.replace(
                    default=inc.resolve(p.name),
                    annotation=inspect.Parameter.empty,
                )
            )
        else:
            final_params.append(p)

    @functools.wraps(func)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        for param_name, env in env_params.items():
            if param_name not in kwargs:
                kwargs[param_name] = env.resolve()
        return func(*args, **kwargs)

    wrapper.__signature__ = sig.replace(parameters=final_params)  # type: ignore[attr-defined]
    wrapper.__annotations__ = {
        k: v for k, v in hints.items() if k not in env_params and k not in incremental_params
    }
    return wrapper


def _resolve_parent_deps(
    func: Callable[..., Any],
    type_registry: dict[type[BaseModel], str],
) -> tuple[Callable[..., Any], str | None]:
    """Detect parent dependency from BaseModel-typed params.

    If a parameter's type annotation is a BaseModel subclass that exists in the
    type_registry (i.e., is produced by another resource), wrap the function as
    a dlt transformer-compatible callable.

    Returns (wrapped_func, parent_resource_name) or (func, None).
    """
    sig = inspect.signature(func)
    # Get evaluated type annotations. inspect.get_annotations may unwrap through
    # __wrapped__, so we intersect with the current signature's parameters.
    try:
        all_hints = inspect.get_annotations(func, eval_str=True)
    except NameError:
        all_hints = inspect.get_annotations(func, eval_str=False)
    hints = {k: v for k, v in all_hints.items() if k in sig.parameters}

    deps: list[tuple[str, type[BaseModel], str]] = []  # (param_name, model_cls, parent_name)
    for param_name, param in sig.parameters.items():
        hint = hints.get(param_name)
        if hint is None:
            continue
        if isinstance(hint, type) and issubclass(hint, BaseModel) and not issubclass(hint, Source):
            if hint in type_registry:
                deps.append((param_name, hint, type_registry[hint]))

    if not deps:
        return func, None

    if len(deps) > 1:
        dep_names = [f"{p}:{m.__name__}" for p, m, _ in deps]
        raise ValueError(
            f"Multiple parent dependencies found: {dep_names}. "
            f"dlt transformers support only one parent."
        )

    dep_param_name, model_cls, parent_name = deps[0]

    @functools.wraps(func)
    def wrapper(item: Any, **kwargs: Any) -> Any:
        validated = model_cls.model_validate(item)
        kwargs[dep_param_name] = validated
        yield from func(**kwargs)

    # Replace dep param with `item` positional param (dlt requires first arg for transformers)
    item_param = inspect.Parameter("item", inspect.Parameter.POSITIONAL_OR_KEYWORD)
    new_params = [item_param] + [
        p for p in sig.parameters.values() if p.name != dep_param_name
    ]
    wrapper.__signature__ = sig.replace(parameters=new_params)  # type: ignore[attr-defined]
    wrapper.__annotations__ = {k: v for k, v in hints.items() if k != dep_param_name}

    return wrapper, parent_name


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
    response_model: type[BaseModel] | None = None
    frozen: bool = False
    produces_type: type[BaseModel] | None = None


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
            updated_at: Annotated[str, Incremental()],
        ) -> Iterator[dict]:
            ...

        app.include_source(github)
        app.run(destination="duckdb")
    """

    model_config = ConfigDict(arbitrary_types_allowed=True)

    _resources: dict[str, _ResourceMeta] = PrivateAttr(default_factory=dict)
    _source_name: str | None = PrivateAttr(default=None)
    _type_registry: dict[type[BaseModel], str] = PrivateAttr(default_factory=dict)

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
        response_model: type[BaseModel] | None = None,
        frozen: bool = False,
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
        response_model:
            Pydantic model for record validation, type enforcement, column
            normalization (via ``alias_generator``), and data quality checks
            (via field validators).  Like FastAPI's ``response_model``.
        frozen:
            If ``True``, extra columns not in ``response_model`` raise
            :class:`SchemaFrozenError` instead of a warning.  Use for
            strict schema enforcement.
        """

        def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
            key = name or func.__name__

            # Extract return type annotation for auto-detection
            try:
                hints = inspect.get_annotations(func, eval_str=True)
            except NameError:
                hints = inspect.get_annotations(func, eval_str=False)
            return_hint = hints.get("return")
            produces_type = _extract_inner_type(return_hint) if return_hint else None

            # Auto-set response_model from return type if not explicitly provided
            effective_response_model = response_model
            if produces_type and response_model is None:
                effective_response_model = produces_type

            # Register producer type (for parent-child detection)
            if produces_type is not None:
                if produces_type in self._type_registry:
                    existing = self._type_registry[produces_type]
                    raise ValueError(
                        f"Multiple resources produce {produces_type.__name__}: "
                        f"'{existing}' and '{key}'. "
                        f"Auto-detection requires unambiguous type producers."
                    )
                self._type_registry[produces_type] = key

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
                response_model=effective_response_model,
                frozen=frozen,
                produces_type=produces_type,
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
        source_name = self._source_name or type(self).__name__.lower()

        # 1. Determine which resources to build
        selected: dict[str, _ResourceMeta] = {}
        for key, meta in self._resources.items():
            if resource_names and key not in resource_names:
                continue
            if not meta.selected and not (resource_names and key in resource_names):
                continue
            selected[key] = meta

        # 2. First pass: resolve all functions and detect dependencies
        prepared: dict[str, tuple[Callable[..., Any], str | None, _ResourceMeta]] = {}
        for key, meta in selected.items():
            if meta.deprecated:
                logger.warning("Resource '{}' is deprecated", key)
            bound = _resolve_env_params(meta.func)
            bound = self._bind_source(bound)
            bound, parent_name = _resolve_parent_deps(bound, self._type_registry)
            if meta.response_model is not None:
                bound = _wrap_with_validation(
                    bound, meta.response_model, meta.frozen, key
                )
            prepared[key] = (bound, parent_name, meta)

        # 3. Auto-include parents not in selection
        for key, (_, parent_name, _) in list(prepared.items()):
            if parent_name and parent_name not in prepared:
                pmeta = self._resources[parent_name]
                bound = _resolve_env_params(pmeta.func)
                bound = self._bind_source(bound)
                bound, _ = _resolve_parent_deps(bound, self._type_registry)
                if pmeta.response_model is not None:
                    bound = _wrap_with_validation(
                        bound, pmeta.response_model, pmeta.frozen, parent_name
                    )
                prepared[parent_name] = (bound, None, pmeta)

        # 4. Topological build: roots first, then children
        built: dict[str, Any] = {}
        pending = dict(prepared)
        while pending:
            progress = False
            for key in list(pending):
                bound, parent_name, meta = pending[key]
                res_kwargs: dict[str, Any] = {
                    "name": meta.table_name or key,
                    "primary_key": meta.primary_key,
                    "write_disposition": meta.write_disposition,
                }
                if meta.merge_key:
                    res_kwargs["merge_key"] = meta.merge_key

                if parent_name is None:
                    built[key] = dlt.resource(bound, **res_kwargs)
                    del pending[key]
                    progress = True
                elif parent_name in built:
                    built[key] = dlt.transformer(
                        bound, data_from=built[parent_name], **res_kwargs
                    )
                    del pending[key]
                    progress = True
            if not progress:
                raise ValueError(
                    f"Circular or unresolvable dependencies: {list(pending)}"
                )

        dlt_resources = list(built.values())

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
            all_hints = inspect.get_annotations(func, eval_str=True)
        except NameError:
            # Annotation references a locally-defined class that can't be resolved
            all_hints = inspect.get_annotations(func, eval_str=False)
        sig = inspect.signature(func)
        # Intersect with current signature to handle wrapped functions
        hints = {k: v for k, v in all_hints.items() if k in sig.parameters}

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

    def get_children(self, resource_name: str) -> list[str]:
        """Return resources that depend on this resource's output type."""
        meta = self._resources[resource_name]
        if meta.produces_type is None:
            return []
        children = []
        for key, m in self._resources.items():
            if key == resource_name:
                continue
            try:
                hints = inspect.get_annotations(m.func, eval_str=True)
            except NameError:
                hints = inspect.get_annotations(m.func, eval_str=False)
            sig = inspect.signature(m.func)
            for param_name in sig.parameters:
                hint = hints.get(param_name)
                if hint is meta.produces_type:
                    children.append(key)
                    break
        return children

    def get_resource_tree(self) -> dict[str, list[str]]:
        """Return {parent: [children]} dependency mapping."""
        tree: dict[str, list[str]] = {}
        for key in self._resources:
            children = self.get_children(key)
            if children:
                tree[key] = children
        return tree
