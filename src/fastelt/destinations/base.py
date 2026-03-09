"""Base Destination and CustomDestination classes."""

from __future__ import annotations

import re
from collections.abc import Callable
from typing import Any

import dlt
from pydantic import BaseModel, ConfigDict, create_model

from fastelt.config import Env


def _class_name_to_destination_name(cls_name: str) -> str:
    """Convert class name to destination name.

    ``BigQueryDestination`` → ``"bigquery"``
    ``DuckDBDestination``   → ``"duckdb"``
    ``MyWarehouse``         → ``"mywarehouse"``
    """
    name = re.sub(r"Destination$", "", cls_name)
    return name.lower()


class Destination(BaseModel):
    """Base destination config.

    The ``name`` is auto-derived from the class name
    (e.g. ``BigQueryDestination`` → ``"bigquery"``).

    Subclass to create specific destinations::

        class BigQueryDestination(Destination):
            destination_type: str = "bigquery"
            project_id: str
            location: str = "US"
    """

    model_config = ConfigDict(arbitrary_types_allowed=True)

    destination_type: str
    dataset_name: str | None = None

    @property
    def name(self) -> str:
        """Destination name, derived from class name."""
        return _class_name_to_destination_name(type(self).__name__)

    def __init__(self, **kwargs: Any) -> None:
        resolved = {
            k: v.resolve() if isinstance(v, Env) else v
            for k, v in kwargs.items()
        }
        super().__init__(**resolved)

    def __new__(cls, **kwargs: Any) -> Destination:
        if cls is Destination:
            resolved = {
                k: v.resolve() if isinstance(v, Env) else v
                for k, v in kwargs.items()
            }
            fields = {
                k: (type(v), v)
                for k, v in resolved.items()
                if k not in Destination.model_fields
            }
            dynamic_cls = create_model("Destination", __base__=Destination, **fields)  # type: ignore[call-overload]
            return dynamic_cls(**resolved)
        return super().__new__(cls)

    def _to_dlt_kwargs(self) -> dict[str, Any]:
        """Return kwargs for ``dlt.pipeline()``."""
        kwargs: dict[str, Any] = {"destination": self.destination_type}
        if self.dataset_name:
            kwargs["dataset_name"] = self.dataset_name
        return kwargs


class CustomDestination(Destination):
    """Destination backed by a user-defined sink function.

    Created by ``@app.destination()`` — wraps ``@dlt.destination`` with
    FastAPI-style decorator DX.

    Usage::

        @app.destination(batch_size=100)
        def my_sink(items, table):
            for item in items:
                print(f"{table['name']}: {item}")

        app.run(destination=my_sink)
    """

    model_config = ConfigDict(arbitrary_types_allowed=True)

    destination_type: str = "custom"
    batch_size: int = 10
    loader_file_format: str | None = None
    naming_convention: str = "direct"
    skip_dlt_columns_and_tables: bool = True
    max_table_nesting: int = 0

    _func: Callable[..., Any] | None = None

    def __init__(self, **kwargs: Any) -> None:
        func = kwargs.pop("_func", None)
        super().__init__(**kwargs)
        object.__setattr__(self, "_func", func)

    @property
    def name(self) -> str:
        if self._func is not None:
            return self._func.__name__
        return super().name

    def _to_dlt_kwargs(self) -> dict[str, Any]:
        """Build dlt.destination from the wrapped function."""
        if self._func is None:
            raise ValueError("CustomDestination has no sink function")

        dest = dlt.destination(
            name=self.name,
            batch_size=self.batch_size,
            loader_file_format=self.loader_file_format,
            naming_convention=self.naming_convention,
            skip_dlt_columns_and_tables=self.skip_dlt_columns_and_tables,
            max_table_nesting=self.max_table_nesting,
        )(self._func)

        kwargs: dict[str, Any] = {"destination": dest}
        if self.dataset_name:
            kwargs["dataset_name"] = self.dataset_name
        return kwargs
