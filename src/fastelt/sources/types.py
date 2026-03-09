"""Source-level types for fastELT."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import dlt


@dataclass(frozen=True, slots=True)
class Incremental:
    """Declarative incremental loading marker — like FastAPI's ``Query()``.

    Use with ``Annotated`` to declare incremental cursor parameters::

        from fastelt.sources import Incremental

        @source.resource(primary_key="id", write_disposition="merge")
        def events(
            updated_at: Annotated[str, Incremental(initial_value="2024-01-01")],
        ) -> Iterator[dict]:
            print(f"Loading since {updated_at.last_value}")
            yield {"id": 1, "updated_at": "2024-06-01"}

    This replaces the raw dlt syntax::

        # Before (dlt-style):
        def events(updated_at=dlt.sources.incremental("updated_at", initial_value="2024-01-01")):

        # After (FastAPI-style):
        def events(updated_at: Annotated[str, Incremental(initial_value="2024-01-01")]):

    Parameters
    ----------
    cursor_path:
        JSON path to the cursor field in yielded records.
        Defaults to the parameter name.
    initial_value:
        Starting cursor value for the first run.
    end_value:
        Optional end value to stop incremental loading.
    row_order:
        ``"asc"`` or ``"desc"`` — order of rows by cursor.
    allow_external_schedulers:
        Whether external schedulers can control the range.
    """

    cursor_path: str | None = None
    initial_value: Any = None
    end_value: Any = None
    row_order: str | None = None
    allow_external_schedulers: bool = False

    def resolve(self, param_name: str) -> dlt.sources.incremental:
        """Build a ``dlt.sources.incremental`` instance."""
        cursor = self.cursor_path or param_name
        kwargs: dict[str, Any] = {}
        if self.initial_value is not None:
            kwargs["initial_value"] = self.initial_value
        if self.end_value is not None:
            kwargs["end_value"] = self.end_value
        if self.row_order is not None:
            kwargs["row_order"] = self.row_order
        if self.allow_external_schedulers:
            kwargs["allow_external_schedulers"] = True
        return dlt.sources.incremental(cursor, **kwargs)
