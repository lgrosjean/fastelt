"""Incremental loading support — cursor-based state tracking.

Declare ``Incremental[T]`` in an extractor signature to enable automatic
cursor tracking between pipeline runs.  The pipeline injects a live
instance with ``last_value`` loaded from persisted state.

Usage::

    from fastelt import Incremental

    @github.entity(primary_key="id", write_disposition="merge")
    def repositories(
        updated_at: Incremental[datetime] = Incremental(
            "updated_at", initial_value=datetime(2000, 1, 1)
        ),
    ) -> Iterator[Repository]:
        since = updated_at.last_value
        for item in fetch_repos(since=since):
            yield item
"""

from __future__ import annotations

import copy
from datetime import date, datetime
from typing import Any, Generic, TypeVar

T = TypeVar("T")


class Incremental(Generic[T]):
    """Injectable cursor for incremental loading.

    Works like ``Field(...)`` in Pydantic — it's both a type annotation
    (``Incremental[datetime]``) and a default value that configures behavior.

    Parameters
    ----------
    cursor_path:
        Field name on the record model whose values are tracked.
    initial_value:
        Cursor value used on the very first run (no prior state).
    last_value_func:
        Aggregation function applied to cursor values to compute the
        new state.  Defaults to ``max`` (most common for timestamps/IDs).
    """

    __slots__ = (
        "cursor_path",
        "initial_value",
        "last_value_func",
        "_last_value",
        "_end_value",
    )

    def __init__(
        self,
        cursor_path: str,
        *,
        initial_value: T | None = None,
        last_value_func: Any = max,
    ) -> None:
        self.cursor_path = cursor_path
        self.initial_value = initial_value
        self.last_value_func = last_value_func
        self._last_value: T | None = None
        self._end_value: T | None = None

    @property
    def last_value(self) -> T | None:
        """Cursor value from the previous run (or *initial_value* on first run)."""
        if self._last_value is not None:
            return self._last_value
        return self.initial_value

    @property
    def end_value(self) -> T | None:
        """Cursor value computed from this run's records."""
        return self._end_value

    def _track(self, value: Any) -> None:
        """Update the running cursor with a new record's field value."""
        if value is None:
            return
        if self._end_value is None:
            self._end_value = value
        else:
            self._end_value = self.last_value_func(self._end_value, value)

    def _clone(self) -> Incremental[T]:
        """Create a fresh copy for injection into a pipeline run."""
        clone = copy.copy(self)
        clone._last_value = None
        clone._end_value = None
        return clone

    # -- Serialization helpers for state persistence --

    def _serialize(self, value: Any) -> Any:
        """Convert a cursor value to a JSON-safe representation."""
        if isinstance(value, datetime):
            return {"__type__": "datetime", "v": value.isoformat()}
        if isinstance(value, date):
            return {"__type__": "date", "v": value.isoformat()}
        return value

    def _deserialize(self, raw: Any) -> T:
        """Reconstruct a cursor value from its JSON representation."""
        if isinstance(raw, dict) and "__type__" in raw:
            type_tag = raw["__type__"]
            if type_tag == "datetime":
                return datetime.fromisoformat(raw["v"])  # type: ignore[return-value]
            if type_tag == "date":
                return date.fromisoformat(raw["v"])  # type: ignore[return-value]
        return raw  # type: ignore[return-value]

    def __repr__(self) -> str:
        return (
            f"Incremental({self.cursor_path!r}, "
            f"initial_value={self.initial_value!r})"
        )
