from __future__ import annotations

from typing import Iterator

from pydantic import BaseModel, Field

from fastelt.extractor import create_extractor_registration
from fastelt.types import PluginGroup


def _make_parquet_extractor(model: type[BaseModel]) -> PluginGroup:
    """Create a Parquet extractor plugin for a given record model."""

    def extract_parquet(
        path: str = Field(..., description="Path to Parquet file"),
    ) -> Iterator[BaseModel]:
        try:
            import pyarrow.parquet as pq
        except ImportError as e:
            raise ImportError(
                "pyarrow is required for Parquet support. "
                "Install with: pip install fastelt[parquet]"
            ) from e

        table = pq.read_table(path)
        for row in table.to_pylist():
            yield model(**row)

    extract_parquet.__annotations__["return"] = Iterator[model]  # type: ignore[valid-type]

    reg = create_extractor_registration("parquet", extract_parquet)
    return PluginGroup(extractors={"parquet": reg})


parquet_extractor = _make_parquet_extractor
