from __future__ import annotations

from pydantic import BaseModel, Field

from fastelt.loader import create_loader_registration
from fastelt.types import PluginGroup, Records


def _make_parquet_loader(model: type[BaseModel]) -> PluginGroup:
    """Create a Parquet loader plugin for a given record model."""

    def load_parquet(
        records: Records[BaseModel],
        path: str = Field(..., description="Output Parquet file path"),
    ) -> None:
        try:
            import pyarrow as pa
            import pyarrow.parquet as pq
        except ImportError as e:
            raise ImportError(
                "pyarrow is required for Parquet support. "
                "Install with: pip install fastelt[parquet]"
            ) from e

        rows = [r.model_dump() for r in records]
        if not rows:
            return
        table = pa.Table.from_pylist(rows)
        pq.write_table(table, path)

    load_parquet.__annotations__["records"] = Records[model]  # type: ignore[valid-type]

    reg = create_loader_registration("parquet", load_parquet)
    return PluginGroup(loaders={"parquet": reg})


parquet_loader = _make_parquet_loader
