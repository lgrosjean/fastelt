from __future__ import annotations

import csv as csv_mod

from pydantic import BaseModel, Field

from fastelt.loader import create_loader_registration
from fastelt.types import PluginGroup, Records


def _make_csv_loader(model: type[BaseModel]) -> PluginGroup:
    """Create a CSV loader plugin for a given record model."""

    def load_csv(
        records: Records[BaseModel],
        path: str = Field(..., description="Output CSV file path"),
        delimiter: str = Field(default=",", description="CSV delimiter"),
    ) -> None:
        rows = records.collect()
        if not rows:
            return
        fieldnames = list(rows[0].model_dump().keys())
        with open(path, "w", newline="") as f:
            writer = csv_mod.DictWriter(f, fieldnames=fieldnames, delimiter=delimiter)
            writer.writeheader()
            for row in rows:
                writer.writerow(row.model_dump())

    load_csv.__annotations__["records"] = Records[model]  # type: ignore[valid-type]

    reg = create_loader_registration("csv", load_csv)
    return PluginGroup(loaders={"csv": reg})


csv_loader = _make_csv_loader
