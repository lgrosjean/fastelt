from __future__ import annotations

import csv as csv_mod
from typing import Iterator

from pydantic import BaseModel, Field

from fastelt.extractor import create_extractor_registration
from fastelt.types import PluginGroup


def _make_csv_extractor(model: type[BaseModel]) -> PluginGroup:
    """Create a CSV extractor plugin for a given record model."""

    def extract_csv(
        path: str = Field(..., description="Path to CSV file"),
        delimiter: str = Field(default=",", description="CSV delimiter"),
    ) -> Iterator[BaseModel]:
        with open(path) as f:
            for row in csv_mod.DictReader(f, delimiter=delimiter):
                yield model(**row)

    # Fix return annotation to use the concrete model
    extract_csv.__annotations__["return"] = Iterator[model]  # type: ignore[valid-type]

    reg = create_extractor_registration("csv", extract_csv)
    return PluginGroup(extractors={"csv": reg})


csv_extractor = _make_csv_extractor
