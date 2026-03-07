from __future__ import annotations

import json as json_mod
from typing import Iterator

from pydantic import BaseModel, Field

from fastelt.extractor import create_extractor_registration
from fastelt.types import PluginGroup


def _make_json_extractor(model: type[BaseModel]) -> PluginGroup:
    """Create a JSON extractor plugin for a given record model."""

    def extract_json(
        path: str = Field(..., description="Path to JSON file"),
    ) -> Iterator[BaseModel]:
        with open(path) as f:
            data = json_mod.load(f)
        if isinstance(data, list):
            for item in data:
                yield model(**item) if isinstance(item, dict) else item
        else:
            yield model(**data) if isinstance(data, dict) else data

    extract_json.__annotations__["return"] = Iterator[model]  # type: ignore[valid-type]

    reg = create_extractor_registration("json", extract_json)
    return PluginGroup(extractors={"json": reg})


json_extractor = _make_json_extractor
