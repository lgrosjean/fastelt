from __future__ import annotations

import json as json_mod

from pydantic import BaseModel, Field

from fastelt.loader import create_loader_registration
from fastelt.types import PluginGroup, Records


def _make_json_loader(model: type[BaseModel]) -> PluginGroup:
    """Create a JSON loader plugin for a given record model."""

    def load_json(
        records: Records[BaseModel],
        path: str = Field(..., description="Output JSON file path"),
    ) -> None:
        with open(path, "w") as f:
            json_mod.dump([r.model_dump() for r in records], f)

    load_json.__annotations__["records"] = Records[model]  # type: ignore[valid-type]

    reg = create_loader_registration("json", load_json)
    return PluginGroup(loaders={"json": reg})


json_loader = _make_json_loader
