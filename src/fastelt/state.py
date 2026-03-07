"""Pipeline state persistence — JSON file backend.

Stores incremental cursor values so that subsequent pipeline runs
can resume from where they left off.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from loguru import logger


class StateStore:
    """Simple JSON-file state backend.

    State is stored at ``{state_dir}/{extractor_name}.json``.
    Each file contains a dict mapping incremental parameter names
    to their persisted cursor values.
    """

    def __init__(self, state_dir: str | Path = ".fastelt/state") -> None:
        self.state_dir = Path(state_dir)

    def _path(self, extractor_name: str) -> Path:
        safe_name = extractor_name.replace(":", "__")
        return self.state_dir / f"{safe_name}.json"

    def load(self, extractor_name: str) -> dict[str, Any]:
        """Load persisted state for an extractor.  Returns ``{}`` on first run."""
        path = self._path(extractor_name)
        if not path.exists():
            logger.debug("No prior state for '{}' (first run)", extractor_name)
            return {}
        data = json.loads(path.read_text())
        logger.debug("Loaded state for '{}': {}", extractor_name, data)
        return data

    def save(self, extractor_name: str, state: dict[str, Any]) -> None:
        """Persist state for an extractor."""
        self.state_dir.mkdir(parents=True, exist_ok=True)
        path = self._path(extractor_name)
        path.write_text(json.dumps(state, indent=2))
        logger.debug("Saved state for '{}': {}", extractor_name, state)
