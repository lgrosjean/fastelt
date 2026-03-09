"""Shared config primitives for fastELT.

``Env`` and ``Secret`` are used by both sources and destinations
to declaratively reference environment variables.
"""

from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class Env:
    """Lazy reference to an environment variable.

    Usage::

        from fastelt.config import Env

        # As a value (resolved at Source/Destination construction):
        github = Source(name="github", token=Env("GH_TOKEN"))

        # As an Annotated type hint (resolved at resource call time):
        @source.resource()
        def repos(token: Annotated[str, Env("GH_TOKEN")]):
            ...
    """

    var: str
    default: str | None = None

    def resolve(self) -> str:
        """Return the current value of the environment variable."""
        value = os.environ.get(self.var, self.default)
        if value is None:
            raise EnvironmentError(
                f"Environment variable '{self.var}' is not set and no default was provided"
            )
        return value


@dataclass(frozen=True, slots=True)
class Secret(Env):
    """Like ``Env`` but masks the value in logs and repr.

    Use for sensitive values (API keys, tokens, passwords).

    Usage::

        from fastelt.config import Secret

        github = Source(name="github", token=Secret("GH_TOKEN"))
    """

    def __repr__(self) -> str:
        return f"Secret({self.var!r})"
