"""fastELT — a FastAPI-inspired wrapper around dlt for ELT pipelines.

Like FastAPI wraps Starlette, fastELT wraps dlt with decorator-driven DX.
You get dlt's battle-tested engine (20+ destinations, incremental loading,
schema evolution, merge strategies) with FastAPI's developer experience.
"""

from fastelt.app import FastELT
from fastelt.rest_api import RESTAPISource
from fastelt.types import Env, Secret, Source

__all__ = [
    "Env",
    "FastELT",
    "RESTAPISource",
    "Secret",
    "Source",
]
