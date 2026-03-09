"""fastELT — a FastAPI-inspired wrapper around dlt for ELT pipelines.

Like FastAPI wraps Starlette, fastELT wraps dlt with decorator-driven DX.
You get dlt's battle-tested engine (20+ destinations, incremental loading,
schema evolution, merge strategies) with FastAPI's developer experience.
"""

from fastelt.app import FastELT
from fastelt.config import Env, Secret
from fastelt.destinations import BigQueryDestination, Destination, DuckDBDestination
from fastelt.sources.rest_api import RESTAPISource
from fastelt.sources.types import Incremental
from fastelt.types import SchemaFrozenError, Source

__all__ = [
    "BigQueryDestination",
    "Destination",
    "DuckDBDestination",
    "Env",
    "FastELT",
    "Incremental",
    "RESTAPISource",
    "SchemaFrozenError",
    "Secret",
    "Source",
]
