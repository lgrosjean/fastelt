# Custom Source: MLflow

This example shows how to build a custom FastELT source that extracts experiment data from [MLflow](https://mlflow.org/) and loads it into DuckDB for analytics.

## About this example

You will learn how to:

- Create a **typed Source subclass** with shared config (tracking URI, credentials)
- Register **multiple resources** on the same source (experiments, runs, metrics)
- Use **`Env`** and **`Secret`** for environment variable resolution
- Use **`Annotated[str, Incremental(...)]`** for efficient incremental syncing
- Use **`response_model`** for Pydantic validation of extracted records

## Full example

```python
"""MLflow → DuckDB pipeline with FastELT."""

from __future__ import annotations

from datetime import datetime
from typing import Annotated, Iterator

import httpx
from pydantic import BaseModel, field_validator

from fastelt import FastELT, Source
from fastelt.config import Env, Secret
from fastelt.destinations import DuckDBDestination
from fastelt.sources import Incremental


# ---------------------------------------------------------------------------
# 1. Pydantic models for validation (like FastAPI's response_model)
# ---------------------------------------------------------------------------


class Experiment(BaseModel):
    experiment_id: str
    name: str
    lifecycle_stage: str
    artifact_location: str

    @field_validator("lifecycle_stage")
    @classmethod
    def must_be_valid_stage(cls, v: str) -> str:
        if v not in ("active", "deleted"):
            raise ValueError(f"Invalid lifecycle_stage: {v}")
        return v


class Run(BaseModel):
    run_id: str
    experiment_id: str
    status: str
    start_time: int
    end_time: int | None = None
    artifact_uri: str | None = None

    @field_validator("status")
    @classmethod
    def must_be_valid_status(cls, v: str) -> str:
        valid = ("RUNNING", "SCHEDULED", "FINISHED", "FAILED", "KILLED")
        if v not in valid:
            raise ValueError(f"Invalid status: {v}")
        return v


class Metric(BaseModel):
    run_id: str
    key: str
    value: float
    timestamp: int
    step: int


# ---------------------------------------------------------------------------
# 2. Source — shared config for the MLflow API (like FastAPI's APIRouter)
# ---------------------------------------------------------------------------


class MLflowSource(Source):
    """MLflow tracking server source."""

    tracking_uri: str
    token: str | None = None

    @property
    def headers(self) -> dict[str, str]:
        h: dict[str, str] = {"Content-Type": "application/json"}
        if self.token:
            h["Authorization"] = f"Bearer {self.token}"
        return h

    def get(self, path: str, **params) -> dict:
        resp = httpx.get(
            f"{self.tracking_uri}/api/2.0/mlflow{path}",
            headers=self.headers,
            params=params,
        )
        resp.raise_for_status()
        return resp.json()


mlflow = MLflowSource(
    tracking_uri=Env("MLFLOW_TRACKING_URI", default="http://localhost:5000"),
    token=Secret("MLFLOW_TOKEN", default=None),
)


# ---------------------------------------------------------------------------
# 3. Resources — generator functions that yield records
# ---------------------------------------------------------------------------


@mlflow.resource(
    primary_key="experiment_id",
    write_disposition="merge",
    response_model=Experiment,
)
def experiments(source: MLflowSource) -> Iterator[dict]:
    """Fetch all experiments from MLflow."""
    data = source.get("/experiments/list")
    yield from data.get("experiments", [])


@mlflow.resource(
    primary_key="run_id",
    write_disposition="merge",
    response_model=Run,
)
def runs(
    source: MLflowSource,
    start_time: Annotated[str, Incremental(initial_value="0")],
) -> Iterator[dict]:
    """Fetch runs, incrementally by start_time."""
    for exp in experiments(source):
        data = source.get(
            "/runs/search",
            experiment_ids=exp["experiment_id"],
        )
        for run in data.get("runs", []):
            info = run.get("info", {})
            yield {
                "run_id": info["run_id"],
                "experiment_id": info["experiment_id"],
                "status": info["status"],
                "start_time": info["start_time"],
                "end_time": info.get("end_time"),
                "artifact_uri": info.get("artifact_uri"),
            }


@mlflow.resource(
    primary_key=["run_id", "key", "timestamp"],
    write_disposition="append",
    response_model=Metric,
)
def metrics(source: MLflowSource) -> Iterator[dict]:
    """Fetch metrics for all runs."""
    for exp in experiments(source):
        data = source.get(
            "/runs/search",
            experiment_ids=exp["experiment_id"],
        )
        for run in data.get("runs", []):
            run_id = run["info"]["run_id"]
            run_metrics = run.get("data", {}).get("metrics", [])
            for m in run_metrics:
                yield {
                    "run_id": run_id,
                    "key": m["key"],
                    "value": m["value"],
                    "timestamp": m["timestamp"],
                    "step": m["step"],
                }


# ---------------------------------------------------------------------------
# 4. App — wire everything together
# ---------------------------------------------------------------------------


db = DuckDBDestination(database="mlflow.duckdb")

app = FastELT(pipeline_name="mlflow_pipeline")
app.include_destination(db)
app.include_source(mlflow)


if __name__ == "__main__":
    app.run(destination=db)
```

## Step-by-step walkthrough

### Typed Source with shared config

`MLflowSource` subclasses `Source` to define typed fields for the tracking server connection. Since `Source` extends Pydantic's `BaseModel`, you get validation, serialization, and IDE autocomplete for free:

```python
class MLflowSource(Source):
    tracking_uri: str
    token: str | None = None
```

`Env("MLFLOW_TRACKING_URI")` resolves the env var at construction time. `Secret` masks the token in logs.

### Response models for data quality

Each resource uses a Pydantic `response_model` to validate records as they're extracted — just like FastAPI validates HTTP responses:

```python
@mlflow.resource(
    primary_key="experiment_id",
    write_disposition="merge",
    response_model=Experiment,  # validates every yielded dict
)
def experiments(source: MLflowSource) -> Iterator[dict]:
    ...
```

Invalid records (e.g. an unknown `lifecycle_stage`) raise a `ValidationError` at extraction time, not at load time.

### Incremental loading

The `runs` resource uses `Annotated[str, Incremental(...)]` to track a cursor between pipeline runs — dlt remembers the last seen `start_time` and only processes newer records on the next run:

```python
def runs(
    source: MLflowSource,
    start_time: Annotated[str, Incremental(initial_value="0")],
) -> Iterator[dict]:
    ...
```

### Source injection

Resources declare `source: MLflowSource` as a parameter. FastELT detects the type annotation and injects the source instance automatically — no global variables needed:

```python
@mlflow.resource(...)
def experiments(source: MLflowSource) -> Iterator[dict]:
    data = source.get("/experiments/list")
    ...
```

## Running the pipeline

### With Python

```bash
export MLFLOW_TRACKING_URI="http://localhost:5000"
python mlflow_pipeline.py
```

### With the CLI

```bash
export MLFLOW_TRACKING_URI="http://localhost:5000"

# Run all resources
fastelt run duckdb

# Run only experiments
fastelt run duckdb mlflow -r experiments

# List available resources
fastelt list
```

### Querying the results

```python
import duckdb

conn = duckdb.connect("mlflow.duckdb")

# All experiments
conn.sql("SELECT * FROM mlflow_pipeline_data.experiments").show()

# Best runs by metric
conn.sql("""
    SELECT r.run_id, r.status, m.key, m.value
    FROM mlflow_pipeline_data.runs r
    JOIN mlflow_pipeline_data.metrics m ON r.run_id = m.run_id
    WHERE m.key = 'accuracy'
    ORDER BY m.value DESC
    LIMIT 10
""").show()
```
