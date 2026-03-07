import json
from typing import Iterator

from pydantic import BaseModel, Field

from fastelt import FastELT, Records, Source


class Artifact(BaseModel):
    name: str
    version: int


class MlflowSource(Source):
    tracking_uri: str
    token: str = ""


def test_source_entity_explicit_type():
    """Entity with explicit Source type annotation."""
    mlflow = MlflowSource(tracking_uri="http://localhost:5000", token="secret")

    @mlflow.entity()
    def artifacts(source: MlflowSource, experiment_id: str = Field(default="1")) -> Iterator[Artifact]:
        assert source.tracking_uri == "http://localhost:5000"
        assert source.token == "secret"
        yield Artifact(name=f"exp-{experiment_id}", version=1)

    app = FastELT()
    app.include_source(mlflow)

    assert "artifacts" in app.list_extractors()
    reg = app.get_extractor("artifacts")
    assert "source" not in reg.config_model.model_fields
    assert "experiment_id" in reg.config_model.model_fields


def test_source_entity_no_annotation():
    """Entity with bare param (no type hint) — source injected by convention."""
    mlflow = MlflowSource(tracking_uri="http://localhost:5000", token="abc")

    @mlflow.entity()
    def artifacts(source, experiment_id: str = Field(default="1")) -> Iterator[Artifact]:
        assert source.tracking_uri == "http://localhost:5000"
        assert source.token == "abc"
        yield Artifact(name=f"exp-{experiment_id}", version=1)

    app = FastELT()
    app.include_source(mlflow)

    collected = []

    @app.loader()
    def sink(records: Records[Artifact]) -> None:
        collected.extend(records.collect())

    app.run(extractor="artifacts", loader="sink")
    assert collected[0].name == "exp-1"

    reg = app.get_extractor("artifacts")
    assert "source" not in reg.config_model.model_fields


def test_source_entity_custom_param_name():
    """The source param can be named anything."""
    mlflow = MlflowSource(tracking_uri="http://localhost:5000")

    @mlflow.entity()
    def artifacts(mlflow_src, tag: str = Field(default="latest")) -> Iterator[Artifact]:
        yield Artifact(name=f"{mlflow_src.tracking_uri}/{tag}", version=1)

    app = FastELT()
    app.include_source(mlflow)
    collected = []

    @app.loader()
    def sink(records: Records[Artifact]) -> None:
        collected.extend(records.collect())

    app.run(extractor="artifacts", loader="sink")
    assert collected[0].name == "http://localhost:5000/latest"


def test_source_entity_custom_name():
    """Entity can have a custom registration name."""
    mlflow = MlflowSource(tracking_uri="http://localhost:5000")

    @mlflow.entity("mlflow_artifacts")
    def artifacts(src) -> Iterator[Artifact]:
        yield Artifact(name="model", version=1)

    app = FastELT()
    app.include_source(mlflow)
    assert "mlflow_artifacts" in app.list_extractors()


def test_source_entity_without_injection():
    """Entity function without source param still works."""
    mlflow = MlflowSource(tracking_uri="http://localhost:5000")

    @mlflow.entity()
    def simple_ext() -> Iterator[Artifact]:
        yield Artifact(name="standalone", version=0)

    app = FastELT()
    app.include_source(mlflow)

    collected = []

    @app.loader()
    def sink(records: Records[Artifact]) -> None:
        collected.extend(records.collect())

    app.run(extractor="simple_ext", loader="sink")
    assert collected[0].name == "standalone"


def test_source_full_pipeline(tmp_path):
    """Full pipeline: source entity -> loader."""
    mlflow = MlflowSource(tracking_uri="http://mlflow.local", token="abc")

    @mlflow.entity()
    def models(source, stage: str = Field(default="production")) -> Iterator[Artifact]:
        yield Artifact(name=f"{source.tracking_uri}/{stage}/model-a", version=1)
        yield Artifact(name=f"{source.tracking_uri}/{stage}/model-b", version=2)

    out = tmp_path / "artifacts.json"
    app = FastELT()
    app.include_source(mlflow)

    @app.loader()
    def json_sink(records: Records[Artifact], path: str = Field(...)) -> None:
        with open(path, "w") as f:
            json.dump([r.model_dump() for r in records], f)

    app.run(
        extractor="models",
        loader="json_sink",
        extractor_config={"stage": "staging"},
        loader_config={"path": str(out)},
    )

    data = json.loads(out.read_text())
    assert len(data) == 2
    assert data[0]["name"] == "http://mlflow.local/staging/model-a"
    assert data[1]["version"] == 2


def test_multiple_entities_on_same_source():
    """A source can have multiple entities."""
    mlflow = MlflowSource(tracking_uri="http://localhost:5000")

    @mlflow.entity()
    def artifacts(src) -> Iterator[Artifact]:
        yield Artifact(name="artifact", version=1)

    @mlflow.entity()
    def experiments(src) -> Iterator[Artifact]:
        yield Artifact(name="experiment", version=2)

    app = FastELT()
    app.include_source(mlflow)

    assert set(app.list_extractors()) == {"artifacts", "experiments"}


def test_source_entity_metadata():
    """Entity decorator passes metadata to registration."""
    mlflow = MlflowSource(tracking_uri="http://localhost:5000")

    @mlflow.entity(
        description="Fetch ML artifacts",
        tags=["ml", "core"],
        deprecated=True,
        primary_key="name",
    )
    def artifacts(src) -> Iterator[Artifact]:
        yield Artifact(name="model", version=1)

    app = FastELT()
    app.include_source(mlflow)

    reg = app.get_extractor("artifacts")
    assert reg.description == "Fetch ML artifacts"
    assert reg.tags == ["ml", "core"]
    assert reg.deprecated is True
    assert reg.primary_key == "name"
