from typing import Iterator

import pytest
from pydantic import BaseModel, Field

from fastelt import FastELT, Records


class Item(BaseModel):
    name: str
    value: int


def test_register_extractor():
    app = FastELT()

    @app.extractor("test_ext")
    def my_extractor(path: str = Field(...)) -> Iterator[Item]:
        yield Item(name="a", value=1)

    assert "test_ext" in app.list_extractors()
    reg = app.get_extractor("test_ext")
    assert reg.record_type is Item
    assert "path" in reg.config_model.model_fields


def test_register_loader_with_records():
    app = FastELT()

    @app.loader("test_ldr")
    def my_loader(records: Records[Item], path: str = Field(...)) -> None:
        pass

    assert "test_ldr" in app.list_loaders()
    reg = app.get_loader("test_ldr")
    assert reg.record_type is Item
    assert reg.records_param == "records"
    assert "path" in reg.config_model.model_fields
    assert "records" not in reg.config_model.model_fields


def test_register_loader_without_records():
    app = FastELT()

    @app.loader("simple_ldr")
    def my_loader(path: str = Field(...)) -> None:
        pass

    assert "simple_ldr" in app.list_loaders()
    reg = app.get_loader("simple_ldr")
    assert reg.record_type is None
    assert reg.records_param is None
    assert "path" in reg.config_model.model_fields


def test_register_batch_extractor():
    app = FastELT()

    @app.extractor("batch_ext")
    def my_batch_extractor(path: str = Field(...)) -> list[Item]:
        return [Item(name="a", value=1), Item(name="b", value=2)]

    assert "batch_ext" in app.list_extractors()
    reg = app.get_extractor("batch_ext")
    assert reg.record_type is Item


def test_extractor_must_have_valid_return_type():
    app = FastELT()
    with pytest.raises(TypeError, match="Iterator\\[T\\] or list\\[T\\]"):

        @app.extractor("bad")
        def bad_return(path: str = Field(...)) -> str:
            return "not records"


def test_run_unknown_extractor():
    app = FastELT()
    with pytest.raises(KeyError, match="Extractor"):
        app.run(extractor="nope", loader="nope")


def test_run_unknown_loader():
    app = FastELT()

    @app.extractor("ext")
    def my_ext() -> Iterator[Item]:
        yield Item(name="a", value=1)

    with pytest.raises(KeyError, match="Loader"):
        app.run(extractor="ext", loader="nope")


def test_include_plugin():
    from fastelt.types import PluginGroup, ExtractorRegistration
    from fastelt._utils import build_config_model

    def dummy_ext() -> Iterator[Item]:
        yield Item(name="x", value=0)

    reg = ExtractorRegistration(
        name="plug_ext",
        func=dummy_ext,
        config_model=build_config_model(dummy_ext),
        record_type=Item,
    )
    plugin = PluginGroup(extractors={"plug_ext": reg})

    app = FastELT()
    app.include(plugin)
    assert "plug_ext" in app.list_extractors()


def test_extractor_metadata():
    app = FastELT()

    @app.extractor(
        description="Extract items from CSV",
        tags=["core", "csv"],
        deprecated=True,
        primary_key="name",
    )
    def items(path: str = Field(...)) -> Iterator[Item]:
        yield Item(name="a", value=1)

    reg = app.get_extractor("items")
    assert reg.description == "Extract items from CSV"
    assert reg.tags == ["core", "csv"]
    assert reg.deprecated is True
    assert reg.primary_key == "name"


def test_extractor_metadata_defaults():
    app = FastELT()

    @app.extractor()
    def items() -> Iterator[Item]:
        """Docstring as description."""
        yield Item(name="a", value=1)

    reg = app.get_extractor("items")
    assert reg.description == "Docstring as description."
    assert reg.tags == []
    assert reg.deprecated is False
    assert reg.primary_key is None


def test_extractor_composite_primary_key():
    app = FastELT()

    @app.extractor(primary_key=["repo", "title"])
    def prs() -> Iterator[Item]:
        yield Item(name="pr", value=1)

    reg = app.get_extractor("prs")
    assert reg.primary_key == ["repo", "title"]
