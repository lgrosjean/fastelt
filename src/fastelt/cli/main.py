from __future__ import annotations

import importlib
import sys
from pathlib import Path
from typing import Annotated

try:
    import typer
except ImportError:
    raise ImportError(
        "typer is required for CLI support. Install with: pip install fastelt[cli]"
    )

from loguru import logger

from fastelt.app import FastELT

cli_app = typer.Typer(name="fastelt", help="FastELT CLI — run dlt pipelines with FastAPI-style DX")

AppPathOpt = Annotated[
    str | None,
    typer.Option("--app", help="App import path (module:attr)"),
]
DestinationOpt = Annotated[
    str | None,
    typer.Option("--destination", "-d", help="dlt destination (duckdb, postgres, bigquery, ...)"),
]
DatasetOpt = Annotated[
    str | None,
    typer.Option("--dataset", help="Dataset/schema name at the destination"),
]
SourceOpt = Annotated[
    str | None,
    typer.Option("--source", "-s", help="Run only this source"),
]
ResourcesOpt = Annotated[
    list[str] | None,
    typer.Option("--resource", "-r", help="Run only these resources (repeatable)"),
]


def _find_fastelt_in_module(module: object) -> FastELT | None:
    """Find the first FastELT instance in a module's attributes."""
    for attr_name in dir(module):
        obj = getattr(module, attr_name, None)
        if isinstance(obj, FastELT):
            return obj
    return None


def _auto_discover_app() -> tuple[str, str, FastELT]:
    """Search for a FastELT app like FastAPI does — scan .py files in cwd."""
    cwd = Path.cwd()

    # 1. Try conventional file first
    for name in ("fastelt_app", "main", "app"):
        candidate = cwd / f"{name}.py"
        if candidate.exists():
            module = importlib.import_module(name)
            app = _find_fastelt_in_module(module)
            if app is not None:
                return name, type(app).__name__.lower(), app

    # 2. Search for package directories with __init__.py
    for init_file in sorted(cwd.rglob("__init__.py")):
        pkg_dir = init_file.parent
        for py_file in sorted(pkg_dir.glob("*.py")):
            if py_file.name == "__init__.py":
                continue
            rel = py_file.relative_to(cwd).with_suffix("")
            module_path = ".".join(rel.parts)
            try:
                module = importlib.import_module(module_path)
            except Exception:
                continue
            app = _find_fastelt_in_module(module)
            if app is not None:
                return module_path, type(app).__name__.lower(), app

    # 3. Search loose .py files in cwd
    for py_file in sorted(cwd.glob("*.py")):
        module_name = py_file.stem
        if module_name.startswith("_"):
            continue
        try:
            module = importlib.import_module(module_name)
        except Exception:
            continue
        app = _find_fastelt_in_module(module)
        if app is not None:
            return module_name, type(app).__name__.lower(), app

    raise typer.Exit(code=1)


def _discover_app(app_path: str | None = None) -> FastELT:
    """Discover and import the FastELT app instance."""
    cwd = str(Path.cwd())
    if cwd not in sys.path:
        sys.path.insert(0, cwd)

    if app_path:
        module_path, _, attr = app_path.partition(":")
        attr = attr or "app"
        module = importlib.import_module(module_path)
        app = getattr(module, attr)
        if not isinstance(app, FastELT):
            raise TypeError(f"{module_path}:{attr} is not a FastELT instance")
        logger.info("Using import string: {}:{}", module_path, attr)
        return app

    logger.debug("Searching for FastELT app in package file structure...")
    module_path, attr, app = _auto_discover_app()
    logger.info("Discovered app in '{}'", module_path)
    return app


@cli_app.command()
def run(
    app_path: AppPathOpt = None,
    destination: DestinationOpt = None,
    dataset: DatasetOpt = None,
    source: SourceOpt = None,
    resources: ResourcesOpt = None,
) -> None:
    """Run the pipeline — extract from sources, load to destination."""
    app = _discover_app(app_path)

    kwargs: dict = {}
    if destination:
        kwargs["destination"] = destination
    if dataset:
        kwargs["dataset_name"] = dataset
    if source:
        kwargs["source"] = source
    if resources:
        kwargs["resources"] = resources

    app.run(**kwargs)
    typer.echo("Pipeline completed successfully.")


@cli_app.command("list")
def list_components(
    app_path: AppPathOpt = None,
) -> None:
    """List registered sources and their resources."""
    app = _discover_app(app_path)

    sources = app.list_sources()
    if not sources:
        typer.echo("No sources registered.")
        return

    all_resources = app.list_resources()
    for src_name in sources:
        res_names = all_resources.get(src_name, [])
        typer.echo(f"Source: {src_name} ({len(res_names)} resources)")
        for res_name in res_names:
            typer.echo(f"  - {res_name}")


@cli_app.command()
def describe(
    name: Annotated[str, typer.Argument(help="Source or source:resource name")],
    app_path: AppPathOpt = None,
) -> None:
    """Describe a source or resource."""
    app = _discover_app(app_path)

    if ":" in name:
        # source:resource
        src_name, res_name = name.split(":", 1)
        src = app.get_source(src_name)
        meta = src.get_resource_meta(res_name)
        typer.echo(f"Resource: {src_name}:{res_name}")
        if meta.description:
            typer.echo(f"  Description: {meta.description}")
        if meta.deprecated:
            typer.echo("  Status: DEPRECATED")
        if meta.tags:
            typer.echo(f"  Tags: {', '.join(meta.tags)}")
        typer.echo(f"  Write disposition: {meta.write_disposition}")
        if meta.primary_key:
            pk = meta.primary_key if isinstance(meta.primary_key, str) else ", ".join(meta.primary_key)
            typer.echo(f"  Primary key: {pk}")
        if meta.merge_key:
            mk = meta.merge_key if isinstance(meta.merge_key, str) else ", ".join(meta.merge_key)
            typer.echo(f"  Merge key: {mk}")
    elif name in app.list_sources():
        src = app.get_source(name)
        typer.echo(f"Source: {name}")
        typer.echo(f"  Resources: {', '.join(src.list_resources())}")
    else:
        typer.echo(f"'{name}' not found. Use 'fastelt list' to see available sources.")
        raise typer.Exit(1)
