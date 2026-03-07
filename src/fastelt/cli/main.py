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

cli_app = typer.Typer(name="fastelt", help="FastELT CLI")

ExtractorArg = Annotated[str, typer.Argument(help="Extractor name")]
LoaderArg = Annotated[str, typer.Argument(help="Loader name")]
NameArg = Annotated[str, typer.Argument(help="Component name")]
ExtractorConfigOpt = Annotated[
    list[str] | None,
    typer.Option("--extractor-config", "-e", help="Extractor config as KEY=VALUE"),
]
LoaderConfigOpt = Annotated[
    list[str] | None,
    typer.Option("--loader-config", "-l", help="Loader config as KEY=VALUE"),
]
AppPathOpt = Annotated[
    str | None,
    typer.Option("--app", help="App import path (module:attr)"),
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


def _parse_config(values: list[str] | None) -> dict[str, str]:
    """Parse KEY=VAL config pairs."""
    if not values:
        return {}
    config: dict[str, str] = {}
    for item in values:
        key, _, val = item.partition("=")
        if not _:
            raise typer.BadParameter(f"Config must be KEY=VALUE, got: {item}")
        config[key] = val
    return config


@cli_app.command()
def run(
    extractor: ExtractorArg,
    loader: LoaderArg,
    extractor_config: ExtractorConfigOpt = None,
    loader_config: LoaderConfigOpt = None,
    app_path: AppPathOpt = None,
) -> None:
    """Run a pipeline from extractor to loader."""
    app = _discover_app(app_path)
    ext_conf = _parse_config(extractor_config)
    ldr_conf = _parse_config(loader_config)
    app.run(
        extractor=extractor,
        loader=loader,
        extractor_config=ext_conf,
        loader_config=ldr_conf,
    )
    typer.echo("Pipeline completed successfully.")


@cli_app.command("list")
def list_components(
    app_path: AppPathOpt = None,
    tag: Annotated[str | None, typer.Option("--tag", "-t", help="Filter by tag")] = None,
) -> None:
    """List registered extractors and loaders."""
    app = _discover_app(app_path)
    typer.echo("Extractors:")
    for name in app.list_extractors():
        reg = app.get_extractor(name)
        if tag and tag not in reg.tags:
            continue
        suffix = ""
        if reg.deprecated:
            suffix += " (deprecated)"
        if reg.tags:
            suffix += f" [{', '.join(reg.tags)}]"
        typer.echo(f"  - {name}{suffix}")
    typer.echo("Loaders:")
    for name in app.list_loaders():
        typer.echo(f"  - {name}")


@cli_app.command()
def describe(
    name: NameArg,
    app_path: AppPathOpt = None,
) -> None:
    """Describe a component's config schema."""
    app = _discover_app(app_path)

    if name in app.list_extractors():
        reg = app.get_extractor(name)
        typer.echo(f"Extractor: {name}")
        if reg.description:
            typer.echo(f"Description: {reg.description}")
        if reg.deprecated:
            typer.echo("Status: DEPRECATED")
        if reg.tags:
            typer.echo(f"Tags: {', '.join(reg.tags)}")
        typer.echo(f"Record type: {reg.record_type.__name__}")
        if reg.primary_key:
            pk = reg.primary_key if isinstance(reg.primary_key, str) else ", ".join(reg.primary_key)
            typer.echo(f"Primary key: {pk}")
    elif name in app.list_loaders():
        reg = app.get_loader(name)
        typer.echo(f"Loader: {name}")
        record_name = reg.record_type.__name__ if reg.record_type else "any"
        typer.echo(f"Record type: {record_name}")
    else:
        typer.echo(f"Component '{name}' not found.")
        raise typer.Exit(1)

    typer.echo("Config schema:")
    schema = reg.config_model.model_json_schema()
    props = schema.get("properties", {})
    required = set(schema.get("required", []))
    for field_name, field_info in props.items():
        req = " (required)" if field_name in required else ""
        ftype = field_info.get("type", "any")
        desc = field_info.get("description", "")
        line = f"  {field_name}: {ftype}{req}"
        if desc:
            line += f" - {desc}"
        typer.echo(line)
