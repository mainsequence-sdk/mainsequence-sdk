# cli.py
import importlib
import json
import os
import sys

import typer
from pydantic import BaseModel, ValidationError

from mainsequence import logger

app = typer.Typer(add_completion=False)

_ENTRYPOINT_ERR = (
    "TOOL_ENTRY_POINT must be 'package.module:ClassName' (preferred) "
    "or 'package.module.ClassName'."
)


def load_class(entry_point: str):
    entry_point = (entry_point or "").strip()
    if not entry_point:
        raise typer.Exit(code=2)

    if ":" in entry_point:
        module_path, class_name = entry_point.split(":", 1)
    else:
        if "." not in entry_point:
            typer.secho(_ENTRYPOINT_ERR, err=True, fg=typer.colors.RED)
            raise typer.Exit(code=2)
        module_path, class_name = entry_point.rsplit(".", 1)

    module = importlib.import_module(module_path)
    try:
        return getattr(module, class_name)
    except AttributeError:
        typer.secho(
            f"Class '{class_name}' not found in module '{module_path}'.",
            err=True,
            fg=typer.colors.RED,
        )
        raise typer.Exit(code=2)


def read_configuration_dict(config_json: str | None) -> dict:
    # Priority: CLI arg -> $TOOL_CONFIGURATION -> stdin (if piped)
    raw = None

    if config_json is not None and config_json.strip():
        raw = config_json
    else:
        raw = os.getenv("TOOL_CONFIGURATION")
        if (raw is None or not raw.strip()) and not sys.stdin.isatty():
            raw = sys.stdin.read()

    if raw is None or not raw.strip():
        return {}

    try:
        cfg = json.loads(raw)
    except json.JSONDecodeError as e:
        typer.secho(f"Invalid JSON configuration: {e}", err=True, fg=typer.colors.RED)
        raise typer.Exit(code=2)

    if not isinstance(cfg, dict):
        typer.secho(
            "TOOL_CONFIGURATION / CONFIG_JSON must be a JSON object (dict).",
            err=True,
            fg=typer.colors.RED,
        )
        raise typer.Exit(code=2)

    return cfg


@app.command("run_tool")
def run_tool(
    config_json: str | None = typer.Argument(
        None,
        help="Configuration as a JSON object. If omitted, uses $TOOL_CONFIGURATION or stdin.",
    ),
    entry_point: str | None = typer.Option(
        None,
        "--entry-point",
        envvar="TOOL_ENTRY_POINT",
        help="Tool entrypoint: package.module:ClassName (preferred) or package.module.ClassName. "
        "Defaults from $TOOL_ENTRY_POINT.",
    ),
) -> None:
    if not entry_point:
        typer.secho("Missing TOOL_ENTRY_POINT env var (or pass --entry-point).", err=True, fg=typer.colors.RED)
        raise typer.Exit(code=2)

    ToolClass = load_class(entry_point)
    cfg_dict = read_configuration_dict(config_json)

    cfg_cls = getattr(ToolClass, "configuration_class", None)
    if not (isinstance(cfg_cls, type) and issubclass(cfg_cls, BaseModel)):
        typer.secho(
            f"{ToolClass.__module__}.{ToolClass.__name__} must define "
            f"'configuration_class' as a pydantic BaseModel subclass.",
            err=True,
            fg=typer.colors.RED,
        )
        raise typer.Exit(code=2)

    try:
        config_obj = cfg_cls(**cfg_dict)
    except ValidationError as e:
        typer.secho(str(e), err=True, fg=typer.colors.RED)
        raise typer.Exit(code=2)

    tool = ToolClass(configuration=config_obj)
    tool.run_and_response()


@app.command("register_tool")
def register_tool(
    entry_point: str | None = typer.Option(
        None,
        "--entry-point",
        envvar="TOOL_ENTRY_POINT",
        help="Tool entrypoint: package.module:ClassName (preferred) or package.module.ClassName. "
        "Defaults from $TOOL_ENTRY_POINT.",
    ),
) -> None:
    if not entry_point:
        typer.secho("Missing TOOL_ENTRY_POINT env var (or pass --entry-point).", err=True, fg=typer.colors.RED)
        raise typer.Exit(code=2)

    try:
        ToolClass = load_class(entry_point)
    except Exception as e:
        logger.exception(f"Couldnt load tool class for entrypoint {entry_point}")
        raise e

    ToolClass.register_to_backend()


def main() -> None:
    app()


if __name__ == "__main__":
    main()
