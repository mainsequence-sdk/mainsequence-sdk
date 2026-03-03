import importlib
import json
import os
import sys

import typer
from pydantic import BaseModel, ValidationError

from mainsequence import logger

app = typer.Typer(add_completion=False)

_ENTRYPOINT_ERR = (
    "JOB_API_ENTRY_POINT must be "
    "'package.module:ClassName' (preferred) or 'package.module.ClassName'."
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


def read_input_dict(input_json: str | None) -> dict:
    raw = None

    if input_json is not None and input_json.strip():
        raw = input_json
    else:
        raw = os.getenv("JOB_API_INPUT")
        if (raw is None or not raw.strip()) and not sys.stdin.isatty():
            raw = sys.stdin.read()

    if raw is None or not raw.strip():
        return {}

    try:
        payload = json.loads(raw)
    except json.JSONDecodeError as exc:
        typer.secho(f"Invalid JSON input: {exc}", err=True, fg=typer.colors.RED)
        raise typer.Exit(code=2)

    if not isinstance(payload, dict):
        typer.secho(
            "JOB_API_INPUT / INPUT_JSON must be a JSON object (dict).",
            err=True,
            fg=typer.colors.RED,
        )
        raise typer.Exit(code=2)

    return payload


@app.command("run_job_api")
def run_job_api(
    input_json: str | None = typer.Argument(
        None,
        help="Input as a JSON object. If omitted, uses $JOB_API_INPUT or stdin.",
    ),
    entry_point: str | None = typer.Option(
        None,
        "--entry-point",
        envvar="JOB_API_ENTRY_POINT",
        help="JobApi entrypoint: package.module:ClassName (preferred) or package.module.ClassName.",
    ),
) -> None:
    if not entry_point:
        typer.secho(
            "Missing JOB_API_ENTRY_POINT env var (or pass --entry-point).",
            err=True,
            fg=typer.colors.RED,
        )
        raise typer.Exit(code=2)

    ApiClass = load_class(entry_point)
    input_dict = read_input_dict(input_json)

    input_cls = getattr(ApiClass, "input_model", None)

    if input_cls and isinstance(input_cls, type) and issubclass(input_cls, BaseModel):
        try:
            input_obj = input_cls(**input_dict)
        except ValidationError as exc:
            typer.secho(str(exc), err=True, fg=typer.colors.RED)
            raise typer.Exit(code=2)
        api = ApiClass(input_data=input_obj)
    else:
        api = ApiClass(input_data=input_dict)

    api.run_and_report()


@app.command("register_job_api")
def register_job_api(
    entry_point: str | None = typer.Option(
        None,
        "--entry-point",
        envvar="JOB_API_ENTRY_POINT",
        help="JobApi entrypoint: package.module:ClassName (preferred) or package.module.ClassName.",
    ),
) -> None:
    if not entry_point:
        typer.secho(
            "Missing JOB_API_ENTRY_POINT env var (or pass --entry-point).",
            err=True,
            fg=typer.colors.RED,
        )
        raise typer.Exit(code=2)

    try:
        ApiClass = load_class(entry_point)
    except Exception:
        logger.exception("Could not load JobApi class for entrypoint %s", entry_point)
        raise

    ApiClass.register_to_backend()


def main() -> None:
    app()


if __name__ == "__main__":
    main()