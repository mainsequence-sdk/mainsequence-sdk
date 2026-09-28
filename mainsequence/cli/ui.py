"""Colored messages for the SDK authentication and settings CLI."""

import typer


def success(msg: str) -> None:
    typer.secho(msg, fg=typer.colors.GREEN)


def warn(msg: str) -> None:
    typer.secho(msg, fg=typer.colors.YELLOW)


def error(msg: str) -> None:
    typer.secho(msg, fg=typer.colors.RED, err=True)
