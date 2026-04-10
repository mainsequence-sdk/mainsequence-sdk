from __future__ import annotations

"""
mainsequence.cli.ui
===================

Small UX helpers:
- Colored messages (works everywhere)
- Optional Rich tables/panels/spinners if `rich` is installed
- Graceful fallbacks when `rich` is unavailable
"""

from collections.abc import Iterable, Sequence
from contextlib import contextmanager
from dataclasses import dataclass

import click
import typer


@dataclass(frozen=True)
class RichSupport:
    available: bool


def rich_support() -> RichSupport:
    """Return whether Rich is available."""
    try:
        import rich  # noqa: F401

        return RichSupport(available=True)
    except Exception:
        return RichSupport(available=False)


def _json_output_enabled() -> bool:
    ctx = click.get_current_context(silent=True)
    if ctx is None:
        return False
    root = ctx.find_root()
    obj = getattr(root, "obj", None) or {}
    return bool(obj.get("json_output"))


def info(msg: str) -> None:
    """Informational message."""
    if _json_output_enabled():
        return
    typer.secho(msg, fg=typer.colors.CYAN)


def success(msg: str) -> None:
    """Success message."""
    if _json_output_enabled():
        return
    typer.secho(msg, fg=typer.colors.GREEN)


def warn(msg: str) -> None:
    """Warning message."""
    if _json_output_enabled():
        return
    typer.secho(msg, fg=typer.colors.YELLOW)


def error(msg: str) -> None:
    """Error message."""
    typer.secho(msg, fg=typer.colors.RED, err=True)


def print_kv(title: str, items: Sequence[tuple[str, str]]) -> None:
    """
    Print key-value data in a nice way (Rich Panel when available).
    """
    rs = rich_support()
    if rs.available:
        from rich.console import Console
        from rich.panel import Panel

        console = Console()
        body = "\n".join([f"[bold]{k}[/bold]: {v}" for k, v in items])
        console.print(Panel(body, title=title))
        return

    typer.echo(title)
    for k, v in items:
        typer.echo(f"  {k}: {v}")


def print_table(title: str, columns: Sequence[str], rows: Iterable[Sequence[str]]) -> None:
    """
    Print a table (Rich Table when available, else aligned text).
    """
    rs = rich_support()
    rows = list(rows)

    if rs.available:
        from rich import box
        from rich.console import Console
        from rich.table import Table

        table = Table(title=title, box=box.SIMPLE, show_lines=False)
        for c in columns:
            table.add_column(c, overflow="fold")
        for r in rows:
            table.add_row(*[str(x) for x in r])
        Console().print(table)
        return

    # fallback: aligned
    widths = [len(c) for c in columns]
    for r in rows:
        for i, cell in enumerate(r):
            widths[i] = max(widths[i], len(str(cell)))

    fmt = "  ".join("{:<" + str(w) + "}" for w in widths)
    typer.echo(title)
    typer.echo(fmt.format(*columns))
    typer.echo(fmt.format(*["-" * len(c) for c in columns]))
    for r in rows:
        typer.echo(fmt.format(*[str(x) for x in r]))


@contextmanager
def status(message: str):
    """
    Context manager for a progress spinner if Rich is available.
    """
    if _json_output_enabled():
        yield
        return
    rs = rich_support()
    if rs.available:
        from rich.console import Console

        console = Console()
        with console.status(message):
            yield
    else:
        info(message)
        yield
