"""CLI presentation helpers with optional Rich rendering."""

from collections.abc import Iterable, Sequence
from contextlib import contextmanager

import typer


def _rich_available() -> bool:
    try:
        import rich  # noqa: F401
    except ImportError:
        return False
    return True


def info(msg: str) -> None:
    typer.secho(msg, fg=typer.colors.CYAN)


def success(msg: str) -> None:
    typer.secho(msg, fg=typer.colors.GREEN)


def warn(msg: str) -> None:
    typer.secho(msg, fg=typer.colors.YELLOW)


def error(msg: str) -> None:
    typer.secho(msg, fg=typer.colors.RED, err=True)


def print_kv(title: str, items: Sequence[tuple[str, object]]) -> None:
    if _rich_available():
        from rich.console import Console
        from rich.panel import Panel

        body = "\n".join(f"[bold]{key}[/bold]: {value}" for key, value in items)
        Console().print(Panel(body, title=title))
        return

    typer.echo(title)
    for key, value in items:
        typer.echo(f"  {key}: {value}")


def print_table(
    title: str,
    columns: Sequence[str],
    rows: Iterable[Sequence[object]],
) -> None:
    rendered_rows = [[str(cell) for cell in row] for row in rows]
    if _rich_available():
        from rich import box
        from rich.console import Console
        from rich.table import Table

        table = Table(title=title, box=box.SIMPLE, show_lines=False)
        for column in columns:
            table.add_column(column, overflow="fold")
        for row in rendered_rows:
            table.add_row(*row)
        Console().print(table)
        return

    widths = [len(column) for column in columns]
    for row in rendered_rows:
        for index, cell in enumerate(row):
            widths[index] = max(widths[index], len(cell))
    row_format = "  ".join(f"{{:<{width}}}" for width in widths)
    typer.echo(title)
    typer.echo(row_format.format(*columns))
    typer.echo(row_format.format(*("-" * len(column) for column in columns)))
    for row in rendered_rows:
        typer.echo(row_format.format(*row))


@contextmanager
def status(message: str):
    if _rich_available():
        from rich.console import Console

        with Console().status(message):
            yield
        return
    info(message)
    yield
