from __future__ import annotations

import dataclasses
import json
import re
from typing import Any

import click
import typer

from mainsequence.client.metatables import (
    DynamicTableDataSource,
    DynamicTableDataSourceMigrationConnectionRequest,
)
from mainsequence.meta_tables.migrations import (
    AlembicMetaTableMigration,
    alembic_config_for_provider,
    load_alembic_metatable_migration_provider,
)

migrations = typer.Typer(help="Alembic-owned MetaTable migration commands")


def _load_migration(provider: str | None) -> AlembicMetaTableMigration:
    try:
        return load_alembic_metatable_migration_provider(provider)
    except Exception as exc:
        raise typer.BadParameter(str(exc), param_hint="--provider") from exc


def _jsonable(value: Any) -> Any:
    if hasattr(value, "model_dump"):
        return value.model_dump(mode="json")
    if dataclasses.is_dataclass(value):
        return dataclasses.asdict(value)
    if isinstance(value, dict):
        return {str(key): _jsonable(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_jsonable(item) for item in value]
    return value


def _emit(payload: Any, *, json_output: bool = False) -> None:
    if json_output or _json_output_enabled():
        typer.echo(json.dumps(_jsonable(payload), indent=2, ensure_ascii=False))
        return
    if isinstance(payload, str):
        typer.echo(payload)
        return
    for key, value in _jsonable(payload).items():
        typer.echo(f"{key}: {value}")


def _json_output_enabled() -> bool:
    ctx = click.get_current_context(silent=True)
    if ctx is None:
        return False
    root = ctx.find_root()
    obj = getattr(root, "obj", None) or {}
    return bool(obj.get("json_output"))


def _prepare_alembic_config(
    migration: AlembicMetaTableMigration,
    *,
    timeout: float | None,
    ttl_seconds: int,
) -> tuple[Any, Any]:
    migration.ensure_alembic_registry(timeout=timeout)
    prepared = migration.prepare_for_alembic(timeout=timeout)
    data_source = DynamicTableDataSource.get_by_uid(prepared.data_source_uid)
    connection = data_source.issue_migration_connection(
        DynamicTableDataSourceMigrationConnectionRequest(
            package=migration.package,
            migration_namespace=migration.migration_namespace,
            meta_table_uids=prepared.meta_table_uids,
            ttl_seconds=ttl_seconds,
        ),
        timeout=timeout,
    )
    config = alembic_config_for_provider(
        migration,
        sqlalchemy_url=connection.uri,
        owner_role_name=connection.owner_role_name or prepared.owner_role_name,
    )
    return prepared, config


def _next_sequential_revision_id(migration: AlembicMetaTableMigration) -> str:
    try:
        from alembic.script import ScriptDirectory
    except ImportError as exc:
        raise RuntimeError("Alembic is required for revision generation.") from exc

    script = ScriptDirectory.from_config(
        alembic_config_for_provider(migration, sqlalchemy_url="postgresql://")
    )
    heads = list(script.get_heads())
    if len(heads) > 1:
        raise typer.BadParameter(
            "Sequential revision IDs require a single Alembic head. Pass --rev-id "
            "explicitly for branched histories.",
            param_hint="--rev-id",
        )
    if heads and not re.fullmatch(r"\d{4,}", str(heads[0])):
        raise typer.BadParameter(
            "Sequential revision IDs require the current Alembic head to be numeric. "
            "Pass --rev-id explicitly for non-numeric histories.",
            param_hint="--rev-id",
        )

    numeric_revisions: list[int] = []
    for revision in script.walk_revisions():
        revision_id = str(revision.revision)
        if re.fullmatch(r"\d{4,}", revision_id):
            numeric_revisions.append(int(revision_id))
    return f"{max(numeric_revisions, default=0) + 1:04d}"


@migrations.command("current")
def current(
    provider: str | None = typer.Option(
        None,
        "--provider",
        help="Migration provider reference, for example msm.migrations:migration.",
    ),
    verbose: bool = typer.Option(False, "--verbose", "-v"),
    timeout: float | None = typer.Option(None, "--timeout"),
    ttl_seconds: int = typer.Option(900, "--ttl-seconds", min=1),
) -> None:
    """Read current Alembic revision through a scoped migration credential."""

    try:
        from alembic import command
    except ImportError as exc:
        raise typer.BadParameter("Alembic is required for migration commands.") from exc

    migration = _load_migration(provider)
    _, config = _prepare_alembic_config(
        migration,
        timeout=timeout,
        ttl_seconds=ttl_seconds,
    )
    command.current(config, verbose=verbose)


@migrations.command("revision")
def revision(
    message: str | None = typer.Option(
        None,
        "--message",
        "-m",
        help="Alembic revision message. Defaults to 'migration'.",
    ),
    autogenerate: bool = typer.Option(
        True,
        "--autogenerate/--no-autogenerate",
        help="Use Alembic autogenerate against the reserved MetaTable metadata.",
    ),
    provider: str | None = typer.Option(
        None,
        "--provider",
        help="Migration provider reference, for example msm.migrations:migration.",
    ),
    rev_id: str | None = typer.Option(None, "--rev-id", help="Explicit Alembic revision id."),
    head: str = typer.Option("head", "--head", help="Alembic head to base the revision on."),
    timeout: float | None = typer.Option(None, "--timeout"),
    ttl_seconds: int = typer.Option(900, "--ttl-seconds", min=1),
    json_output: bool = typer.Option(False, "--json", help="Emit JSON."),
) -> None:
    """Create a normal Alembic revision for the selected provider."""

    try:
        from alembic import command
    except ImportError as exc:
        raise typer.BadParameter("Alembic is required for revision generation.") from exc

    migration = _load_migration(provider)
    resolved_message = (message or "").strip() or "migration"
    resolved_rev_id = rev_id or _next_sequential_revision_id(migration)
    prepared, config = _prepare_alembic_config(
        migration,
        timeout=timeout,
        ttl_seconds=ttl_seconds,
    )
    script = command.revision(
        config,
        message=resolved_message,
        autogenerate=autogenerate,
        rev_id=resolved_rev_id,
        head=head,
    )
    _emit(
        {
            "revision": getattr(script, "revision", None),
            "path": getattr(script, "path", None),
            "package": migration.package,
            "migration_namespace": migration.migration_namespace,
            "meta_table_uids": prepared.meta_table_uids,
        },
        json_output=json_output,
    )


@migrations.command("upgrade")
def upgrade(
    target_revision: str = typer.Argument("head", help="Target Alembic revision."),
    provider: str | None = typer.Option(
        None,
        "--provider",
        help="Migration provider reference, for example msm.migrations:migration.",
    ),
    timeout: float | None = typer.Option(None, "--timeout"),
    ttl_seconds: int = typer.Option(900, "--ttl-seconds", min=1),
    json_output: bool = typer.Option(False, "--json", help="Emit JSON."),
) -> None:
    """Run Alembic upgrade directly and refresh MetaTable catalog rows."""

    try:
        from alembic import command
    except ImportError as exc:
        raise typer.BadParameter("Alembic is required for migration commands.") from exc

    migration = _load_migration(provider)
    prepared, config = _prepare_alembic_config(
        migration,
        timeout=timeout,
        ttl_seconds=ttl_seconds,
    )
    command.upgrade(config, target_revision)
    registered = migration.refresh_metatable_catalog(timeout=timeout)
    _emit(
        {
            "ok": True,
            "revision": target_revision,
            "package": migration.package,
            "migration_namespace": migration.migration_namespace,
            "meta_table_uids": prepared.meta_table_uids,
            "registered_count": len(registered),
        },
        json_output=json_output,
    )


@migrations.command("downgrade")
def downgrade(
    target_revision: str = typer.Argument(..., help="Target Alembic downgrade revision."),
    provider: str | None = typer.Option(
        None,
        "--provider",
        help="Migration provider reference, for example msm.migrations:migration.",
    ),
    timeout: float | None = typer.Option(None, "--timeout"),
    ttl_seconds: int = typer.Option(900, "--ttl-seconds", min=1),
    json_output: bool = typer.Option(False, "--json", help="Emit JSON."),
) -> None:
    """Run Alembic downgrade directly and refresh MetaTable catalog rows."""

    try:
        from alembic import command
    except ImportError as exc:
        raise typer.BadParameter("Alembic is required for migration commands.") from exc

    migration = _load_migration(provider)
    prepared, config = _prepare_alembic_config(
        migration,
        timeout=timeout,
        ttl_seconds=ttl_seconds,
    )
    command.downgrade(config, target_revision)
    registered = migration.refresh_metatable_catalog(timeout=timeout)
    _emit(
        {
            "ok": True,
            "revision": target_revision,
            "package": migration.package,
            "migration_namespace": migration.migration_namespace,
            "meta_table_uids": prepared.meta_table_uids,
            "registered_count": len(registered),
        },
        json_output=json_output,
    )
