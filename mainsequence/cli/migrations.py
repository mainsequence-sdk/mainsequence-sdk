from __future__ import annotations

import dataclasses
import json
from typing import Any

import click
import typer

from mainsequence.client.metatables import (
    AlembicMigrationDirection,
    AlembicMigrationOperation,
    AlembicMigrationStatusRequest,
    MetaTable,
)
from mainsequence.meta_tables.migrations import (
    AlembicMetaTableMigration,
    PackagedAlembicMigrationArtifact,
    load_alembic_metatable_migration_provider,
    render_packaged_alembic_migration_for_provider,
)

migrations = typer.Typer(help="Alembic-backed MetaTable migration commands")


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


def _registry_uid(migration: AlembicMetaTableMigration) -> str:
    uid = migration.alembic_registry.get_meta_table_uid()
    if uid in (None, ""):
        raise typer.BadParameter(
            "Alembic registry MetaTable UID is not bound. Run register-version-table "
            "or set __metatable_uid__ on the registry class.",
            param_hint="--provider",
        )
    return str(uid)


def _ensure_registry(
    migration: AlembicMetaTableMigration,
    *,
    data_source_uid: str | None,
    timeout: float | None,
) -> None:
    migration.ensure_alembic_registry(data_source_uid=data_source_uid, timeout=timeout)


def _status_request(migration: AlembicMetaTableMigration) -> AlembicMigrationStatusRequest:
    return AlembicMigrationStatusRequest(
        alembic_version_meta_table_uid=_registry_uid(migration),
        data_source_uid=migration.resolve_data_source_uid(),
        package=migration.package,
        migration_namespace=migration.migration_namespace,
    )


def _render_artifact(
    migration: AlembicMetaTableMigration,
    *,
    target_revision: str,
    direction: AlembicMigrationDirection,
    current_revision: str | None,
    sqlalchemy_url: str,
) -> PackagedAlembicMigrationArtifact:
    return render_packaged_alembic_migration_for_provider(
        migration,
        revision=target_revision,
        direction=direction,
        current_revision=current_revision,
        sqlalchemy_url=sqlalchemy_url,
    )


def _operation(
    migration: AlembicMetaTableMigration,
    *,
    artifact: PackagedAlembicMigrationArtifact,
    expected_current_revision: str | None,
    dry_run: bool,
) -> AlembicMigrationOperation:
    return AlembicMigrationOperation(
        alembic_version_meta_table_uid=_registry_uid(migration),
        data_source_uid=migration.resolve_data_source_uid(),
        package=migration.package,
        migration_namespace=migration.migration_namespace,
        revision=str(artifact.manifest["revision"]),
        down_revision=artifact.manifest.get("down_revision"),
        direction=artifact.manifest.get("direction", "upgrade"),
        expected_current_revision=expected_current_revision,
        manifest=artifact.manifest,
        sql=artifact.sql,
        statement_boundaries=artifact.statement_boundaries,
        dry_run=dry_run,
    )


def _alembic_config(
    migration: AlembicMetaTableMigration,
    *,
    sqlalchemy_url: str,
) -> Any:
    try:
        from alembic.config import Config
    except ImportError as exc:
        raise RuntimeError("Alembic is required for migration CLI commands.") from exc

    config = Config()
    config.set_main_option("script_location", migration.script_location)
    config.set_main_option("sqlalchemy.url", sqlalchemy_url)
    config.attributes["mainsequence_migration_provider"] = migration
    config.attributes["target_metadata"] = migration.target_metadata
    config.attributes["alembic_version_table"] = migration.alembic_version_table
    config.attributes["version_table"] = migration.version_table
    config.attributes["version_table_schema"] = migration.version_table_schema
    return config


@migrations.command("register-version-table")
def register_version_table(
    provider: str | None = typer.Option(
        None,
        "--provider",
        help="Migration provider reference, for example msm.migrations:migration.",
    ),
    data_source_uid: str | None = typer.Option(
        None,
        "--data-source-uid",
        help="Explicit override for cross-data-source workflows.",
    ),
    timeout: float | None = typer.Option(None, "--timeout"),
    json_output: bool = typer.Option(False, "--json", help="Emit JSON."),
) -> None:
    """Register the provider's Alembic version table as an external MetaTable."""

    migration = _load_migration(provider)
    meta_table = migration.register_alembic_registry(
        data_source_uid=data_source_uid,
        timeout=timeout,
    )
    _emit(
        {
            "uid": migration.alembic_registry.get_meta_table_uid()
            or getattr(meta_table, "uid", None),
            "data_source_uid": migration.alembic_registry.get_data_source_uid(),
            "alembic_version_table": migration.alembic_version_table,
            "package": migration.package,
            "migration_namespace": migration.migration_namespace,
        },
        json_output=json_output,
    )


@migrations.command("current")
def current(
    provider: str | None = typer.Option(
        None,
        "--provider",
        help="Migration provider reference, for example msm.migrations:migration.",
    ),
    data_source_uid: str | None = typer.Option(
        None,
        "--data-source-uid",
        help="Explicit override for cross-data-source workflows.",
    ),
    timeout: float | None = typer.Option(None, "--timeout"),
    json_output: bool = typer.Option(False, "--json", help="Emit JSON."),
) -> None:
    """Read current Alembic revision through the provider's registry MetaTable."""

    migration = _load_migration(provider)
    _ensure_registry(migration, data_source_uid=data_source_uid, timeout=timeout)
    status = MetaTable.get_migration_status(_status_request(migration), timeout=timeout)
    _emit(status, json_output=json_output)


@migrations.command("revision")
def revision(
    message: str = typer.Option(..., "--message", "-m", help="Alembic revision message."),
    autogenerate: bool = typer.Option(False, "--autogenerate", help="Use Alembic autogenerate."),
    provider: str | None = typer.Option(
        None,
        "--provider",
        help="Migration provider reference, for example msm.migrations:migration.",
    ),
    rev_id: str | None = typer.Option(None, "--rev-id", help="Explicit Alembic revision id."),
    head: str = typer.Option("head", "--head", help="Alembic head to base the revision on."),
    sqlalchemy_url: str = typer.Option(
        "postgresql://",
        "--sqlalchemy-url",
        help="SQLAlchemy URL passed to the Alembic environment.",
    ),
    json_output: bool = typer.Option(False, "--json", help="Emit JSON."),
) -> None:
    """Create a normal Alembic revision for the selected provider."""

    try:
        from alembic import command
    except ImportError as exc:
        raise typer.BadParameter("Alembic is required for revision generation.") from exc

    migration = _load_migration(provider)
    script = command.revision(
        _alembic_config(migration, sqlalchemy_url=sqlalchemy_url),
        message=message,
        autogenerate=autogenerate,
        rev_id=rev_id,
        head=head,
    )
    _emit(
        {
            "revision": getattr(script, "revision", None),
            "path": getattr(script, "path", None),
            "package": migration.package,
            "migration_namespace": migration.migration_namespace,
        },
        json_output=json_output,
    )


@migrations.command("render")
def render(
    target_revision: str = typer.Option("head", "--to", help="Target Alembic revision."),
    direction: str = typer.Option(
        "upgrade",
        "--direction",
        help="Alembic direction to render.",
    ),
    current_revision: str | None = typer.Option(
        None,
        "--from-revision",
        help="Current/source revision. Defaults to base for initial upgrades.",
    ),
    provider: str | None = typer.Option(
        None,
        "--provider",
        help="Migration provider reference, for example msm.migrations:migration.",
    ),
    sqlalchemy_url: str = typer.Option(
        "postgresql://",
        "--sqlalchemy-url",
        help="SQLAlchemy URL passed to the Alembic environment.",
    ),
    json_output: bool = typer.Option(False, "--json", help="Emit JSON."),
) -> None:
    """Render Alembic SQL without applying it locally or remotely."""

    migration = _load_migration(provider)
    if direction not in {"upgrade", "downgrade"}:
        raise typer.BadParameter("direction must be 'upgrade' or 'downgrade'.")
    artifact = _render_artifact(
        migration,
        target_revision=target_revision,
        direction=direction,
        current_revision=current_revision,
        sqlalchemy_url=sqlalchemy_url,
    )
    if json_output or _json_output_enabled():
        _emit(artifact, json_output=True)
        return
    typer.echo(artifact.sql, nl=False)


@migrations.command("upgrade")
def upgrade(
    target_revision: str = typer.Option("head", "--to", help="Target Alembic revision."),
    provider: str | None = typer.Option(
        None,
        "--provider",
        help="Migration provider reference, for example msm.migrations:migration.",
    ),
    data_source_uid: str | None = typer.Option(
        None,
        "--data-source-uid",
        help="Explicit override for cross-data-source workflows.",
    ),
    apply: bool = typer.Option(False, "--apply", help="Apply after validating dry-run."),
    dry_run: bool = typer.Option(False, "--dry-run", help="Validate without executing SQL."),
    register_metatables: bool = typer.Option(
        False,
        "--register-metatables",
        help="Register provider MetaTable models after a successful apply.",
    ),
    sqlalchemy_url: str = typer.Option(
        "postgresql://",
        "--sqlalchemy-url",
        help="SQLAlchemy URL passed to the Alembic environment.",
    ),
    timeout: float | None = typer.Option(None, "--timeout"),
    json_output: bool = typer.Option(False, "--json", help="Emit JSON."),
) -> None:
    """Dry-run or apply an Alembic-rendered SQL artifact through the backend."""

    if apply and dry_run:
        raise typer.BadParameter("Use either --apply or --dry-run, not both.")

    migration = _load_migration(provider)
    _ensure_registry(migration, data_source_uid=data_source_uid, timeout=timeout)
    status = MetaTable.get_migration_status(_status_request(migration), timeout=timeout)
    current_revision = status.current_revision
    artifact = _render_artifact(
        migration,
        target_revision=target_revision,
        direction="upgrade",
        current_revision=current_revision,
        sqlalchemy_url=sqlalchemy_url,
    )

    validation_operation = _operation(
        migration,
        artifact=artifact,
        expected_current_revision=current_revision,
        dry_run=True,
    )
    validation = MetaTable.apply_migration(validation_operation, timeout=timeout)
    if not apply:
        _emit(validation, json_output=json_output)
        return
    if not validation.ok:
        _emit(validation, json_output=json_output)
        raise typer.Exit(code=1)

    apply_operation = validation_operation.model_copy(update={"dry_run": False})
    result = MetaTable.apply_migration(apply_operation, timeout=timeout)
    registered = []
    if register_metatables and result.ok:
        registered = migration.register_metatables(timeout=timeout)

    if json_output or _json_output_enabled():
        _emit(
            {"validation": validation, "apply": result, "registered": registered}, json_output=True
        )
        return
    _emit(result, json_output=False)
