from __future__ import annotations

import dataclasses
import json
import re
from collections.abc import Callable, Mapping
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
            "Alembic registry MetaTable UID is not bound. Run current or upgrade "
            "to auto-register the registry, or set __metatable_uid__ on the registry class.",
            param_hint="--provider",
        )
    return str(uid)


def _model_reference(model: type[Any]) -> str:
    module = getattr(model, "__module__", None)
    qualname = getattr(model, "__qualname__", None)
    if module and qualname:
        return f"{module}.{qualname}"
    return repr(model)


def _meta_table_value(meta_table: Any, *names: str) -> Any:
    if isinstance(meta_table, Mapping):
        for name in names:
            value = meta_table.get(name)
            if value not in (None, ""):
                return value
        return None
    for name in names:
        value = getattr(meta_table, name, None)
        if value not in (None, ""):
            return value
    return None


def _print_metatable_resolution_callback(
    migration: AlembicMetaTableMigration,
) -> Callable[[type[Any], str, str, Any | None], None]:
    def _print(model: type[Any], identifier: str, status: str, meta_table: Any | None) -> None:
        fields = [
            f"identifier={identifier}",
            f"model={_model_reference(model)}",
            f"package={migration.package}",
            f"migration_namespace={migration.migration_namespace}",
        ]
        uid = _meta_table_value(meta_table, "uid", "meta_table_uid")
        if uid is not None:
            fields.append(f"uid={uid}")
        physical_table_name = _meta_table_value(meta_table, "physical_table_name")
        if physical_table_name is not None:
            fields.append(f"physical_table_name={physical_table_name}")
        typer.echo(f"migration MetaTable {status}: " + " ".join(fields), err=True)

    return _print


def _ensure_registry(
    migration: AlembicMetaTableMigration,
    *,
    timeout: float | None,
) -> None:
    migration.ensure_alembic_registry(timeout=timeout)


def _status_request(migration: AlembicMetaTableMigration) -> AlembicMigrationStatusRequest:
    return AlembicMigrationStatusRequest(
        alembic_version_meta_table_uid=_registry_uid(migration),
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


def _next_sequential_revision_id(migration: AlembicMetaTableMigration) -> str:
    try:
        from alembic.script import ScriptDirectory
    except ImportError as exc:
        raise RuntimeError("Alembic is required for revision generation.") from exc

    script = ScriptDirectory.from_config(
        _alembic_config(migration, sqlalchemy_url="postgresql://")
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
    timeout: float | None = typer.Option(None, "--timeout"),
    json_output: bool = typer.Option(False, "--json", help="Emit JSON."),
) -> None:
    """Read current Alembic revision through the provider's registry MetaTable."""

    migration = _load_migration(provider)
    _ensure_registry(migration, timeout=timeout)
    status = MetaTable.get_migration_status(_status_request(migration), timeout=timeout)
    _emit(status, json_output=json_output)


@migrations.command("revision")
def revision(
    message: str | None = typer.Option(
        None,
        "--message",
        "-m",
        help="Alembic revision message. Defaults to 'migration'.",
    ),
    autogenerate: bool = typer.Option(
        False,
        "--autogenerate",
        help="Use Alembic autogenerate. Requires --sqlalchemy-url.",
    ),
    provider: str | None = typer.Option(
        None,
        "--provider",
        help="Migration provider reference, for example msm.migrations:migration.",
    ),
    rev_id: str | None = typer.Option(None, "--rev-id", help="Explicit Alembic revision id."),
    head: str = typer.Option("head", "--head", help="Alembic head to base the revision on."),
    sqlalchemy_url: str | None = typer.Option(
        None,
        "--sqlalchemy-url",
        help="SQLAlchemy URL passed to the Alembic environment. Required for --autogenerate.",
    ),
    json_output: bool = typer.Option(False, "--json", help="Emit JSON."),
) -> None:
    """Create a normal Alembic revision for the selected provider."""

    try:
        from alembic import command
    except ImportError as exc:
        raise typer.BadParameter("Alembic is required for revision generation.") from exc

    migration = _load_migration(provider)
    resolved_message = (message or "").strip() or "migration"
    if autogenerate and not sqlalchemy_url:
        raise typer.BadParameter(
            "--sqlalchemy-url is required with --autogenerate because Alembic "
            "must connect to a baseline database to compute the diff.",
            param_hint="--sqlalchemy-url",
        )
    resolved_rev_id = rev_id or _next_sequential_revision_id(migration)
    script = command.revision(
        _alembic_config(migration, sqlalchemy_url=sqlalchemy_url or "postgresql://"),
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
    migration.resolve_or_register_metatable_models(
        on_metatable_resolution=_print_metatable_resolution_callback(migration),
    )
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
    dry_run: bool = typer.Option(False, "--dry-run", help="Validate without executing SQL."),
    sqlalchemy_url: str = typer.Option(
        "postgresql://",
        "--sqlalchemy-url",
        help="SQLAlchemy URL passed to the Alembic environment.",
    ),
    timeout: float | None = typer.Option(None, "--timeout"),
    json_output: bool = typer.Option(False, "--json", help="Emit JSON."),
) -> None:
    """Dry-run or apply an Alembic-rendered SQL artifact through the backend."""

    migration = _load_migration(provider)
    _ensure_registry(migration, timeout=timeout)
    migration.resolve_or_register_metatable_models(
        timeout=timeout,
        on_metatable_resolution=_print_metatable_resolution_callback(migration),
    )
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
    if dry_run:
        _emit(validation, json_output=json_output)
        return
    if not validation.ok:
        _emit(validation, json_output=json_output)
        raise typer.Exit(code=1)

    apply_operation = validation_operation.model_copy(update={"dry_run": False})
    result = MetaTable.apply_migration(apply_operation, timeout=timeout)
    if not result.ok:
        _emit(result, json_output=json_output)
        raise typer.Exit(code=1)
    applied_status = MetaTable.get_migration_status(_status_request(migration), timeout=timeout)
    registered = migration.sync_metatable_catalog(timeout=timeout)

    if json_output or _json_output_enabled():
        _emit(
            {
                "validation": validation,
                "apply": result,
                "status": applied_status,
                "registered": registered,
            },
            json_output=True,
        )
        return
    _emit(result, json_output=False)
