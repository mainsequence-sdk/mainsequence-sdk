from __future__ import annotations

import dataclasses
import json
import re
from collections.abc import Mapping, Sequence
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
REGISTER_ENDPOINT = "/orm/api/ts_manager/meta_table/register/"
RESERVE_MANAGED_ENDPOINT = "/orm/api/ts_manager/meta_table/reserve-managed/"


def _emit_status(message: str) -> None:
    typer.echo(f"[mainsequence migrations] {message}", err=True)


def _load_migration(provider: str | None) -> AlembicMetaTableMigration:
    provider_label = provider or "<default>"
    _emit_status(f"Loading migration provider {provider_label}...")
    try:
        migration = load_alembic_metatable_migration_provider(provider)
    except Exception as exc:
        raise typer.BadParameter(str(exc), param_hint="--provider") from exc
    _emit_status(
        "Loaded migration provider "
        f"package={migration.package} migration_namespace={migration.migration_namespace}"
    )
    return migration


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


def _item_value(item: Any, key: str) -> Any:
    if isinstance(item, Mapping):
        return item.get(key)
    return getattr(item, key, None)


def _contract_physical_table_name(item: Any) -> Any:
    contract = _item_value(item, "table_contract")
    if contract is None:
        return None
    physical = contract.get("physical") if isinstance(contract, Mapping) else getattr(contract, "physical", None)
    if physical is None:
        return None
    if isinstance(physical, Mapping):
        return physical.get("table_name")
    return getattr(physical, "table_name", None)


def _metatable_message(
    *,
    endpoint: str,
    action: str,
    model: type[Any],
    item: Any,
) -> str:
    model_name = getattr(model, "__name__", repr(model))
    identifier = (
        _item_value(item, "identifier")
        or getattr(model, "__metatable_identifier__", None)
        or model_name
    )
    uid = _item_value(item, "meta_table_uid") or _item_value(item, "uid")
    physical_table_name = (
        _item_value(item, "physical_table_name")
        or _contract_physical_table_name(item)
    )
    provisioning_status = _item_value(item, "provisioning_status")
    created = _item_value(item, "created")
    matched_by = _item_value(item, "matched_by")

    parts = [
        f"POST {endpoint}",
        f"{action} MetaTable identifier={identifier}",
    ]
    if model_name != identifier:
        parts.append(f"model={model_name}")
    if uid not in (None, ""):
        parts.append(f"uid={uid}")
    if physical_table_name not in (None, ""):
        parts.append(f"physical_table={physical_table_name}")
    if provisioning_status not in (None, ""):
        parts.append(f"provisioning_status={provisioning_status}")
    if created is not None:
        parts.append(f"created={created}")
    if matched_by not in (None, ""):
        parts.append(f"matched_by={matched_by}")
    return " ".join(parts)


def _emit_metatable_registration(model: type[Any], item: Any) -> None:
    typer.echo(
        _metatable_message(
            endpoint=REGISTER_ENDPOINT,
            action="registered",
            model=model,
            item=item,
        ),
        err=True,
    )


def _emit_metatable_reservation_request(
    models: Sequence[type[Any]],
    tables: Sequence[Any],
) -> None:
    identifiers = []
    for model, table in zip(models, tables, strict=True):
        identifiers.append(
            str(
                _item_value(table, "identifier")
                or getattr(model, "__metatable_identifier__", None)
                or getattr(model, "__name__", repr(model))
            )
        )
    _emit_status(
        f"Sending POST {RESERVE_MANAGED_ENDPOINT} request for {len(tables)} "
        f"MetaTables identifiers={','.join(identifiers)}"
    )


def _emit_metatable_reservation(model: type[Any], item: Any) -> None:
    typer.echo(
        _metatable_message(
            endpoint=RESERVE_MANAGED_ENDPOINT,
            action="reserved",
            model=model,
            item=item,
        ),
        err=True,
    )


def _prepare_alembic_config(
    migration: AlembicMetaTableMigration,
    *,
    timeout: float | None,
    ttl_seconds: int,
) -> tuple[Any, Any]:
    _emit_status("Ensuring Alembic registry MetaTable...")
    migration.ensure_alembic_registry(
        timeout=timeout,
        on_metatable_registered=_emit_metatable_registration,
    )
    _emit_status("Preparing platform-managed MetaTable reservations...")
    prepared = migration.prepare_for_alembic(
        timeout=timeout,
        on_metatable_reservation_request=_emit_metatable_reservation_request,
        on_metatable_reserved=_emit_metatable_reservation,
    )
    _emit_status(
        "Prepared migration scope "
        f"data_source_uid={prepared.data_source_uid} "
        f"meta_table_count={len(prepared.meta_table_uids)}"
    )
    _emit_status(f"Loading DynamicTableDataSource uid={prepared.data_source_uid}...")
    data_source = DynamicTableDataSource.get_by_uid(prepared.data_source_uid)
    _emit_status(
        "Requesting scoped migration connection "
        f"meta_table_count={len(prepared.meta_table_uids)} ttl_seconds={ttl_seconds}..."
    )
    connection = data_source.issue_migration_connection(
        DynamicTableDataSourceMigrationConnectionRequest(
            package=migration.package,
            migration_namespace=migration.migration_namespace,
            meta_table_uids=prepared.meta_table_uids,
            ttl_seconds=ttl_seconds,
        ),
        timeout=timeout,
    )
    _emit_status("Scoped migration connection acquired.")
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
    _emit_status("Running Alembic current...")
    command.current(config, verbose=verbose)
    _emit_status("Alembic current finished.")


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
    _emit_status(f"Running Alembic revision rev_id={resolved_rev_id}...")
    script = command.revision(
        config,
        message=resolved_message,
        autogenerate=autogenerate,
        rev_id=resolved_rev_id,
        head=head,
    )
    _emit_status("Alembic revision finished.")
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
    _emit_status(f"Running Alembic upgrade target={target_revision}...")
    command.upgrade(config, target_revision)
    _emit_status("Refreshing MetaTable catalog after upgrade...")
    registered = migration.refresh_metatable_catalog(
        timeout=timeout,
        on_metatable_registered=_emit_metatable_registration,
    )
    _emit_status("MetaTable catalog refresh finished.")
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
    _emit_status(f"Running Alembic downgrade target={target_revision}...")
    command.downgrade(config, target_revision)
    _emit_status("Refreshing MetaTable catalog after downgrade...")
    registered = migration.refresh_metatable_catalog(
        timeout=timeout,
        on_metatable_registered=_emit_metatable_registration,
    )
    _emit_status("MetaTable catalog refresh finished.")
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
