from __future__ import annotations

import dataclasses
import json
import logging
import re
import sys
from collections.abc import Mapping, Sequence
from contextlib import contextmanager
from types import SimpleNamespace
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
FINALIZE_MANAGED_ENDPOINT = "/orm/api/ts_manager/meta_table/finalize-managed/"
ALEMBIC_PROVIDER_RESET_ENDPOINT = "/orm/api/ts_manager/meta_table/alembic-provider-reset/"


class _AlembicOutput:
    def __init__(self) -> None:
        self._chunks: list[str] = []

    def write(self, data: str) -> int:
        text = str(data)
        self._chunks.append(text)
        sys.stderr.write(text)
        sys.stderr.flush()
        return len(text)

    def flush(self) -> None:
        sys.stderr.flush()

    @property
    def has_visible_output(self) -> bool:
        return any(chunk.strip() for chunk in self._chunks)


class _AlembicLogHandler(logging.Handler):
    def emit(self, record: logging.LogRecord) -> None:
        try:
            message = self.format(record)
        except Exception:
            self.handleError(record)
            return
        print(message, file=sys.stderr, flush=True)


@contextmanager
def _forward_alembic_logging():
    logger_names = {"alembic"}
    logger_names.update(
        name
        for name in logging.Logger.manager.loggerDict
        if name == "alembic" or name.startswith("alembic.")
    )
    loggers = [logging.getLogger(name) for name in logger_names]
    previous_state = [(logger, logger.level, logger.propagate) for logger in loggers]
    handler = _AlembicLogHandler()
    handler.setLevel(logging.DEBUG)
    handler.setFormatter(
        logging.Formatter("[alembic] %(levelname)s %(name)s: %(message)s")
    )
    root_logger = logging.getLogger("alembic")
    root_logger.addHandler(handler)
    for logger in loggers:
        logger.setLevel(logging.DEBUG)
        logger.propagate = True
    root_logger.propagate = False
    try:
        yield
    finally:
        root_logger.removeHandler(handler)
        for logger, level, propagate in previous_state:
            logger.setLevel(level)
            logger.propagate = propagate


def _emit_status(message: str) -> None:
    print(f"[mainsequence migrations] {message}", file=sys.stderr, flush=True)


def _emit_progress(message: str) -> None:
    print(message, file=sys.stderr, flush=True)


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


def _load_alembic_command(command_name: str) -> Any:
    _emit_status(f"Importing Alembic command module for {command_name}...")
    try:
        from alembic import command
    except ImportError as exc:
        raise typer.BadParameter("Alembic is required for migration commands.") from exc
    _emit_status(f"Imported Alembic command module for {command_name}.")
    return command


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


def _meta_table_uid(item: Any) -> str | None:
    if item is None:
        return None
    if isinstance(item, Mapping):
        uid = item.get("meta_table_uid") or item.get("uid")
    else:
        uid = getattr(item, "meta_table_uid", None) or getattr(item, "uid", None)
    if uid in (None, ""):
        return None
    return str(uid)


def _include_alembic_registry_in_scope(
    migration: AlembicMetaTableMigration,
    prepared: Any,
    registry_meta_table: Any,
) -> None:
    registry_meta_table = registry_meta_table or migration.alembic_registry.get_meta_table()
    registry_uid = _meta_table_uid(registry_meta_table)
    if registry_uid in (None, ""):
        return
    prepared.meta_table_uids = list(
        dict.fromkeys([registry_uid, *list(prepared.meta_table_uids)])
    )


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
    _emit_progress(
        _metatable_message(
            endpoint=REGISTER_ENDPOINT,
            action="registered",
            model=model,
            item=item,
        ),
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
    _emit_progress(
        _metatable_message(
            endpoint=RESERVE_MANAGED_ENDPOINT,
            action="reserved",
            model=model,
            item=item,
        ),
    )


def _emit_metatable_finalization(model: type[Any], item: Any) -> None:
    _emit_progress(
        _metatable_message(
            endpoint=FINALIZE_MANAGED_ENDPOINT,
            action="finalized",
            model=model,
            item=item,
        ),
    )


def _prepare_alembic_config(
    migration: AlembicMetaTableMigration,
    *,
    timeout: float | None,
    ttl_seconds: int,
    alembic_output: _AlembicOutput,
    stage_existing_schema_management: bool = True,
    require_existing_contract_match: bool = True,
    prepare_provider_metatables: bool = True,
) -> tuple[Any, Any]:
    _emit_status("Ensuring Alembic registry MetaTable...")
    registry_meta_table = migration.ensure_alembic_registry(
        timeout=timeout,
        on_metatable_registered=_emit_metatable_registration,
    )

    if prepare_provider_metatables:
        _emit_status("Preparing platform-managed MetaTable reservations...")
        prepared = migration.prepare_for_alembic(
            timeout=timeout,
            stage_existing_schema_management=stage_existing_schema_management,
            require_existing_contract_match=require_existing_contract_match,
            on_metatable_reservation_request=_emit_metatable_reservation_request,
            on_metatable_reservation_status=_emit_status,
            on_metatable_reserved=_emit_metatable_reservation,
        )
    else:
        _emit_status("Skipping provider MetaTable reservations for read-only Alembic command.")
        prepared = SimpleNamespace(
            data_source_uid=migration._resolve_provider_data_source_uid(),
            meta_table_uids=[],
            owner_role_name=None,
        )
    _include_alembic_registry_in_scope(migration, prepared, registry_meta_table)
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
    _emit_status("Building Alembic config...")
    config = alembic_config_for_provider(
        migration,
        sqlalchemy_url=connection.uri,
        owner_role_name=connection.owner_role_name or prepared.owner_role_name,
        stdout=alembic_output,
        output_buffer=alembic_output,
    )
    _emit_status("Alembic config built.")
    return prepared, config


def _next_sequential_revision_id(
    migration: AlembicMetaTableMigration,
    *,
    alembic_output: _AlembicOutput,
) -> str:
    _emit_status("Importing Alembic ScriptDirectory for revision id scan...")
    try:
        from alembic.script import ScriptDirectory
    except ImportError as exc:
        raise RuntimeError("Alembic is required for revision generation.") from exc
    _emit_status("Imported Alembic ScriptDirectory.")

    _emit_status("Scanning Alembic revision directory for next sequential id...")
    with _forward_alembic_logging():
        script = ScriptDirectory.from_config(
            alembic_config_for_provider(
                migration,
                sqlalchemy_url="postgresql://",
                stdout=alembic_output,
                output_buffer=alembic_output,
            )
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
    with _forward_alembic_logging():
        for revision in script.walk_revisions():
            revision_id = str(revision.revision)
            if re.fullmatch(r"\d{4,}", revision_id):
                numeric_revisions.append(int(revision_id))
    next_revision_id = f"{max(numeric_revisions, default=0) + 1:04d}"
    _emit_status(f"Next Alembic revision id is {next_revision_id}.")
    return next_revision_id


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

    command = _load_alembic_command("current")
    migration = _load_migration(provider)
    alembic_output = _AlembicOutput()
    _, config = _prepare_alembic_config(
        migration,
        timeout=timeout,
        ttl_seconds=ttl_seconds,
        alembic_output=alembic_output,
        stage_existing_schema_management=False,
        require_existing_contract_match=False,
        prepare_provider_metatables=False,
    )
    _emit_status("Starting Alembic current now...")
    with _forward_alembic_logging():
        command.current(config, verbose=verbose)
    if not alembic_output.has_visible_output:
        _emit_status(
            "Alembic current produced no revision output. The version table is "
            "empty or Alembic found no current revision for this migration scope."
        )
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

    command = _load_alembic_command("revision")
    migration = _load_migration(provider)
    resolved_message = (message or "").strip() or "migration"
    alembic_output = _AlembicOutput()
    resolved_rev_id = rev_id or _next_sequential_revision_id(
        migration,
        alembic_output=alembic_output,
    )
    prepared, config = _prepare_alembic_config(
        migration,
        timeout=timeout,
        ttl_seconds=ttl_seconds,
        alembic_output=alembic_output,
        stage_existing_schema_management=False,
    )
    _emit_status(f"Starting Alembic revision now rev_id={resolved_rev_id}...")
    with _forward_alembic_logging():
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
    """Run Alembic upgrade directly and finalize reserved MetaTables."""

    command = _load_alembic_command("upgrade")
    migration = _load_migration(provider)
    alembic_output = _AlembicOutput()
    prepared, config = _prepare_alembic_config(
        migration,
        timeout=timeout,
        ttl_seconds=ttl_seconds,
        alembic_output=alembic_output,
    )
    _emit_status(f"Starting Alembic upgrade now target={target_revision}...")
    with _forward_alembic_logging():
        command.upgrade(config, target_revision)
    _emit_status("Finalizing MetaTable catalog after upgrade...")
    finalize_response = migration.finalize_metatable_catalog(
        prepared=prepared,
        alembic_revision=target_revision,
        timeout=timeout,
        on_metatable_finalized=_emit_metatable_finalization,
        on_metatable_finalize_status=_emit_status,
    )
    _emit_status("MetaTable catalog finalization finished.")
    _emit(
        {
            "ok": True,
            "revision": target_revision,
            "package": migration.package,
            "migration_namespace": migration.migration_namespace,
            "meta_table_uids": prepared.meta_table_uids,
            "finalized_count": finalize_response.finalized_count,
            "active_count": finalize_response.active_count,
            "reserved_count": finalize_response.reserved_count,
            "failed_count": finalize_response.failed_count,
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
    """Run Alembic downgrade directly and finalize reserved MetaTables."""

    command = _load_alembic_command("downgrade")
    migration = _load_migration(provider)
    alembic_output = _AlembicOutput()
    prepared, config = _prepare_alembic_config(
        migration,
        timeout=timeout,
        ttl_seconds=ttl_seconds,
        alembic_output=alembic_output,
    )
    _emit_status(f"Starting Alembic downgrade now target={target_revision}...")
    with _forward_alembic_logging():
        command.downgrade(config, target_revision)
    _emit_status("Finalizing MetaTable catalog after downgrade...")
    finalize_response = migration.finalize_metatable_catalog(
        prepared=prepared,
        alembic_revision=target_revision,
        timeout=timeout,
        on_metatable_finalized=_emit_metatable_finalization,
        on_metatable_finalize_status=_emit_status,
    )
    _emit_status("MetaTable catalog finalization finished.")
    _emit(
        {
            "ok": True,
            "revision": target_revision,
            "package": migration.package,
            "migration_namespace": migration.migration_namespace,
            "meta_table_uids": prepared.meta_table_uids,
            "finalized_count": finalize_response.finalized_count,
            "active_count": finalize_response.active_count,
            "reserved_count": finalize_response.reserved_count,
            "failed_count": finalize_response.failed_count,
        },
        json_output=json_output,
    )


@migrations.command("reset")
def reset(
    provider: str | None = typer.Option(
        None,
        "--provider",
        help="Migration provider reference, for example msm.migrations:migration.",
    ),
    confirm_reset: bool = typer.Option(
        False,
        "--confirm-reset",
        help="Required confirmation for destructive provider-scoped reset.",
    ),
    drop_physical_tables: bool = typer.Option(
        True,
        "--drop-physical-tables/--keep-physical-tables",
        help="Drop provider physical tables during reset.",
    ),
    clear_alembic_version_table: bool = typer.Option(
        True,
        "--clear-alembic-version-table/--keep-alembic-version-table",
        help="Clear the provider Alembic version table during reset.",
    ),
    include_reserved: bool = typer.Option(
        True,
        "--include-reserved/--active-only",
        help="Include already-reserved provider MetaTables in reset results.",
    ),
    timeout: float | None = typer.Option(None, "--timeout"),
    json_output: bool = typer.Option(False, "--json", help="Emit JSON."),
) -> None:
    """Reset an Alembic-managed provider catalog/physical state."""

    if not confirm_reset:
        raise typer.BadParameter(
            "Pass --confirm-reset to call the destructive provider reset endpoint.",
            param_hint="--confirm-reset",
        )
    migration = _load_migration(provider)
    _emit_status(
        "Calling provider reset endpoint "
        f"{ALEMBIC_PROVIDER_RESET_ENDPOINT} provider={migration.migration_provider_key}..."
    )
    response = migration.reset_alembic_provider(
        confirm_reset=True,
        drop_physical_tables=drop_physical_tables,
        clear_alembic_version_table=clear_alembic_version_table,
        include_reserved=include_reserved,
        timeout=timeout,
        on_reset_status=_emit_status,
    )
    _emit_status("Alembic provider reset finished.")
    _emit(response, json_output=json_output)
