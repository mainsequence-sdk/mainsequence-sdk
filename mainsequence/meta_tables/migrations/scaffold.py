from __future__ import annotations

import importlib.resources
from dataclasses import dataclass
from pathlib import Path

from .alembic import namespace_version_slug


@dataclass(frozen=True, slots=True)
class MigrationScaffoldFile:
    path: Path
    action: str


@dataclass(frozen=True, slots=True)
class MigrationScaffoldResult:
    root: Path
    files: tuple[MigrationScaffoldFile, ...]


def scaffold_migration_package(
    *,
    project_root: str | Path,
    module: str,
    package: str,
    namespace: str,
    metadata_ref: str,
    base_ref: str | None = None,
    model_registry_ref: str | None = None,
    alembic_version_name: str = "ProjectAlembicVersion",
    alembic_version_identifier: str | None = None,
    alembic_version_schema: str | None = "public",
    alembic_version_table_name: str = "alembic_version",
    source_root: str | Path = "src",
    force: bool = False,
) -> MigrationScaffoldResult:
    """Create an SDK-shaped MetaTable migration package skeleton."""

    if not module:
        raise ValueError("Migration scaffold module cannot be empty.")
    if not package:
        raise ValueError("Migration scaffold package cannot be empty.")
    if not namespace:
        raise ValueError("Migration scaffold namespace cannot be empty.")

    project_root_path = Path(project_root).expanduser().resolve()
    package_root = project_root_path / source_root / Path(*module.split("."))
    version_root = package_root / "versions"
    namespace_root = version_root / namespace_version_slug(namespace)
    for directory in (package_root, version_root, namespace_root):
        directory.mkdir(parents=True, exist_ok=True)

    resolved_model_registry_ref = model_registry_ref or f"{module}.registry:metatable_provider_models"
    files = [
        _write_scaffold_file(
            package_root / "__init__.py",
            _render_package_init(
                module=module,
                package=package,
                namespace=namespace,
                metadata_ref=metadata_ref,
                model_registry_ref=resolved_model_registry_ref,
                alembic_version_name=alembic_version_name,
                alembic_version_identifier=(
                    alembic_version_identifier or f"{package}.alembic_version"
                ),
                alembic_version_schema=alembic_version_schema,
                alembic_version_table_name=alembic_version_table_name,
            ),
            force=force,
        ),
        _write_scaffold_file(
            package_root / "registry.py",
            _render_registry(base_ref=base_ref),
            force=force,
        ),
        _write_scaffold_file(
            package_root / "env.py",
            _render_env_template(migration_module=module),
            force=force,
        ),
        _write_scaffold_file(
            package_root / "script.py.mako",
            _read_template("script.py.mako"),
            force=force,
        ),
        _write_scaffold_file(
            version_root / "__init__.py",
            "",
            force=force,
        ),
        _write_scaffold_file(
            namespace_root / "__init__.py",
            "",
            force=force,
        ),
    ]
    return MigrationScaffoldResult(root=package_root, files=tuple(files))


def _render_package_init(
    *,
    module: str,
    package: str,
    namespace: str,
    metadata_ref: str,
    model_registry_ref: str,
    alembic_version_name: str,
    alembic_version_identifier: str,
    alembic_version_schema: str | None,
    alembic_version_table_name: str,
) -> str:
    metadata_import, metadata_expression = _import_statement_for_ref(metadata_ref)
    registry_import, registry_expression = _import_statement_for_ref(model_registry_ref)
    schema_literal = repr(alembic_version_schema)
    return f'''from __future__ import annotations

from mainsequence.meta_tables.migrations import (
    build_alembic_version_metatable,
    build_metatable_migration_provider,
)

{metadata_import}
{registry_import}


{alembic_version_name} = build_alembic_version_metatable(
    class_name="{alembic_version_name}",
    namespace={namespace!r},
    identifier={alembic_version_identifier!r},
    schema={schema_literal},
    table_name={alembic_version_table_name!r},
)

migration = build_metatable_migration_provider(
    package={package!r},
    migration_namespace={namespace!r},
    script_location="{module}:",
    version_location_prefix="{module}:versions",
    target_metadata={metadata_expression},
    alembic_registry={alembic_version_name},
    metatable_models={registry_expression}(),
)


__all__ = ["{alembic_version_name}", "migration"]
'''


def _render_registry(*, base_ref: str | None) -> str:
    if base_ref is None:
        base_import = ""
        base_argument = ""
    else:
        base_import, base_expression = _import_statement_for_ref(base_ref)
        base_argument = f", base={base_expression}"
    return f'''from __future__ import annotations

from typing import Any

from mainsequence.meta_tables.migrations import build_metatable_model_registry

{base_import}


def _metatable_model_sources() -> list[type[Any]]:
    return [
        # Add project-owned MetaTable model classes here, or expand package
        # functions that return model classes.
    ]


def metatable_provider_models() -> list[type[Any]]:
    return build_metatable_model_registry(_metatable_model_sources(){base_argument})


__all__ = ["metatable_provider_models"]
'''


def _render_env_template(*, migration_module: str) -> str:
    return _read_template("env.py.mako").replace("${migration_module}", migration_module)


def _read_template(name: str) -> str:
    return (
        importlib.resources.files("mainsequence.meta_tables.migrations.templates")
        .joinpath(name)
        .read_text(encoding="utf-8")
    )


def _import_statement_for_ref(reference: str) -> tuple[str, str]:
    module_name, separator, expression = reference.partition(":")
    if not separator or not module_name or not expression:
        raise ValueError(f"Expected import reference in module:object form, got {reference!r}.")
    root_name = expression.split(".", 1)[0]
    return f"from {module_name} import {root_name}", expression


def _write_scaffold_file(path: Path, content: str, *, force: bool) -> MigrationScaffoldFile:
    if path.exists():
        existing = path.read_text(encoding="utf-8")
        if existing == content:
            return MigrationScaffoldFile(path=path, action="skipped")
        if not force:
            raise FileExistsError(
                f"Refusing to overwrite existing migration scaffold file: {path}"
            )
        path.write_text(content, encoding="utf-8")
        return MigrationScaffoldFile(path=path, action="overwritten")
    path.write_text(content, encoding="utf-8")
    return MigrationScaffoldFile(path=path, action="created")


__all__ = [
    "MigrationScaffoldFile",
    "MigrationScaffoldResult",
    "scaffold_migration_package",
]
