from __future__ import annotations

import datetime
import uuid
from types import SimpleNamespace

import pytest
from sqlalchemy.orm import Mapped

import mainsequence.meta_tables.sqlalchemy_contracts as sqlalchemy_contracts
from mainsequence.client.metatables import (
    DataSource,
    DynamicTableDataSource,
    MetaTable,
    MetaTableRegistrationRequest,
    TimeIndexMetaTable,
    TimeIndexMetaTableRegistrationRequest,
)
from mainsequence.meta_tables import (
    PlatformManagedMetaTable,
    PlatformTimeIndexMetaTable,
    external_registered_registration_request_from_sqlalchemy_model,
    platform_managed_migration_registration_context,
    platform_managed_registration_request_from_sqlalchemy_model,
    schema_index_name,
    sqlalchemy_naming_convention,
    table_contract_from_sqlalchemy_model,
    time_indexed_registration_request_from_sqlalchemy_model,
)
from mainsequence.meta_tables.data_nodes.persist_managers import ensure_registered_storage_table


@pytest.fixture(autouse=True)
def _clear_metatable_registration_registry(monkeypatch):
    monkeypatch.setattr(
        "mainsequence.client.metatables.SessionDataSource.data_source",
        SimpleNamespace(
            uid="dddddddd-dddd-4ddd-8ddd-dddddddddddd",
            related_resource=SimpleNamespace(
                uid="dddddddd-dddd-4ddd-8ddd-dddddddddddd",
                data_source_uid="dddddddd-dddd-4ddd-8ddd-dddddddddddd",
                status="AVAILABLE",
            ),
        ),
    )
    sqlalchemy_contracts._METATABLE_REGISTRATION_REGISTRY.clear()
    yield
    sqlalchemy_contracts._METATABLE_REGISTRATION_REGISTRY.clear()


class Uuid:
    def __str__(self):
        return "CHAR(32)"


class String:
    def __init__(self, length: int):
        self.length = length

    def __str__(self):
        return f"VARCHAR({self.length})"


class Date:
    def __str__(self):
        return "DATE"


class DateTime:
    def __init__(self, *, timezone: bool):
        self.timezone = timezone

    def __str__(self):
        return "TIMESTAMP WITH TIME ZONE" if self.timezone else "TIMESTAMP WITHOUT TIME ZONE"


class FakeColumn:
    def __init__(
        self,
        name,
        column_type,
        *,
        nullable=True,
        primary_key=False,
        unique=False,
        info=None,
    ):
        self.name = name
        self.type = column_type
        self.nullable = nullable
        self.primary_key = primary_key
        self.unique = unique
        self.info = info or {}
        self.table = None


class FakeIndex:
    def __init__(self, name, columns, *, unique=False, method=None):
        self.name = name
        self.columns = columns
        self.unique = unique
        self.dialect_options = {"postgresql": {"using": method}} if method else {}


class FakeForeignKeyElement:
    def __init__(self, parent, column, *, ondelete=None):
        self.parent = parent
        self.column = column
        self.ondelete = ondelete


class FakeForeignKeyConstraint:
    def __init__(self, name, elements):
        self.name = name
        self.elements = elements


class FakeTable:
    def __init__(self, name, *, schema="public", columns=None, indexes=None, foreign_keys=None):
        self.name = name
        self.schema = schema
        self.columns = columns or []
        self.indexes = indexes or []
        self.foreign_key_constraints = foreign_keys or []
        self.info = {}
        for column in self.columns:
            column.table = self

    @property
    def fullname(self):
        return f"{self.schema}.{self.name}" if self.schema else self.name


def _model_attrs(name, table, *, namespace="example.assets", identifier=None):
    return {
        "__module__": "tests.client_tables",
        "__metatable_namespace__": namespace,
        "__metatable_identifier__": identifier or name,
        "__table__": table,
    }


def _model_class(name, table, *, namespace="example.assets", identifier=None):
    return type(
        name,
        (),
        _model_attrs(
            name,
            table,
            namespace=namespace,
            identifier=identifier,
        ),
    )


def _platform_model_class(
    name,
    table,
    *,
    namespace="example.assets",
    identifier=None,
):
    return type(
        name,
        (PlatformManagedMetaTable,),
        _model_attrs(
            name,
            table,
            namespace=namespace,
            identifier=identifier,
        ),
    )


def _time_index_model_class(
    name,
    table,
    *,
    namespace="example.assets",
    identifier=None,
    time_index_name="time_index",
    index_names=None,
    cadence=None,
):
    attrs = _model_attrs(
        name,
        table,
        namespace=namespace,
        identifier=identifier,
    )
    attrs.update(
        {
            "__time_index_name__": time_index_name,
            "__index_names__": list(index_names or [time_index_name]),
        }
    )
    if cadence is not None:
        attrs["__cadence__"] = cadence
    return type(
        name,
        (PlatformTimeIndexMetaTable,),
        attrs,
    )


def _dynamic_table_data_source(uid: str = "dddddddd-dddd-4ddd-8ddd-dddddddddddd"):
    physical = DataSource.model_construct(
        uid="eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee",
        data_source_uid="eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee",
        display_name="Local DuckDB",
        class_type="DUCK_DB",
        status="AVAILABLE",
    )
    return DynamicTableDataSource.model_construct(
        uid=uid,
        related_resource=physical,
        related_resource_class_type="DUCK_DB",
    )


def _configured_storage_hash(model_or_table):
    return sqlalchemy_contracts._configured_storage_hash_for_model(model_or_table)


def _request_payload(request):
    return request.model_dump(mode="json", exclude_none=True)


def _assert_omits_storage_hash(request):
    assert "storage_hash" not in _request_payload(request)


def test_metatable_identifier_is_optional_with_pyproject(tmp_path, monkeypatch):
    (tmp_path / "pyproject.toml").write_text(
        '[project]\nname = "alpha-project"\n',
        encoding="utf-8",
    )
    monkeypatch.chdir(tmp_path)
    table = FakeTable("asset")
    AssetModel = type(
        "AssetModel",
        (),
        {
            "__module__": "project.models.assets",
            "__qualname__": "Outer.AssetModel",
            "__metatable_namespace__": "example.assets",
            "__table__": table,
        },
    )

    assert sqlalchemy_contracts.resolve_metatable_identifier(AssetModel) is None


def test_explicit_metatable_identifier_does_not_require_pyproject(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    table = FakeTable("asset")
    AssetModel = type(
        "AssetModel",
        (),
        {
            "__module__": "project.models.assets",
            "__metatable_namespace__": "example.assets",
            "__metatable_identifier__": "global.asset",
            "__table__": table,
        },
    )

    assert sqlalchemy_contracts.resolve_metatable_identifier(AssetModel) == "global.asset"


def test_metatable_identifier_is_optional_without_pyproject(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    table = FakeTable("asset")
    AssetModel = type(
        "AssetModel",
        (),
        {
            "__module__": "project.models.assets",
            "__metatable_namespace__": "example.assets",
            "__table__": table,
        },
    )

    assert sqlalchemy_contracts.resolve_metatable_identifier(AssetModel) is None


def test_platform_managed_registration_request_from_sqlalchemy_metadata():
    table_name = "example_assets__account"
    account_table = FakeTable(
        table_name,
        columns=[
            FakeColumn("uid", Uuid(), nullable=False, primary_key=True),
            FakeColumn(
                "name",
                String(255),
                nullable=False,
                info={"description": "Display name", "label": "Name"},
            ),
        ],
    )
    Account = _model_class("Account", account_table)

    request = platform_managed_registration_request_from_sqlalchemy_model(
        Account,
        data_source_uid="dddddddd-dddd-4ddd-8ddd-dddddddddddd",
    )

    assert isinstance(request, MetaTableRegistrationRequest)
    assert request.management_mode == "platform_managed"
    _assert_omits_storage_hash(request)
    assert request.identifier == "Account"
    assert request.namespace == "example.assets"
    assert request.provisioning == {"create_table": True, "if_not_exists": True}
    assert request.table_contract.physical.table_name == table_name
    assert request.table_contract.columns[0].data_type == "uuid"
    assert request.table_contract.columns[0].backend_type == "UUID"
    assert request.table_contract.columns[0].primary_key is True
    assert request.table_contract.columns[0].server_default == "gen_random_uuid()"
    assert request.table_contract.columns[1].data_type == "string"
    assert request.table_contract.columns[1].backend_type == "VARCHAR(255)"
    assert request.table_contract.columns[1].description == "Display name"


def test_registration_request_accepts_dynamic_table_data_source():
    table_name = "example_assets__account"
    account_table = FakeTable(
        table_name,
        columns=[FakeColumn("uid", Uuid(), nullable=False, primary_key=True)],
    )
    Account = _model_class("Account", account_table)

    request = platform_managed_registration_request_from_sqlalchemy_model(
        Account,
        data_source=_dynamic_table_data_source(),
    )

    assert request.data_source_uid == "dddddddd-dddd-4ddd-8ddd-dddddddddddd"


def test_registration_request_rejects_non_dynamic_table_data_source():
    table_name = "example_assets__account"
    account_table = FakeTable(
        table_name,
        columns=[FakeColumn("uid", Uuid(), nullable=False, primary_key=True)],
    )
    Account = _model_class("Account", account_table)

    with pytest.raises(TypeError, match="DynamicTableDataSource"):
        platform_managed_registration_request_from_sqlalchemy_model(
            Account,
            data_source=SimpleNamespace(uid="dddddddd-dddd-4ddd-8ddd-dddddddddddd"),
        )


def test_registration_request_uses_class_metatable_description():
    table_name = "example_assets__account"
    account_table = FakeTable(
        table_name,
        columns=[FakeColumn("uid", Uuid(), nullable=False, primary_key=True)],
    )
    Account = _model_class("Account", account_table)
    Account.__metatable_description__ = (
        "Accounts are legal entities used to scope balances and holdings."
    )

    request = platform_managed_registration_request_from_sqlalchemy_model(
        Account,
        data_source_uid="dddddddd-dddd-4ddd-8ddd-dddddddddddd",
    )

    assert request.description == (
        "Accounts are legal entities used to scope balances and holdings."
    )


def test_registration_request_description_argument_overrides_class_default():
    table_name = "example_assets__account"
    account_table = FakeTable(
        table_name,
        columns=[FakeColumn("uid", Uuid(), nullable=False, primary_key=True)],
    )
    Account = _model_class("Account", account_table)
    Account.__metatable_description__ = "Class-level account table description."

    request = platform_managed_registration_request_from_sqlalchemy_model(
        Account,
        data_source_uid="dddddddd-dddd-4ddd-8ddd-dddddddddddd",
        description="Explicit request description.",
    )

    assert request.description == "Explicit request description."


def test_external_registration_request_uses_class_metatable_description():
    account_table = FakeTable(
        "account",
        columns=[FakeColumn("uid", Uuid(), nullable=False, primary_key=True)],
    )
    Account = _model_class("Account", account_table)
    Account.__metatable_description__ = "External account rows imported from the warehouse."

    request = external_registered_registration_request_from_sqlalchemy_model(
        Account,
        data_source_uid="dddddddd-dddd-4ddd-8ddd-dddddddddddd",
    )

    assert request.description == "External account rows imported from the warehouse."


def test_sqlalchemy_contract_marks_id_uuid_primary_key_with_server_default():
    table_name = "example_assets__asset"
    asset_table = FakeTable(
        table_name,
        columns=[
            FakeColumn("id", Uuid(), nullable=False, primary_key=True),
            FakeColumn("symbol", String(64), nullable=False),
        ],
    )
    Asset = _model_class("Asset", asset_table)

    contract = table_contract_from_sqlalchemy_model(Asset)

    id_column = contract.columns[0]
    assert id_column.name == "id"
    assert id_column.data_type == "uuid"
    assert id_column.backend_type == "UUID"
    assert id_column.server_default == "gen_random_uuid()"


def test_sqlalchemy_contract_maps_date_and_timezone_aware_datetime():
    table_name = "example_assets__observation"
    table = FakeTable(
        table_name,
        columns=[
            FakeColumn("as_of", Date(), nullable=False),
            FakeColumn("seen_at", DateTime(timezone=True), nullable=False),
        ],
    )
    Observation = _model_class("Observation", table)

    contract = table_contract_from_sqlalchemy_model(Observation)

    assert contract.columns[0].data_type == "date"
    assert contract.columns[0].backend_type == "DATE"
    assert contract.columns[1].data_type == "timestamp with time zone"
    assert contract.columns[1].backend_type == "TIMESTAMP WITH TIME ZONE"


def test_sqlalchemy_contract_rejects_timezone_naive_datetime():
    table_name = "example_assets__observation"
    table = FakeTable(
        table_name,
        columns=[FakeColumn("seen_at", DateTime(timezone=False), nullable=False)],
    )
    Observation = _model_class("Observation", table)

    with pytest.raises(ValueError, match="Timezone-naive"):
        table_contract_from_sqlalchemy_model(Observation)


def test_sqlalchemy_contract_omits_indexes_and_foreign_keys():
    account_table_name = "example_assets__account"
    account_uid = FakeColumn("uid", Uuid(), nullable=False, primary_key=True)
    FakeTable(account_table_name, columns=[account_uid])

    asset_table_name = "example_assets__asset"
    asset_uid = FakeColumn("uid", Uuid(), nullable=False, primary_key=True)
    account_uid_source = FakeColumn("account_uid", Uuid(), nullable=False)
    symbol = FakeColumn("symbol", String(64), nullable=False)
    asset_index = FakeIndex("asset_account_uid_idx", [account_uid_source], method="btree")
    asset_fk = FakeForeignKeyConstraint(
        "asset_account_uid_fkey",
        [
            FakeForeignKeyElement(
                account_uid_source,
                account_uid,
                ondelete="RESTRICT",
            )
        ],
    )
    asset_table = FakeTable(
        asset_table_name,
        columns=[asset_uid, account_uid_source, symbol],
        indexes=[asset_index],
        foreign_keys=[asset_fk],
    )
    Asset = _model_class("Asset", asset_table)

    contract = table_contract_from_sqlalchemy_model(Asset)

    assert contract.physical.table_name == asset_table_name
    assert not hasattr(contract, "indexes")
    assert not hasattr(contract, "foreign_keys")
    payload = contract.model_dump(mode="json", exclude_none=True)
    assert "indexes" not in payload
    assert "foreign_keys" not in payload


def test_configured_storage_hash_changes_with_column_shape():
    base_table = FakeTable(
        "placeholder",
        columns=[FakeColumn("uid", Uuid(), nullable=False, primary_key=True)],
    )
    extended_table = FakeTable(
        "placeholder",
        columns=[
            FakeColumn("uid", Uuid(), nullable=False, primary_key=True),
            FakeColumn("symbol", String(64), nullable=False),
        ],
    )
    BaseAsset = _model_class("Asset", base_table)
    ExtendedAsset = _model_class("Asset", extended_table)

    assert _configured_storage_hash(BaseAsset) != _configured_storage_hash(ExtendedAsset)


def test_configured_storage_hash_changes_with_index_shape():
    uid = FakeColumn("uid", Uuid(), nullable=False, primary_key=True)
    symbol = FakeColumn("symbol", String(64), nullable=False)
    base_table = FakeTable("placeholder", columns=[uid, symbol])

    indexed_uid = FakeColumn("uid", Uuid(), nullable=False, primary_key=True)
    indexed_symbol = FakeColumn("symbol", String(64), nullable=False)
    indexed_table = FakeTable(
        "placeholder",
        columns=[indexed_uid, indexed_symbol],
        indexes=[FakeIndex("asset_symbol_idx", [indexed_symbol], method="btree")],
    )
    BaseAsset = _model_class("Asset", base_table)
    IndexedAsset = _model_class("Asset", indexed_table)

    assert _configured_storage_hash(BaseAsset) != _configured_storage_hash(IndexedAsset)


def test_configured_storage_hash_ignores_index_name():
    first_symbol = FakeColumn("symbol", String(64), nullable=False)
    first_table = FakeTable(
        "placeholder",
        columns=[FakeColumn("uid", Uuid(), nullable=False, primary_key=True), first_symbol],
        indexes=[FakeIndex("asset_symbol_idx", [first_symbol], method="btree")],
    )
    second_symbol = FakeColumn("symbol", String(64), nullable=False)
    second_table = FakeTable(
        "placeholder",
        columns=[FakeColumn("uid", Uuid(), nullable=False, primary_key=True), second_symbol],
        indexes=[FakeIndex("backend_generated_name", [second_symbol], method="btree")],
    )
    FirstAsset = _model_class("Asset", first_table)
    SecondAsset = _model_class("Asset", second_table)

    assert _configured_storage_hash(FirstAsset) == _configured_storage_hash(SecondAsset)


def test_configured_storage_hash_changes_with_foreign_key_shape():
    account_uid = FakeColumn("uid", Uuid(), nullable=False, primary_key=True)
    account_table = FakeTable("example_assets__account", columns=[account_uid])

    base_asset_uid = FakeColumn("uid", Uuid(), nullable=False, primary_key=True)
    base_account_uid = FakeColumn("account_uid", Uuid(), nullable=False)
    base_table = FakeTable(
        "placeholder",
        columns=[base_asset_uid, base_account_uid],
    )

    fk_asset_uid = FakeColumn("uid", Uuid(), nullable=False, primary_key=True)
    fk_account_uid = FakeColumn("account_uid", Uuid(), nullable=False)
    fk_table = FakeTable(
        "placeholder",
        columns=[fk_asset_uid, fk_account_uid],
        foreign_keys=[
            FakeForeignKeyConstraint(
                "asset_account_uid_fkey",
                [
                    FakeForeignKeyElement(
                        fk_account_uid,
                        account_table.columns[0],
                        ondelete="RESTRICT",
                    )
                ],
            )
        ],
    )
    BaseAsset = _model_class("Asset", base_table)
    ForeignKeyAsset = _model_class("Asset", fk_table)

    assert _configured_storage_hash(BaseAsset) != _configured_storage_hash(ForeignKeyAsset)


def test_configured_storage_hash_ignores_foreign_key_name():
    account_uid = FakeColumn("uid", Uuid(), nullable=False, primary_key=True)
    account_table = FakeTable("example_assets__account", columns=[account_uid])

    first_account_uid = FakeColumn("account_uid", Uuid(), nullable=False)
    first_table = FakeTable(
        "placeholder",
        columns=[
            FakeColumn("uid", Uuid(), nullable=False, primary_key=True),
            first_account_uid,
        ],
        foreign_keys=[
            FakeForeignKeyConstraint(
                "asset_account_uid_fkey",
                [
                    FakeForeignKeyElement(
                        first_account_uid,
                        account_table.columns[0],
                        ondelete="RESTRICT",
                    )
                ],
            )
        ],
    )
    second_account_uid = FakeColumn("account_uid", Uuid(), nullable=False)
    second_table = FakeTable(
        "placeholder",
        columns=[
            FakeColumn("uid", Uuid(), nullable=False, primary_key=True),
            second_account_uid,
        ],
        foreign_keys=[
            FakeForeignKeyConstraint(
                "backend_generated_name",
                [
                    FakeForeignKeyElement(
                        second_account_uid,
                        account_table.columns[0],
                        ondelete="RESTRICT",
                    )
                ],
            )
        ],
    )
    FirstAsset = _model_class("Asset", first_table)
    SecondAsset = _model_class("Asset", second_table)

    assert _configured_storage_hash(FirstAsset) == _configured_storage_hash(SecondAsset)


def test_configured_storage_hash_changes_with_foreign_key_action():
    account_uid = FakeColumn("uid", Uuid(), nullable=False, primary_key=True)
    account_table = FakeTable("example_assets__account", columns=[account_uid])

    restrict_account_uid = FakeColumn("account_uid", Uuid(), nullable=False)
    restrict_table = FakeTable(
        "placeholder",
        columns=[
            FakeColumn("uid", Uuid(), nullable=False, primary_key=True),
            restrict_account_uid,
        ],
        foreign_keys=[
            FakeForeignKeyConstraint(
                None,
                [
                    FakeForeignKeyElement(
                        restrict_account_uid,
                        account_table.columns[0],
                        ondelete="RESTRICT",
                    )
                ],
            )
        ],
    )
    cascade_account_uid = FakeColumn("account_uid", Uuid(), nullable=False)
    cascade_table = FakeTable(
        "placeholder",
        columns=[
            FakeColumn("uid", Uuid(), nullable=False, primary_key=True),
            cascade_account_uid,
        ],
        foreign_keys=[
            FakeForeignKeyConstraint(
                None,
                [
                    FakeForeignKeyElement(
                        cascade_account_uid,
                        account_table.columns[0],
                        ondelete="CASCADE",
                    )
                ],
            )
        ],
    )
    RestrictAsset = _model_class("Asset", restrict_table)
    CascadeAsset = _model_class("Asset", cascade_table)

    assert _configured_storage_hash(RestrictAsset) != _configured_storage_hash(CascadeAsset)


def test_configured_storage_hash_ignores_logical_identifier():
    account_table = FakeTable(
        "placeholder",
        columns=[FakeColumn("uid", Uuid(), nullable=False, primary_key=True)],
    )
    renamed_table = FakeTable(
        "placeholder",
        columns=[FakeColumn("uid", Uuid(), nullable=False, primary_key=True)],
    )
    Account = _model_class("Account", account_table, identifier="Account")
    RenamedAccount = _model_class("RenamedAccount", renamed_table, identifier="CustomerAccount")

    assert _configured_storage_hash(Account) == _configured_storage_hash(RenamedAccount)


def test_configured_storage_hash_includes_physical_table_name():
    account_table = FakeTable(
        "example_assets__account",
        columns=[FakeColumn("uid", Uuid(), nullable=False, primary_key=True)],
    )
    renamed_table = FakeTable(
        "example_assets__account_v2",
        columns=[FakeColumn("uid", Uuid(), nullable=False, primary_key=True)],
    )
    Account = _model_class("Account", account_table, identifier="Account")
    RenamedAccount = _model_class("RenamedAccount", renamed_table, identifier="Account")

    assert _configured_storage_hash(Account) != _configured_storage_hash(RenamedAccount)


def test_legacy_schema_migration_bases_are_not_public():
    import mainsequence.meta_tables as meta_tables

    removed_names = [
        "Migration" + "Managed" + "MetaTable",
        "Migration" + "Managed" + "TimeIndexMetaTable",
    ]
    for name in removed_names:
        assert not hasattr(meta_tables, name)
        assert not hasattr(sqlalchemy_contracts, name)


def test_platform_managed_schema_resolves_from_sqlalchemy_table_args_only():
    class Account:
        __table_args__ = {"schema": "table_args_schema"}

    setattr(Account, "__metatable_" + "schema__", "ignored_schema")
    assert sqlalchemy_contracts._resolve_class_schema(Account) == "table_args_schema"


def test_platform_managed_default_public_schema_stays_sqlalchemy_default():
    pytest.importorskip("sqlalchemy")

    from sqlalchemy import MetaData
    from sqlalchemy import Uuid as SQLAlchemyUuid
    from sqlalchemy.orm import DeclarativeBase, mapped_column

    class Base(DeclarativeBase):
        metadata = MetaData()

    class Account(PlatformManagedMetaTable, Base):
        __tablename__ = "example_assets__account"
        __table_args__ = {"schema": "public"}
        __metatable_namespace__ = "example.assets"
        __metatable_identifier__ = "Account"

        uid: Mapped[uuid.UUID] = mapped_column(SQLAlchemyUuid, primary_key=True)

    assert Account.__table__.schema is None
    assert sqlalchemy_contracts._resolve_schema(Account.__table__) == "public"

    request = Account.build_registration_request(
        data_source_uid="dddddddd-dddd-4ddd-8ddd-dddddddddddd",
    )

    assert request.table_contract.physical.schema_ == "public"


def test_platform_managed_default_public_metadata_schema_stays_sqlalchemy_default():
    pytest.importorskip("sqlalchemy")

    from sqlalchemy import MetaData
    from sqlalchemy import Uuid as SQLAlchemyUuid
    from sqlalchemy.orm import DeclarativeBase, mapped_column

    class Base(DeclarativeBase):
        metadata = MetaData(schema="public")

    class Account(PlatformManagedMetaTable, Base):
        __tablename__ = "example_assets__account"
        __metatable_namespace__ = "example.assets"
        __metatable_identifier__ = "Account"

        uid: Mapped[uuid.UUID] = mapped_column(SQLAlchemyUuid, primary_key=True)

    assert Account.__table__.schema is None
    assert Account.__table__.fullname == "example_assets__account"
    assert "example_assets__account" in Base.metadata.tables
    assert "public.example_assets__account" not in Base.metadata.tables

    request = Account.build_registration_request(
        data_source_uid="dddddddd-dddd-4ddd-8ddd-dddddddddddd",
    )

    assert request.table_contract.physical.schema_ == "public"


def test_time_index_optional_table_info_resolvers_allow_pending_declarative_class():
    class PendingTimeIndexTable:
        __time_index_name__ = "time_index"
        __index_names__ = ["time_index", "account_uid"]

    assert sqlalchemy_contracts._resolve_time_index_storage_layout(PendingTimeIndexTable) is None
    assert sqlalchemy_contracts._resolve_time_index_name(PendingTimeIndexTable) == "time_index"
    assert sqlalchemy_contracts._resolve_time_index_names(
        PendingTimeIndexTable,
        time_index_name="time_index",
    ) == ["time_index", "account_uid"]


def test_platform_managed_preserves_authored_physical_table_name():
    table = FakeTable(
        "example_assets__asset",
        columns=[
            FakeColumn("uid", Uuid(), nullable=False, primary_key=True),
            FakeColumn("symbol", String(64), nullable=False),
        ],
    )
    Asset = _model_class("Asset", table)

    request = platform_managed_registration_request_from_sqlalchemy_model(
        Asset,
        data_source_uid="dddddddd-dddd-4ddd-8ddd-dddddddddddd",
    )

    assert request.management_mode == "platform_managed"
    _assert_omits_storage_hash(request)
    assert request.table_contract.physical.table_name == "example_assets__asset"


def test_platform_managed_metatable_build_request_uses_session_data_source(monkeypatch):
    import mainsequence.client.metatables as models_metatables

    table = FakeTable(
        "example_assets__account",
        columns=[FakeColumn("uid", Uuid(), nullable=False, primary_key=True)],
    )
    Account = _platform_model_class("Account", table)

    monkeypatch.setattr(
        models_metatables.SessionDataSource,
        "data_source",
        SimpleNamespace(
            uid="dddddddd-dddd-4ddd-8ddd-dddddddddddd",
            related_resource=SimpleNamespace(status="AVAILABLE"),
        ),
    )

    request = Account.build_registration_request(description="Account table")

    assert request.data_source_uid == "dddddddd-dddd-4ddd-8ddd-dddddddddddd"
    assert request.identifier == "Account"
    assert request.description == "Account table"
    _assert_omits_storage_hash(request)
    assert _configured_storage_hash(Account) != table.name


def test_platform_managed_metatable_does_not_use_physical_data_source_uid(monkeypatch):
    import mainsequence.client.metatables as models_metatables

    table = FakeTable(
        "example_assets__account",
        columns=[FakeColumn("uid", Uuid(), nullable=False, primary_key=True)],
    )
    Account = _platform_model_class("Account", table)

    monkeypatch.setattr(
        models_metatables.SessionDataSource,
        "data_source",
        SimpleNamespace(
            uid=None,
            related_resource=SimpleNamespace(
                uid="eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee",
                data_source_uid="eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee",
                status="AVAILABLE",
            ),
        ),
    )

    with pytest.raises(ValueError, match="time-indexed data source uid"):
        Account.build_registration_request()


def test_platform_managed_metatable_omits_bound_target_model_uid():
    account_table = FakeTable(
        "example_assets__account",
        columns=[FakeColumn("uid", Uuid(), nullable=False, primary_key=True)],
    )
    Account = _platform_model_class("Account", account_table)

    asset_uid = FakeColumn("uid", Uuid(), nullable=False, primary_key=True)
    account_uid_source = FakeColumn("account_uid", Uuid(), nullable=False)
    account_fk_element = FakeForeignKeyElement(
        account_uid_source,
        account_table.columns[0],
        ondelete="RESTRICT",
    )
    asset_fk = FakeForeignKeyConstraint(
        "asset_account_uid_fkey",
        [account_fk_element],
    )
    asset_table = FakeTable(
        "example_assets__asset",
        columns=[asset_uid, account_uid_source],
        foreign_keys=[asset_fk],
    )
    Asset = _platform_model_class("Asset", asset_table)
    Account._bind_meta_table(
        MetaTable.model_construct(
            uid="bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb",
            storage_hash=_configured_storage_hash(Account),
            data_source_uid="dddddddd-dddd-4ddd-8ddd-dddddddddddd",
        )
    )

    request = Asset.build_registration_request(
        data_source_uid="dddddddd-dddd-4ddd-8ddd-dddddddddddd",
    )

    assert not hasattr(request.table_contract, "foreign_keys")


def test_platform_managed_metatable_register_does_not_register_fk_targets_recursively(
    monkeypatch,
):
    account_table = FakeTable(
        "example_assets__account",
        columns=[FakeColumn("uid", Uuid(), nullable=False, primary_key=True)],
    )

    asset_uid = FakeColumn("uid", Uuid(), nullable=False, primary_key=True)
    account_uid_source = FakeColumn("account_uid", Uuid(), nullable=False)
    account_fk_element = FakeForeignKeyElement(
        account_uid_source,
        account_table.columns[0],
        ondelete="RESTRICT",
    )
    asset_fk = FakeForeignKeyConstraint(
        "asset_account_uid_fkey",
        [account_fk_element],
    )
    asset_table = FakeTable(
        "example_assets__asset",
        columns=[asset_uid, account_uid_source],
        foreign_keys=[asset_fk],
    )
    Asset = _platform_model_class("Asset", asset_table)

    captured_requests = []

    def fake_register(cls, request, timeout=None):
        captured_requests.append(request)
        return MetaTable.model_construct(
            uid="aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
            data_source_uid=request.data_source_uid,
            physical_table_name=request.table_contract.physical.table_name,
        )

    monkeypatch.setattr(
        sqlalchemy_contracts.MetaTable,
        "register",
        classmethod(fake_register),
    )

    Asset.__metatable_identifier__ = "ChildOverride"
    Asset.__metatable_description__ = "Child-specific description."
    Asset.__metatable_labels__ = ["child"]

    with platform_managed_migration_registration_context():
        Asset.register(data_source_uid="eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee")

    assert [request.identifier for request in captured_requests] == ["ChildOverride"]
    assert captured_requests[0].data_source_uid == "eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee"
    assert captured_requests[0].description == "Child-specific description."
    assert captured_requests[0].labels == ["child"]
    assert not hasattr(captured_requests[0].table_contract, "foreign_keys")


def test_platform_managed_register_reuses_local_registry(monkeypatch):
    table = FakeTable(
        "example_assets__account",
        columns=[FakeColumn("uid", Uuid(), nullable=False, primary_key=True)],
    )
    Account = _platform_model_class("Account", table)
    captured_requests = []

    def fake_register(cls, request, timeout=None):
        captured_requests.append(request)
        return MetaTable.model_construct(
            uid="aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
            data_source_uid=request.data_source_uid,
            physical_table_name=request.table_contract.physical.table_name,
        )

    monkeypatch.setattr(
        sqlalchemy_contracts.MetaTable,
        "register",
        classmethod(fake_register),
    )

    with platform_managed_migration_registration_context():
        first = Account.register()
        second = Account.register()

    assert first is second
    assert len(captured_requests) == 1


def test_platform_managed_register_clears_failed_registry_entry(monkeypatch):
    table = FakeTable(
        "example_assets__account",
        columns=[FakeColumn("uid", Uuid(), nullable=False, primary_key=True)],
    )
    Account = _platform_model_class("Account", table)

    def fake_register(cls, request, timeout=None):
        raise RuntimeError("registration failed")

    monkeypatch.setattr(
        sqlalchemy_contracts.MetaTable,
        "register",
        classmethod(fake_register),
    )

    with platform_managed_migration_registration_context():
        with pytest.raises(RuntimeError, match="registration failed"):
            Account.register()

    assert table.name not in sqlalchemy_contracts._METATABLE_REGISTRATION_REGISTRY


def test_platform_managed_register_ignores_foreign_key_cycles(monkeypatch):
    account_uid = FakeColumn("uid", Uuid(), nullable=False, primary_key=True)
    account_owner_uid = FakeColumn("owner_uid", Uuid(), nullable=False)
    account_table = FakeTable("example_assets__account", columns=[account_uid, account_owner_uid])
    Account = _platform_model_class("Account", account_table)

    owner_uid = FakeColumn("uid", Uuid(), nullable=False, primary_key=True)
    owner_account_uid = FakeColumn("account_uid", Uuid(), nullable=False)
    owner_table = FakeTable("example_assets__owner", columns=[owner_uid, owner_account_uid])

    account_table.foreign_key_constraints = [
        FakeForeignKeyConstraint(
            "account_owner_uid_fkey",
            [FakeForeignKeyElement(account_owner_uid, owner_uid, ondelete="RESTRICT")],
        )
    ]
    owner_table.foreign_key_constraints = [
        FakeForeignKeyConstraint(
            "owner_account_uid_fkey",
            [FakeForeignKeyElement(owner_account_uid, account_uid, ondelete="RESTRICT")],
        )
    ]

    captured_requests = []

    def fake_register(cls, request, timeout=None):
        captured_requests.append(request)
        return MetaTable.model_construct(
            uid="aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
            data_source_uid=request.data_source_uid,
            physical_table_name=request.table_contract.physical.table_name,
        )

    monkeypatch.setattr(
        sqlalchemy_contracts.MetaTable,
        "register",
        classmethod(fake_register),
    )

    with platform_managed_migration_registration_context():
        Account.register(data_source_uid="dddddddd-dddd-4ddd-8ddd-dddddddddddd")

    assert [request.identifier for request in captured_requests] == ["Account"]


def test_platform_managed_omits_foreign_key_contracts():
    account_table = FakeTable(
        "example_assets__account",
        columns=[FakeColumn("uid", Uuid(), nullable=False, primary_key=True)],
    )
    Account = _platform_model_class("Account", account_table)
    Account._bind_meta_table(
        MetaTable.model_construct(
            uid="bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb",
            storage_hash=_configured_storage_hash(Account),
            data_source_uid="dddddddd-dddd-4ddd-8ddd-dddddddddddd",
        )
    )

    asset_uid = FakeColumn("uid", Uuid(), nullable=False, primary_key=True)
    account_uid_source = FakeColumn("account_uid", Uuid(), nullable=False)
    asset_table = FakeTable(
        "asset_storage",
        columns=[asset_uid, account_uid_source],
        foreign_keys=[
            FakeForeignKeyConstraint(
                None,
                [
                    FakeForeignKeyElement(
                        account_uid_source,
                        account_table.columns[0],
                        ondelete="RESTRICT",
                    )
                ],
            )
        ],
    )
    Asset = _platform_model_class("Asset", asset_table)

    request = Asset.build_registration_request(
        data_source_uid="dddddddd-dddd-4ddd-8ddd-dddddddddddd",
    )

    assert not hasattr(request.table_contract, "foreign_keys")
    assert "foreign_keys" not in request.table_contract.model_dump(
        mode="json",
        exclude_none=True,
    )


def test_platform_managed_metatable_allows_normal_sqlalchemy_foreign_key():
    account_table = FakeTable(
        "account_storage_hash",
        columns=[FakeColumn("uid", Uuid(), nullable=False, primary_key=True)],
    )

    asset_uid = FakeColumn("uid", Uuid(), nullable=False, primary_key=True)
    account_uid_source = FakeColumn("account_uid", Uuid(), nullable=False)
    asset_fk = FakeForeignKeyConstraint(
        "asset_account_uid_fkey",
        [
            FakeForeignKeyElement(
                account_uid_source,
                account_table.columns[0],
                ondelete="RESTRICT",
            )
        ],
    )
    asset_table = FakeTable(
        "example_assets__asset",
        columns=[asset_uid, account_uid_source],
        foreign_keys=[asset_fk],
    )
    Asset = _platform_model_class("Asset", asset_table)

    request = Asset.build_registration_request(
        data_source_uid="dddddddd-dddd-4ddd-8ddd-dddddddddddd",
    )

    assert not hasattr(request.table_contract, "foreign_keys")


def test_platform_managed_metatable_register_delegates_to_meta_table_register(monkeypatch):
    table = FakeTable(
        "example_assets__account",
        columns=[FakeColumn("uid", Uuid(), nullable=False, primary_key=True)],
    )
    Account = _platform_model_class("Account", table)

    captured = {}

    def fake_register(cls, request, timeout=None):
        captured["request"] = request
        captured["timeout"] = timeout
        return MetaTable.model_construct(
            uid="aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
            data_source_uid="dddddddd-dddd-4ddd-8ddd-dddddddddddd",
            physical_table_name="mt_aaaaaaaaaaaa4aaa8aaaaaaaaaaaaaaa",
        )

    monkeypatch.setattr(
        sqlalchemy_contracts.MetaTable,
        "register",
        classmethod(fake_register),
    )

    with platform_managed_migration_registration_context():
        registered = Account.register(
            data_source_uid="eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee",
            timeout=15,
        )

    assert registered.uid == "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
    assert Account.get_meta_table() is registered
    assert Account.get_meta_table_uid() == "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
    assert Account.get_data_source_uid() == "dddddddd-dddd-4ddd-8ddd-dddddddddddd"
    assert Account.get_physical_table_name() == "example_assets__account"
    assert Account.__table__.name == "example_assets__account"
    assert captured["timeout"] == 15
    assert captured["request"].data_source_uid == "eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee"
    _assert_omits_storage_hash(captured["request"])
    assert captured["request"].table_contract.physical.table_name == "example_assets__account"
    assert not hasattr(Account, "bind_meta_table")


def test_platform_managed_register_requires_migration_context():
    table = FakeTable(
        "example_assets__account",
        columns=[FakeColumn("uid", Uuid(), nullable=False, primary_key=True)],
    )
    Account = _platform_model_class("Account", table)

    with pytest.raises(ValueError, match="migration-managed"):
        Account.register()


def test_platform_managed_free_function_register_path_is_not_public():
    import mainsequence.meta_tables as meta_tables

    assert not hasattr(sqlalchemy_contracts, "register_platform_managed_sqlalchemy_model")
    assert "register_platform_managed_sqlalchemy_model" not in meta_tables.__all__
    missing_name = "register_platform_managed_sqlalchemy_model"
    with pytest.raises(AttributeError):
        getattr(meta_tables, missing_name)


def test_time_index_meta_table_registration_request_uses_dynamic_contract():
    table = FakeTable(
        "placeholder",
        columns=[
            FakeColumn("time_index", DateTime(timezone=True), nullable=False),
            FakeColumn("account_uid", Uuid(), nullable=False),
            FakeColumn("unique_identifier", String(255), nullable=False),
            FakeColumn("quantity", String(64), nullable=False),
        ],
    )
    AccountHoldings = _time_index_model_class(
        "AccountHoldings",
        table,
        index_names=["time_index", "account_uid", "unique_identifier"],
    )
    table.name = "example_assets__account_holdings"

    request = AccountHoldings.build_registration_request(
        data_source_uid="dddddddd-dddd-4ddd-8ddd-dddddddddddd",
        description="Account holdings data node storage",
    )

    assert isinstance(request, TimeIndexMetaTableRegistrationRequest)
    _assert_omits_storage_hash(request)
    assert _configured_storage_hash(AccountHoldings) != table.name
    assert request.identifier == "AccountHoldings"
    assert request.namespace == "example.assets"
    assert request.description == "Account holdings data node storage"
    assert request.time_index_name == "time_index"
    assert request.table_contract["physical"]["table_name"] == table.name
    assert request.table_contract["authoring"]["time_indexed"]["index_names"] == [
        "time_index",
        "account_uid",
        "unique_identifier",
    ]
    assert [column["name"] for column in request.table_contract["columns"]] == [
        "time_index",
        "account_uid",
        "unique_identifier",
        "quantity",
    ]

    payload = request.model_dump(mode="json", exclude_none=True)
    assert "table_contract" in payload
    assert "columns" not in payload
    assert "index_names" not in payload
    assert "foreign_keys" not in payload
    assert "storage_layout" not in payload
    assert "management_mode" not in payload
    assert "physical_table_name" not in payload
    assert "identity_dimensions" not in payload
    assert "index_progress" not in payload
    assert "tail_delete" not in payload
    assert "uniqueness" not in payload
    assert "physical_index_plan" not in payload


def test_time_index_meta_table_registration_request_uses_class_cadence():
    table = FakeTable(
        "example_assets__prices",
        columns=[
            FakeColumn("time_index", DateTime(timezone=True), nullable=False),
            FakeColumn("asset_uid", Uuid(), nullable=False),
            FakeColumn("price", String(64), nullable=False),
        ],
    )
    DailyPrices = _time_index_model_class(
        "DailyPrices",
        table,
        index_names=["time_index", "asset_uid"],
        cadence="1D",
    )

    request = DailyPrices.build_registration_request(
        data_source_uid="dddddddd-dddd-4ddd-8ddd-dddddddddddd",
    )

    assert request.cadence == "1d"
    assert request.table_contract["authoring"]["time_indexed"]["cadence"] == "1d"
    payload = request.model_dump(mode="json", exclude_none=True)
    assert payload["cadence"] == "1d"


def test_time_index_meta_table_cadence_changes_configured_storage_hash():
    table = FakeTable(
        "example_assets__prices",
        columns=[
            FakeColumn("time_index", DateTime(timezone=True), nullable=False),
            FakeColumn("asset_uid", Uuid(), nullable=False),
            FakeColumn("price", String(64), nullable=False),
        ],
    )
    DailyPrices = _time_index_model_class(
        "DailyPrices",
        table,
        index_names=["time_index", "asset_uid"],
        cadence="1d",
    )
    IntradayPrices = _time_index_model_class(
        "IntradayPrices",
        table,
        index_names=["time_index", "asset_uid"],
        cadence="5m",
    )

    assert _configured_storage_hash(DailyPrices) != _configured_storage_hash(IntradayPrices)


def test_time_index_meta_table_rejects_invalid_class_cadence():
    table = FakeTable(
        "example_assets__prices",
        columns=[
            FakeColumn("time_index", DateTime(timezone=True), nullable=False),
            FakeColumn("asset_uid", Uuid(), nullable=False),
        ],
    )
    Prices = _time_index_model_class(
        "Prices",
        table,
        index_names=["time_index", "asset_uid"],
        cadence="daily",
    )

    with pytest.raises(ValueError, match="cadence"):
        Prices.build_registration_request(
            data_source_uid="dddddddd-dddd-4ddd-8ddd-dddddddddddd",
        )


def test_time_index_meta_table_registration_request_uses_class_metatable_description():
    table = FakeTable(
        "placeholder",
        columns=[
            FakeColumn("time_index", DateTime(timezone=True), nullable=False),
            FakeColumn("account_uid", Uuid(), nullable=False),
            FakeColumn("quantity", String(64), nullable=False),
        ],
    )
    AccountHoldings = _time_index_model_class(
        "AccountHoldings",
        table,
        index_names=["time_index", "account_uid"],
    )
    AccountHoldings.__metatable_description__ = (
        "Account holdings history used to reconstruct portfolio state."
    )
    table.name = "example_assets__account_holdings"

    request = AccountHoldings.build_registration_request(
        data_source_uid="dddddddd-dddd-4ddd-8ddd-dddddddddddd",
    )

    assert request.description == ("Account holdings history used to reconstruct portfolio state.")


def test_time_index_meta_table_configured_storage_hash_changes_with_index_grain():
    one_index_table = FakeTable(
        "placeholder",
        columns=[
            FakeColumn("time_index", DateTime(timezone=True), nullable=False),
            FakeColumn("unique_identifier", String(255), nullable=False),
        ],
    )
    three_index_table = FakeTable(
        "placeholder",
        columns=[
            FakeColumn("time_index", DateTime(timezone=True), nullable=False),
            FakeColumn("account_uid", Uuid(), nullable=False),
            FakeColumn("unique_identifier", String(255), nullable=False),
        ],
    )
    OneIndex = _time_index_model_class(
        "AccountHoldings",
        one_index_table,
        index_names=["time_index"],
    )
    ThreeIndex = _time_index_model_class(
        "AccountHoldings",
        three_index_table,
        index_names=["time_index", "account_uid", "unique_identifier"],
    )

    assert _configured_storage_hash(OneIndex) != _configured_storage_hash(ThreeIndex)


def test_time_index_meta_table_rejects_first_index_not_time_index():
    table = FakeTable(
        "placeholder",
        columns=[
            FakeColumn("time_index", DateTime(timezone=True), nullable=False),
            FakeColumn("account_uid", Uuid(), nullable=False),
        ],
    )
    BadHoldings = _time_index_model_class(
        "BadHoldings",
        table,
        index_names=["account_uid", "time_index"],
    )

    with pytest.raises(ValueError, match="must start with the time_index_name"):
        _configured_storage_hash(BadHoldings)


def test_time_index_meta_table_rejects_nullable_index_columns():
    table = FakeTable(
        "placeholder",
        columns=[
            FakeColumn("time_index", DateTime(timezone=True), nullable=False),
            FakeColumn("account_uid", Uuid(), nullable=True),
        ],
    )
    BadHoldings = _time_index_model_class(
        "BadHoldings",
        table,
        index_names=["time_index", "account_uid"],
    )

    with pytest.raises(ValueError, match="index columns must be non-nullable"):
        _configured_storage_hash(BadHoldings)


def test_time_index_meta_table_register_posts_to_dynamic_table_endpoint(monkeypatch):
    import mainsequence.client.metatables as models_metatables

    table = FakeTable(
        "placeholder",
        columns=[
            FakeColumn("time_index", DateTime(timezone=True), nullable=False),
            FakeColumn("account_uid", Uuid(), nullable=False),
            FakeColumn("unique_identifier", String(255), nullable=False),
        ],
    )
    AccountHoldings = _time_index_model_class(
        "AccountHoldings",
        table,
        index_names=["time_index", "account_uid", "unique_identifier"],
    )
    table.name = "example_assets__account_holdings"

    captured = {}

    class Response:
        status_code = 201
        content = b"{}"

        def json(self):
            return {
                "uid": "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
                "data_source_uid": "dddddddd-dddd-4ddd-8ddd-dddddddddddd",
                "identifier": "AccountHoldings",
                "namespace": "example.assets",
                "management_mode": "platform_managed",
                "physical_table_name": "mt_aaaaaaaaaaaa4aaa8aaaaaaaaaaaaaaa",
                "table_contract": {
                    "version": "relational-table.v1",
                    "physical": {"table_name": "mt_aaaaaaaaaaaa4aaa8aaaaaaaaaaaaaaa"},
                    "columns": [],
                    "dynamic_table": {
                        "time_index_name": "time_index",
                        "index_names": ["time_index", "account_uid", "unique_identifier"],
                    },
                },
                "time_indexed_profile": {
                    "dynamic_table_uid": "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
                    "time_index_name": "time_index",
                    "index_names": ["time_index", "account_uid", "unique_identifier"],
                    "column_dtypes_map": {
                        "time_index": "timestamp with time zone",
                        "account_uid": "uuid",
                        "unique_identifier": "string",
                    },
                },
            }

    def fake_make_request(*, s, loaders, r_type, url, payload=None, time_out=None):
        captured["r_type"] = r_type
        captured["url"] = url
        captured["payload"] = payload
        captured["timeout"] = time_out
        return Response()

    monkeypatch.setattr(models_metatables, "make_request", fake_make_request)

    with platform_managed_migration_registration_context():
        registered = AccountHoldings.register(
            data_source_uid="eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee",
            timeout=15,
        )

    assert registered.uid == "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
    assert AccountHoldings.get_meta_table() is registered
    assert AccountHoldings.get_time_index_meta_table() is registered
    assert AccountHoldings.get_meta_table_uid() == "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
    assert AccountHoldings.get_physical_table_name() == "example_assets__account_holdings"
    assert AccountHoldings.__table__.name == "example_assets__account_holdings"
    assert captured["r_type"] == "POST"
    assert captured["url"].endswith("/ts_manager/dynamic_table/register/")
    assert captured["timeout"] == 15
    assert captured["payload"]["json"]["data_source_uid"] == (
        "eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee"
    )
    assert "storage_hash" not in captured["payload"]["json"]
    assert captured["payload"]["json"]["time_index_name"] == "time_index"
    assert "index_names" not in captured["payload"]["json"]
    assert captured["payload"]["json"]["table_contract"]["authoring"]["time_indexed"][
        "index_names"
    ] == [
        "time_index",
        "account_uid",
        "unique_identifier",
    ]


def test_time_index_meta_table_bind_accepts_typed_metadata():
    table = FakeTable(
        "example_assets__account_holdings",
        columns=[
            FakeColumn("time_index", DateTime(timezone=True), nullable=False),
            FakeColumn("account_uid", Uuid(), nullable=False),
        ],
    )
    AccountHoldings = _time_index_model_class(
        "AccountHoldings",
        table,
        index_names=["time_index", "account_uid"],
    )

    typed_meta_table = TimeIndexMetaTable.model_construct(
        uid="aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
        data_source_uid="dddddddd-dddd-4ddd-8ddd-dddddddddddd",
        storage_hash="holdings-storage-hash",
        physical_table_name="mt_holdings",
    )

    AccountHoldings._bind_meta_table(typed_meta_table)

    assert AccountHoldings.get_time_index_meta_table() is typed_meta_table


def test_time_index_meta_table_bind_rejects_unflagged_generic_metatable():
    class AccountHoldings(PlatformTimeIndexMetaTable):
        pass

    generic_meta_table = MetaTable.model_construct(
        uid="aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
        data_source_uid="dddddddd-dddd-4ddd-8ddd-dddddddddddd",
        storage_hash="holdings-storage-hash",
        management_mode="platform_managed",
        physical_table_name="mt_holdings",
    )

    with pytest.raises(TypeError, match="requires TimeIndexMetaTable"):
        AccountHoldings._bind_meta_table(generic_meta_table)


def test_ensure_registered_storage_table_rejects_unbound_storage(monkeypatch):
    columns = [
        FakeColumn("time_index", DateTime(timezone=True), nullable=False),
        FakeColumn("asset_uid", Uuid(), nullable=False),
    ]
    table = FakeTable("example_assets__asset_snapshots", columns=columns)
    AssetSnapshots = _time_index_model_class(
        "AssetSnapshots",
        table,
        index_names=["time_index", "asset_uid"],
    )
    AssetSnapshots.__metatable_data_source_uid__ = "dddddddd-dddd-4ddd-8ddd-dddddddddddd"
    monkeypatch.setattr(
        TimeIndexMetaTable,
        "filter_by_body",
        classmethod(lambda cls, **filters: []),
    )

    with pytest.raises(ValueError) as exc_info:
        ensure_registered_storage_table(AssetSnapshots, context="DataNode")
    message = str(exc_info.value)
    assert "not bound to backend TimeIndexMetaTable" in message
    assert "found no backend TimeIndexMetaTable catalog row" in message
    assert "example_assets__asset_snapshots" in message


def test_ensure_registered_storage_table_binds_existing_time_index_meta_table(monkeypatch):
    columns = [
        FakeColumn("time_index", DateTime(timezone=True), nullable=False),
        FakeColumn("asset_uid", Uuid(), nullable=False),
    ]
    table = FakeTable("example_assets__asset_snapshots", columns=columns)
    AssetSnapshots = _time_index_model_class(
        "AssetSnapshots",
        table,
        index_names=["time_index", "asset_uid"],
    )
    AssetSnapshots.__metatable_data_source_uid__ = "dddddddd-dddd-4ddd-8ddd-dddddddddddd"
    backend_metadata = TimeIndexMetaTable.model_construct(
        uid="aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
        data_source_uid="dddddddd-dddd-4ddd-8ddd-dddddddddddd",
        storage_hash="storage-hash",
        physical_schema="public",
        physical_table_name="example_assets__asset_snapshots",
    )
    captured = {}

    def fake_filter_by_body(cls, **filters):
        captured.update(filters)
        return [backend_metadata]

    monkeypatch.setattr(
        TimeIndexMetaTable,
        "filter_by_body",
        classmethod(fake_filter_by_body),
    )

    assert (
        ensure_registered_storage_table(AssetSnapshots, context="DataNode")
        is AssetSnapshots
    )
    assert AssetSnapshots.get_time_index_meta_table() is backend_metadata
    assert AssetSnapshots.get_meta_table_uid() == "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
    assert captured == {
        "data_source__uid": "dddddddd-dddd-4ddd-8ddd-dddddddddddd",
        "physical_schema__in": ["public"],
        "physical_table_name__in": ["example_assets__asset_snapshots"],
        "limit": 20,
    }


def test_ensure_registered_storage_table_reports_duplicate_matches(monkeypatch):
    columns = [
        FakeColumn("time_index", DateTime(timezone=True), nullable=False),
        FakeColumn("asset_uid", Uuid(), nullable=False),
    ]
    table = FakeTable("example_assets__asset_snapshots", columns=columns)
    AssetSnapshots = _time_index_model_class(
        "AssetSnapshots",
        table,
        index_names=["time_index", "asset_uid"],
    )
    AssetSnapshots.__metatable_data_source_uid__ = "dddddddd-dddd-4ddd-8ddd-dddddddddddd"
    first = TimeIndexMetaTable.model_construct(
        uid="aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
        data_source_uid="dddddddd-dddd-4ddd-8ddd-dddddddddddd",
        storage_hash="storage-hash",
        physical_schema="public",
        physical_table_name="example_assets__asset_snapshots",
    )
    second = TimeIndexMetaTable.model_construct(
        uid="bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb",
        data_source_uid="dddddddd-dddd-4ddd-8ddd-dddddddddddd",
        storage_hash="storage-hash",
        physical_schema="public",
        physical_table_name="example_assets__asset_snapshots",
    )

    monkeypatch.setattr(
        TimeIndexMetaTable,
        "filter_by_body",
        classmethod(lambda cls, **filters: [first, second]),
    )

    with pytest.raises(ValueError) as exc_info:
        ensure_registered_storage_table(AssetSnapshots, context="DataNode")
    message = str(exc_info.value)
    assert "found 2 matching backend TimeIndexMetaTable catalog rows" in message
    assert "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa" in message
    assert "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb" in message


def test_contract_hash_extra_components_are_explicit_utility_inputs():
    columns_a = [
        FakeColumn("time_index", DateTime(timezone=True), nullable=False),
        FakeColumn("random_number", String(255), nullable=False),
    ]
    columns_b = [
        FakeColumn("time_index", DateTime(timezone=True), nullable=False),
        FakeColumn("random_number", String(255), nullable=False),
    ]
    table_a = FakeTable("mainsequence_examples__daily_random_number", columns=columns_a)
    table_b = FakeTable("mainsequence_examples__daily_random_addition", columns=columns_b)
    RandomNumber = _time_index_model_class(
        "RandomNumber",
        table_a,
        namespace="mainsequence.examples",
        identifier="daily_random_number_project",
    )
    RandomAddition = _time_index_model_class(
        "RandomAddition",
        table_b,
        namespace="mainsequence.examples",
        identifier="daily_random_addition_project",
    )

    random_number_storage_hash = _configured_storage_hash(RandomNumber)
    random_addition_storage_hash = _configured_storage_hash(RandomAddition)
    assert random_number_storage_hash != random_addition_storage_hash
    assert (
        sqlalchemy_contracts.compute_metatable_contract_hash(
            RandomNumber,
            extra_components={"storage_name": "daily_random_number"},
        )
        != sqlalchemy_contracts.compute_metatable_contract_hash(
            RandomNumber,
            extra_components={"storage_name": "daily_random_addition"},
        )
    )

    request = time_indexed_registration_request_from_sqlalchemy_model(
        RandomNumber,
        data_source_uid="dddddddd-dddd-4ddd-8ddd-dddddddddddd",
    )
    _assert_omits_storage_hash(request)
    assert request.table_contract["physical"]["table_name"] == table_a.name
    assert request.identifier == "daily_random_number_project"
    assert request.namespace == "mainsequence.examples"


def test_platform_managed_metatable_preserves_authored_tablename_with_sqlalchemy():
    pytest.importorskip("sqlalchemy")

    from sqlalchemy import ForeignKey, Index, MetaData, String, Uuid
    from sqlalchemy.orm import DeclarativeBase, mapped_column

    naming_convention = {
        "ix": "%(table_name)s_%(column_0_name)s_idx",
        "fk": "%(table_name)s_%(column_0_name)s_fkey",
        "pk": "%(table_name)s_pkey",
    }

    class Base(DeclarativeBase):
        metadata = MetaData(naming_convention=naming_convention)

    class Account(PlatformManagedMetaTable, Base):
        __tablename__ = "example_assets__account"
        __table_args__ = {"schema": "public"}

        __metatable_namespace__ = "example.assets"
        __metatable_identifier__ = "Account"

        uid: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True)
        name: Mapped[str] = mapped_column(String(255), nullable=False)

    class Asset(PlatformManagedMetaTable, Base):
        __tablename__ = "example_assets__asset"
        __table_args__ = (
            Index(None, "account_uid"),
            {"schema": "public"},
        )

        __metatable_namespace__ = "example.assets"
        __metatable_identifier__ = "Asset"

        uid: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True)
        account_uid: Mapped[uuid.UUID] = mapped_column(
            Uuid,
            ForeignKey("public.example_assets__account.uid", ondelete="RESTRICT"),
            nullable=False,
        )
        symbol: Mapped[str] = mapped_column(String(64), nullable=False)

    assert Account.__table__.name == "example_assets__account"
    assert Asset.__table__.name == "example_assets__asset"
    Account._bind_meta_table(
        MetaTable.model_construct(
            uid="bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb",
            data_source_uid="dddddddd-dddd-4ddd-8ddd-dddddddddddd",
            storage_hash=_configured_storage_hash(Account),
        )
    )

    request = platform_managed_registration_request_from_sqlalchemy_model(
        Asset,
        data_source_uid="dddddddd-dddd-4ddd-8ddd-dddddddddddd",
    )

    _assert_omits_storage_hash(request)
    assert request.table_contract.physical.table_name == "example_assets__asset"
    assert not hasattr(request.table_contract, "indexes")
    assert not hasattr(request.table_contract, "foreign_keys")


def test_time_index_meta_table_generates_unique_grain_index_with_schema_name():
    pytest.importorskip("sqlalchemy")

    from sqlalchemy import DateTime, Float, MetaData, String
    from sqlalchemy.orm import DeclarativeBase, mapped_column

    class Base(DeclarativeBase):
        metadata = MetaData(naming_convention=sqlalchemy_naming_convention())

    class Prices(PlatformTimeIndexMetaTable, Base):
        __tablename__ = "ms_markets__prices__mainsequence_examples"
        __metatable_namespace__ = "mainsequence.examples"
        __time_index_name__ = "time_index"
        __index_names__ = ["time_index", "asset_identifier"]

        time_index: Mapped[datetime.datetime] = mapped_column(
            DateTime(timezone=True),
            nullable=False,
        )
        asset_identifier: Mapped[str] = mapped_column(String(255), nullable=False)
        close: Mapped[float] = mapped_column(Float, nullable=True)

    grain_indexes = [
        index
        for index in Prices.__table__.indexes
        if index.unique
        and [column.name for column in index.columns] == ["time_index", "asset_identifier"]
    ]

    assert len(grain_indexes) == 1
    assert grain_indexes[0].name == schema_index_name(
        "ms_markets__prices__mainsequence_examples",
        ["time_index", "asset_identifier"],
        unique=True,
    )


def test_time_index_meta_table_reuses_existing_unique_grain_constraint():
    pytest.importorskip("sqlalchemy")

    from sqlalchemy import DateTime, Float, MetaData, String, UniqueConstraint
    from sqlalchemy.orm import DeclarativeBase, mapped_column

    class Base(DeclarativeBase):
        metadata = MetaData(naming_convention=sqlalchemy_naming_convention())

    class Prices(PlatformTimeIndexMetaTable, Base):
        __tablename__ = "ms_markets__prices__mainsequence_examples"
        __table_args__ = (
            UniqueConstraint(
                "asset_identifier",
                "time_index",
                name="uix_custom_asset_time",
            ),
        )
        __metatable_namespace__ = "mainsequence.examples"
        __time_index_name__ = "time_index"
        __index_names__ = ["time_index", "asset_identifier"]

        time_index: Mapped[datetime.datetime] = mapped_column(
            DateTime(timezone=True),
            nullable=False,
        )
        asset_identifier: Mapped[str] = mapped_column(String(255), nullable=False)
        close: Mapped[float] = mapped_column(Float, nullable=True)

    assert not Prices.__table__.indexes
    unique_constraints = [
        constraint
        for constraint in Prices.__table__.constraints
        if isinstance(constraint, UniqueConstraint)
    ]
    assert len(unique_constraints) == 1
    assert [column.name for column in unique_constraints[0].columns] == [
        "asset_identifier",
        "time_index",
    ]


def test_platform_managed_register_preserves_authored_sqlalchemy_table_name(
    monkeypatch,
):
    pytest.importorskip("sqlalchemy")

    from sqlalchemy import MetaData, String, Uuid, select
    from sqlalchemy.dialects import postgresql
    from sqlalchemy.orm import DeclarativeBase, mapped_column

    class Base(DeclarativeBase):
        metadata = MetaData()

    class Account(PlatformManagedMetaTable, Base):
        __tablename__ = "example_assets__account"
        __metatable_namespace__ = "example.assets"
        __metatable_identifier__ = "Account"

        uid: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True)
        name: Mapped[str] = mapped_column(String(255), nullable=False)

    storage_hash = _configured_storage_hash(Account)
    physical_table_name = "mt_aaaaaaaaaaaa4aaa8aaaaaaaaaaaaaaa"
    captured = {}

    def fake_register(cls, request, timeout=None):
        captured["request"] = request
        return MetaTable.model_construct(
            uid="aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
            data_source_uid="dddddddd-dddd-4ddd-8ddd-dddddddddddd",
            physical_table_name=physical_table_name,
        )

    monkeypatch.setattr(
        sqlalchemy_contracts.MetaTable,
        "register",
        classmethod(fake_register),
    )

    with platform_managed_migration_registration_context():
        Account.register()

    _assert_omits_storage_hash(captured["request"])
    assert captured["request"].table_contract.physical.table_name == "example_assets__account"
    assert _configured_storage_hash(Account) == storage_hash
    assert Account.get_physical_table_name() == "example_assets__account"
    assert Account.__table__.name == "example_assets__account"
    assert Account.__table__.schema is None
    assert Account.__table__.fullname == "example_assets__account"
    assert Base.metadata.tables["example_assets__account"] is Account.__table__
    assert f"public.{physical_table_name}" not in Base.metadata.tables

    compiled_sql = str(
        select(Account.__table__).compile(dialect=postgresql.dialect(paramstyle="pyformat"))
    )
    assert "FROM example_assets__account" in compiled_sql
    assert physical_table_name not in compiled_sql
    assert storage_hash not in compiled_sql


def test_bound_parent_table_foreign_key_stays_sqlalchemy_only():
    pytest.importorskip("sqlalchemy")

    from sqlalchemy import ForeignKey, MetaData, String, Uuid
    from sqlalchemy.orm import DeclarativeBase, mapped_column

    class Base(DeclarativeBase):
        metadata = MetaData()

    class Account(PlatformManagedMetaTable, Base):
        __tablename__ = "example_assets__account"
        __metatable_namespace__ = "example.assets"
        __metatable_identifier__ = "Account"

        uid: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True)

    account_storage_hash = _configured_storage_hash(Account)
    account_physical_table_name = "mt_aaaaaaaaaaaa4aaa8aaaaaaaaaaaaaaa"
    Account._bind_meta_table(
        MetaTable.model_construct(
            uid="aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
            data_source_uid="dddddddd-dddd-4ddd-8ddd-dddddddddddd",
            storage_hash=account_storage_hash,
            physical_table_name=account_physical_table_name,
        )
    )

    assert Account.__table__.schema is None
    assert Account.__table__.fullname == "example_assets__account"

    class Asset(PlatformManagedMetaTable, Base):
        __tablename__ = "example_assets__asset"
        __metatable_namespace__ = "example.assets"
        __metatable_identifier__ = "Asset"

        uid: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True)
        account_uid: Mapped[uuid.UUID] = mapped_column(
            Uuid,
            ForeignKey("example_assets__account.uid", ondelete="RESTRICT"),
            nullable=False,
        )
        symbol: Mapped[str] = mapped_column(String(64), nullable=False)

    foreign_key = next(iter(Asset.__table__.foreign_keys))
    assert foreign_key.ondelete == "RESTRICT"
    assert Asset.__table__.name == "example_assets__asset"

    request = Asset.build_registration_request(
        data_source_uid="dddddddd-dddd-4ddd-8ddd-dddddddddddd",
    )

    assert not hasattr(request.table_contract, "foreign_keys")


def test_time_index_meta_table_preserves_authored_tablename_with_sqlalchemy():
    pytest.importorskip("sqlalchemy")

    from sqlalchemy import DateTime, ForeignKey, Index, MetaData, String, Uuid
    from sqlalchemy.orm import DeclarativeBase, mapped_column

    class Base(DeclarativeBase):
        metadata = MetaData()

    class Account(PlatformManagedMetaTable, Base):
        __tablename__ = "example_assets__account"
        __metatable_namespace__ = "example.assets"
        __metatable_identifier__ = "Account"

        uid: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True)
        account_code: Mapped[str] = mapped_column(String(64), nullable=False, unique=True)
        name: Mapped[str] = mapped_column(String(255), nullable=False)

    class AccountHoldings(PlatformTimeIndexMetaTable, Base):
        __tablename__ = "example_assets__account_holdings"
        __table_args__ = (
            Index(None, "account_uid"),
            {"schema": "public"},
        )
        __metatable_namespace__ = "example.assets"
        __metatable_identifier__ = "AccountHoldings"
        __time_index_name__ = "time_index"
        __index_names__ = ["time_index", "account_uid", "unique_identifier"]

        time_index: Mapped[datetime.datetime] = mapped_column(
            DateTime(timezone=True),
            nullable=False,
        )
        account_uid: Mapped[uuid.UUID] = mapped_column(
            Uuid,
            ForeignKey("example_assets__account.uid", ondelete="RESTRICT"),
            nullable=False,
        )
        unique_identifier: Mapped[str] = mapped_column(String(255), nullable=False)

    assert AccountHoldings.__table__.name == "example_assets__account_holdings"
    assert list(AccountHoldings.__table__.primary_key.columns.keys()) == []
    assert [column.name for column in AccountHoldings.__mapper__.primary_key] == [
        "time_index",
        "account_uid",
        "unique_identifier",
    ]
    Account._bind_meta_table(
        MetaTable.model_construct(
            uid="bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb",
            data_source_uid="dddddddd-dddd-4ddd-8ddd-dddddddddddd",
            storage_hash=_configured_storage_hash(Account),
        )
    )

    request = AccountHoldings.build_registration_request(
        data_source_uid="dddddddd-dddd-4ddd-8ddd-dddddddddddd",
    )

    _assert_omits_storage_hash(request)
    assert request.time_index_name == "time_index"
    assert request.table_contract["physical"]["table_name"] == "example_assets__account_holdings"
    assert request.table_contract["authoring"]["time_indexed"]["index_names"] == [
        "time_index",
        "account_uid",
        "unique_identifier",
    ]
    assert [column["primary_key"] for column in request.table_contract["columns"]] == [
        False,
        False,
        False,
    ]
    assert "foreign_keys" not in request.table_contract
    assert "indexes" not in request.table_contract


def test_platform_managed_allows_authored_table_name_distinct_from_storage_hash():
    table = FakeTable(
        "asset",
        columns=[FakeColumn("uid", Uuid(), nullable=False, primary_key=True)],
    )
    Asset = _model_class("Asset", table)

    request = platform_managed_registration_request_from_sqlalchemy_model(
        Asset,
        data_source_uid="dddddddd-dddd-4ddd-8ddd-dddddddddddd",
    )

    _assert_omits_storage_hash(request)
    assert _configured_storage_hash(Asset) != "asset"
    assert request.table_contract.physical.table_name == "asset"


def test_external_registered_allows_physical_table_name_to_differ_from_storage_hash():
    table = FakeTable(
        "asset",
        columns=[FakeColumn("uid", Uuid(), nullable=False, primary_key=True)],
    )
    Asset = _model_class("Asset", table)

    request = external_registered_registration_request_from_sqlalchemy_model(
        Asset,
        data_source_uid="dddddddd-dddd-4ddd-8ddd-dddddddddddd",
    )

    assert request.management_mode == "external_registered"
    _assert_omits_storage_hash(request)
    assert _configured_storage_hash(Asset) != "asset"
    assert request.table_contract.physical.table_name == "asset"
    assert request.introspect is True
