from __future__ import annotations

import datetime
import uuid
from types import SimpleNamespace

import pytest
from sqlalchemy.orm import Mapped

import mainsequence.meta_tables.sqlalchemy_contracts as sqlalchemy_contracts
from mainsequence.client.models_metatables import (
    MetaTableRegistrationRequest,
    TimeIndexMetaTableRegistrationRequest,
)
from mainsequence.meta_tables import (
    MetaTableForeignKey,
    PlatformManagedMetaTable,
    PlatformTimeIndexMetaData,
    external_registered_registration_request_from_sqlalchemy_model,
    metatable_configured_tablename,
    metatable_tablename,
    platform_managed_registration_request_from_sqlalchemy_model,
    table_contract_from_sqlalchemy_model,
    time_indexed_registration_request_from_sqlalchemy_model,
)
from mainsequence.meta_tables.data_nodes.persist_managers import ensure_registered_storage_table


@pytest.fixture(autouse=True)
def _clear_metatable_registration_registry(monkeypatch):
    monkeypatch.setattr(
        "mainsequence.client.models_metatables.SessionDataSource.data_source",
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
    return type(
        name,
        (PlatformTimeIndexMetaData,),
        attrs,
    )


def _fake_metatable_fk_element(
    source_column,
    target_model,
    *,
    target_column="uid",
    ondelete="RESTRICT",
):
    target_columns = getattr(getattr(target_model, "__table__", None), "columns", [])
    target_column_object = next(
        (column for column in target_columns if column.name == target_column),
        FakeColumn(target_column, Uuid(), primary_key=True),
    )
    element = FakeForeignKeyElement(
        source_column,
        target_column_object,
        ondelete=ondelete,
    )
    element.info = {
        sqlalchemy_contracts._METATABLE_FOREIGN_KEY_INFO_KEY: {
            "target_model": target_model,
            "target_column": target_column,
        }
    }
    return element


def test_platform_managed_registration_request_from_sqlalchemy_metadata():
    table_name = metatable_tablename(namespace="example.assets", identifier="Account")
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
    assert request.storage_hash == table_name
    assert request.identifier == "Account"
    assert request.namespace == "example.assets"
    assert request.provisioning == {"create_table": True, "if_not_exists": True}
    assert request.table_contract.physical.table_name is None
    assert request.table_contract.columns[0].data_type == "uuid"
    assert request.table_contract.columns[0].backend_type == "UUID"
    assert request.table_contract.columns[0].primary_key is True
    assert request.table_contract.columns[0].server_default == "gen_random_uuid()"
    assert request.table_contract.columns[1].data_type == "string"
    assert request.table_contract.columns[1].backend_type == "VARCHAR(255)"
    assert request.table_contract.columns[1].description == "Display name"


def test_registration_request_uses_class_metatable_description():
    table_name = metatable_tablename(namespace="example.assets", identifier="Account")
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
    table_name = metatable_tablename(namespace="example.assets", identifier="Account")
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
    table_name = metatable_tablename(namespace="example.assets", identifier="Asset")
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
    table_name = metatable_tablename(namespace="example.assets", identifier="Observation")
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
    table_name = metatable_tablename(namespace="example.assets", identifier="Observation")
    table = FakeTable(
        table_name,
        columns=[FakeColumn("seen_at", DateTime(timezone=False), nullable=False)],
    )
    Observation = _model_class("Observation", table)

    with pytest.raises(ValueError, match="Timezone-naive"):
        table_contract_from_sqlalchemy_model(Observation)


def test_sqlalchemy_contract_includes_indexes_and_foreign_keys():
    account_table_name = metatable_tablename(namespace="example.assets", identifier="Account")
    account_uid = FakeColumn("uid", Uuid(), nullable=False, primary_key=True)
    account_table = FakeTable(account_table_name, columns=[account_uid])
    Account = _model_class("Account", account_table)

    asset_table_name = metatable_tablename(namespace="example.assets", identifier="Asset")
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

    contract = table_contract_from_sqlalchemy_model(
        Asset,
        target_meta_tables={
            Account: SimpleNamespace(uid="bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"),
        },
    )

    assert contract.physical.table_name == asset_table_name
    assert contract.indexes[0].name == "asset_account_uid_idx"
    assert contract.indexes[0].columns == ["account_uid"]
    assert contract.indexes[0].method == "btree"
    assert contract.foreign_keys[0].name == "asset_account_uid_fkey"
    assert contract.foreign_keys[0].source_columns == ["account_uid"]
    assert contract.foreign_keys[0].target_meta_table_uid == (
        "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"
    )
    assert contract.foreign_keys[0].target_columns == ["uid"]
    assert contract.foreign_keys[0].on_delete == "restrict"


def test_configured_metatable_tablename_changes_with_column_shape():
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

    assert metatable_configured_tablename(BaseAsset) != metatable_configured_tablename(
        ExtendedAsset
    )


def test_configured_metatable_tablename_ignores_logical_identifier():
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

    assert metatable_configured_tablename(Account) == metatable_configured_tablename(RenamedAccount)


def test_platform_managed_schema_resolves_from_sqlalchemy_table_args_only():
    class Account:
        __table_args__ = {"schema": "table_args_schema"}

    setattr(Account, "__metatable_" + "schema__", "ignored_schema")
    assert sqlalchemy_contracts._resolve_class_schema(Account) == "table_args_schema"


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


def test_platform_managed_accepts_configured_storage_hash_table_name():
    table = FakeTable(
        "placeholder",
        columns=[
            FakeColumn("uid", Uuid(), nullable=False, primary_key=True),
            FakeColumn("symbol", String(64), nullable=False),
        ],
    )
    Asset = _model_class("Asset", table)
    table.name = metatable_configured_tablename(Asset)

    request = platform_managed_registration_request_from_sqlalchemy_model(
        Asset,
        data_source_uid="dddddddd-dddd-4ddd-8ddd-dddddddddddd",
    )

    assert request.management_mode == "platform_managed"
    assert request.storage_hash == table.name
    assert request.table_contract.physical.table_name is None


def test_platform_managed_metatable_build_request_uses_session_data_source(monkeypatch):
    import mainsequence.client.models_metatables as models_metatables

    table = FakeTable(
        "placeholder",
        columns=[FakeColumn("uid", Uuid(), nullable=False, primary_key=True)],
    )
    Account = _platform_model_class("Account", table)
    table.name = metatable_configured_tablename(Account)

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
    assert request.storage_hash == table.name


def test_platform_managed_metatable_does_not_use_physical_data_source_uid(monkeypatch):
    import mainsequence.client.models_metatables as models_metatables

    table = FakeTable(
        "placeholder",
        columns=[FakeColumn("uid", Uuid(), nullable=False, primary_key=True)],
    )
    Account = _platform_model_class("Account", table)
    table.name = metatable_configured_tablename(Account)

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


def test_platform_managed_metatable_uses_bound_target_model_uid():
    account_table = FakeTable(
        "placeholder",
        columns=[FakeColumn("uid", Uuid(), nullable=False, primary_key=True)],
    )
    Account = _platform_model_class("Account", account_table)
    account_table.name = metatable_configured_tablename(Account)

    asset_uid = FakeColumn("uid", Uuid(), nullable=False, primary_key=True)
    account_uid_source = FakeColumn("account_uid", Uuid(), nullable=False)
    account_fk_element = FakeForeignKeyElement(
        account_uid_source,
        account_table.columns[0],
        ondelete="RESTRICT",
    )
    account_fk_element.info = {
        sqlalchemy_contracts._METATABLE_FOREIGN_KEY_INFO_KEY: {
            "target_model": Account,
            "target_column": "uid",
        }
    }
    asset_fk = FakeForeignKeyConstraint(
        "asset_account_uid_fkey",
        [account_fk_element],
    )
    asset_table = FakeTable(
        "placeholder",
        columns=[asset_uid, account_uid_source],
        foreign_keys=[asset_fk],
    )
    Asset = _platform_model_class("Asset", asset_table)
    asset_table.name = metatable_configured_tablename(Asset)
    Account._bind_meta_table(
        SimpleNamespace(
            uid="bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb",
            storage_hash=account_table.name,
            data_source_uid="dddddddd-dddd-4ddd-8ddd-dddddddddddd",
        )
    )

    request = Asset.build_registration_request(
        data_source_uid="dddddddd-dddd-4ddd-8ddd-dddddddddddd",
    )

    assert request.table_contract.foreign_keys[0].target_meta_table_uid == (
        "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"
    )


def test_platform_managed_metatable_registers_fk_targets_recursively(monkeypatch):
    account_table = FakeTable(
        "placeholder",
        columns=[FakeColumn("uid", Uuid(), nullable=False, primary_key=True)],
    )
    Account = _platform_model_class("Account", account_table)
    account_table.name = metatable_configured_tablename(Account)

    asset_uid = FakeColumn("uid", Uuid(), nullable=False, primary_key=True)
    account_uid_source = FakeColumn("account_uid", Uuid(), nullable=False)
    account_fk_element = FakeForeignKeyElement(
        account_uid_source,
        account_table.columns[0],
        ondelete="RESTRICT",
    )
    account_fk_element.info = {
        sqlalchemy_contracts._METATABLE_FOREIGN_KEY_INFO_KEY: {
            "target_model": Account,
            "target_column": "uid",
        }
    }
    asset_fk = FakeForeignKeyConstraint(
        "asset_account_uid_fkey",
        [account_fk_element],
    )
    asset_table = FakeTable(
        "placeholder",
        columns=[asset_uid, account_uid_source],
        foreign_keys=[asset_fk],
    )
    Asset = _platform_model_class("Asset", asset_table)
    asset_table.name = metatable_configured_tablename(Asset)

    captured_requests = []

    def fake_register(cls, request, timeout=None):
        captured_requests.append(request)
        uid = (
            "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"
            if request.identifier == "Account"
            else "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
        )
        return SimpleNamespace(
            uid=uid,
            data_source_uid=request.data_source_uid,
            storage_hash=request.storage_hash,
        )

    monkeypatch.setattr(
        sqlalchemy_contracts.MetaTable,
        "register",
        classmethod(fake_register),
    )

    Asset.__metatable_identifier__ = "ChildOverride"
    Asset.__metatable_description__ = "Child-specific description."
    Asset.__metatable_labels__ = ["child"]

    Asset.register()

    assert [request.identifier for request in captured_requests] == ["Account", "ChildOverride"]
    assert captured_requests[0].description is None
    assert captured_requests[0].labels == []
    assert captured_requests[1].identifier == "ChildOverride"
    assert captured_requests[1].description == "Child-specific description."
    assert captured_requests[1].labels == ["child"]
    assert captured_requests[1].table_contract.foreign_keys[0].target_meta_table_uid == (
        "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"
    )


def test_platform_managed_register_reuses_local_registry(monkeypatch):
    table = FakeTable(
        "placeholder",
        columns=[FakeColumn("uid", Uuid(), nullable=False, primary_key=True)],
    )
    Account = _platform_model_class("Account", table)
    table.name = metatable_configured_tablename(Account)
    captured_requests = []

    def fake_register(cls, request, timeout=None):
        captured_requests.append(request)
        return SimpleNamespace(
            uid="aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
            data_source_uid=request.data_source_uid,
            storage_hash=request.storage_hash,
        )

    monkeypatch.setattr(
        sqlalchemy_contracts.MetaTable,
        "register",
        classmethod(fake_register),
    )

    first = Account.register()
    second = Account.register()

    assert first is second
    assert len(captured_requests) == 1


def test_platform_managed_register_clears_failed_registry_entry(monkeypatch):
    table = FakeTable(
        "placeholder",
        columns=[FakeColumn("uid", Uuid(), nullable=False, primary_key=True)],
    )
    Account = _platform_model_class("Account", table)
    table.name = metatable_configured_tablename(Account)

    def fake_register(cls, request, timeout=None):
        raise RuntimeError("registration failed")

    monkeypatch.setattr(
        sqlalchemy_contracts.MetaTable,
        "register",
        classmethod(fake_register),
    )

    with pytest.raises(RuntimeError, match="registration failed"):
        Account.register()

    assert table.name not in sqlalchemy_contracts._METATABLE_REGISTRATION_REGISTRY


def test_platform_managed_register_detects_recursive_registration_cycle():
    account_uid = FakeColumn("uid", Uuid(), nullable=False, primary_key=True)
    account_owner_uid = FakeColumn("owner_uid", Uuid(), nullable=False)
    account_table = FakeTable("placeholder", columns=[account_uid, account_owner_uid])
    Account = _platform_model_class("Account", account_table)
    account_table.name = metatable_configured_tablename(Account)

    owner_uid = FakeColumn("uid", Uuid(), nullable=False, primary_key=True)
    owner_account_uid = FakeColumn("account_uid", Uuid(), nullable=False)
    owner_table = FakeTable("placeholder", columns=[owner_uid, owner_account_uid])
    Owner = _platform_model_class("Owner", owner_table)
    owner_table.name = metatable_configured_tablename(Owner)

    account_table.foreign_key_constraints = [
        FakeForeignKeyConstraint(
            "account_owner_uid_fkey",
            [_fake_metatable_fk_element(account_owner_uid, Owner)],
        )
    ]
    owner_table.foreign_key_constraints = [
        FakeForeignKeyConstraint(
            "owner_account_uid_fkey",
            [_fake_metatable_fk_element(owner_account_uid, Account)],
        )
    ]

    with pytest.raises(ValueError, match="recursive registration cycle"):
        Account.register()


def test_platform_managed_omits_name_for_metatable_foreign_key():
    account_table = FakeTable(
        "placeholder",
        columns=[FakeColumn("uid", Uuid(), nullable=False, primary_key=True)],
    )
    Account = _platform_model_class("Account", account_table)
    account_table.name = metatable_configured_tablename(Account)
    Account._bind_meta_table(
        SimpleNamespace(
            uid="bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb",
            storage_hash=account_table.name,
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
                [_fake_metatable_fk_element(account_uid_source, Account)],
            )
        ],
    )
    Asset = _platform_model_class("Asset", asset_table)

    request = Asset.build_registration_request(
        data_source_uid="dddddddd-dddd-4ddd-8ddd-dddddddddddd",
        enforce_storage_hash_name=False,
    )

    assert request.table_contract.foreign_keys[0].name is None
    assert "name" not in request.table_contract.foreign_keys[0].model_dump(
        mode="json",
        exclude_none=True,
    )


def test_platform_managed_metatable_requires_metatable_foreign_key():
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
        "placeholder",
        columns=[asset_uid, account_uid_source],
        foreign_keys=[asset_fk],
    )
    Asset = _platform_model_class("Asset", asset_table)
    asset_table.name = metatable_configured_tablename(Asset)

    with pytest.raises(ValueError, match="must use MetaTableForeignKey"):
        Asset.build_registration_request(
            data_source_uid="dddddddd-dddd-4ddd-8ddd-dddddddddddd",
        )


def test_platform_managed_metatable_register_delegates_to_meta_table_register(monkeypatch):
    table = FakeTable(
        "placeholder",
        columns=[FakeColumn("uid", Uuid(), nullable=False, primary_key=True)],
    )
    Account = _platform_model_class("Account", table)
    table.name = metatable_configured_tablename(Account)

    captured = {}

    def fake_register(cls, request, timeout=None):
        captured["request"] = request
        captured["timeout"] = timeout
        return SimpleNamespace(
            uid="aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
            data_source_uid="dddddddd-dddd-4ddd-8ddd-dddddddddddd",
            storage_hash=table.name,
            physical_table_name="mt_aaaaaaaaaaaa4aaa8aaaaaaaaaaaaaaa",
        )

    monkeypatch.setattr(
        sqlalchemy_contracts.MetaTable,
        "register",
        classmethod(fake_register),
    )

    registered = Account.register(timeout=15)

    assert registered.uid == "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
    assert Account.get_meta_table() is registered
    assert Account.get_meta_table_uid() == "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
    assert Account.get_data_source_uid() == "dddddddd-dddd-4ddd-8ddd-dddddddddddd"
    assert Account.get_storage_hash() == captured["request"].storage_hash
    assert Account.get_physical_table_name() == "mt_aaaaaaaaaaaa4aaa8aaaaaaaaaaaaaaa"
    assert Account.__table__.name == "mt_aaaaaaaaaaaa4aaa8aaaaaaaaaaaaaaa"
    assert captured["timeout"] == 15
    assert captured["request"].storage_hash != Account.__table__.name
    assert captured["request"].table_contract.physical.table_name is None
    assert not hasattr(Account, "bind_meta_table")


def test_platform_managed_free_function_register_path_is_not_public():
    import mainsequence.meta_tables as meta_tables

    assert not hasattr(sqlalchemy_contracts, "register_platform_managed_sqlalchemy_model")
    assert "register_platform_managed_sqlalchemy_model" not in meta_tables.__all__
    missing_name = "register_platform_managed_sqlalchemy_model"
    with pytest.raises(AttributeError):
        getattr(meta_tables, missing_name)


def test_time_index_metadata_registration_request_uses_dynamic_contract():
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
    table.name = metatable_configured_tablename(AccountHoldings)

    request = AccountHoldings.build_registration_request(
        data_source_uid="dddddddd-dddd-4ddd-8ddd-dddddddddddd",
        description="Account holdings data node storage",
    )

    assert isinstance(request, TimeIndexMetaTableRegistrationRequest)
    assert request.storage_hash == table.name
    assert request.identifier == "AccountHoldings"
    assert request.namespace == "example.assets"
    assert request.description == "Account holdings data node storage"
    assert request.time_index_name == "time_index"
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


def test_time_index_metadata_registration_request_uses_class_metatable_description():
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
    table.name = metatable_configured_tablename(AccountHoldings)

    request = AccountHoldings.build_registration_request(
        data_source_uid="dddddddd-dddd-4ddd-8ddd-dddddddddddd",
    )

    assert request.description == ("Account holdings history used to reconstruct portfolio state.")


def test_time_index_metadata_configured_tablename_changes_with_index_grain():
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

    assert metatable_configured_tablename(OneIndex) != metatable_configured_tablename(ThreeIndex)


def test_time_index_metadata_rejects_first_index_not_time_index():
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
        metatable_configured_tablename(BadHoldings)


def test_time_index_metadata_rejects_nullable_index_columns():
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
        metatable_configured_tablename(BadHoldings)


def test_time_index_metadata_register_posts_to_dynamic_table_endpoint(monkeypatch):
    import mainsequence.client.models_metatables as models_metatables

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
    table.name = metatable_configured_tablename(AccountHoldings)

    captured = {}

    class Response:
        status_code = 201
        content = b"{}"

        def json(self):
            return {
                "uid": "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
                "data_source_uid": "dddddddd-dddd-4ddd-8ddd-dddddddddddd",
                "storage_hash": table.name,
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

    registered = AccountHoldings.register(timeout=15)

    assert registered.uid == "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
    assert AccountHoldings.get_meta_table() is registered
    assert AccountHoldings.get_time_index_metadata() is registered
    assert AccountHoldings.get_meta_table_uid() == "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
    assert AccountHoldings.get_storage_hash() == captured["payload"]["json"]["storage_hash"]
    assert AccountHoldings.get_physical_table_name() == "mt_aaaaaaaaaaaa4aaa8aaaaaaaaaaaaaaa"
    assert AccountHoldings.__table__.name == "mt_aaaaaaaaaaaa4aaa8aaaaaaaaaaaaaaa"
    assert captured["r_type"] == "POST"
    assert captured["url"].endswith("/ts_manager/dynamic_table/register/")
    assert captured["timeout"] == 15
    assert captured["payload"]["json"]["time_index_name"] == "time_index"
    assert "index_names" not in captured["payload"]["json"]
    assert captured["payload"]["json"]["table_contract"]["authoring"]["time_indexed"][
        "index_names"
    ] == [
        "time_index",
        "account_uid",
        "unique_identifier",
    ]


def test_ensure_registered_storage_table_registers_unbound_storage(monkeypatch):
    import mainsequence.client.models_metatables as models_metatables

    columns = [
        FakeColumn("time_index", DateTime(timezone=True), nullable=False),
        FakeColumn("asset_uid", Uuid(), nullable=False),
    ]
    table = FakeTable("placeholder", columns=columns)
    AssetSnapshots = _time_index_model_class(
        "AssetSnapshots",
        table,
        index_names=["time_index", "asset_uid"],
    )
    table.name = metatable_configured_tablename(AssetSnapshots)
    captured = {}

    def fake_register(cls, request, timeout=None):
        captured["request"] = request
        return SimpleNamespace(
            uid="aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
            data_source_uid=request.data_source_uid,
            storage_hash=request.storage_hash,
        )

    monkeypatch.setattr(
        models_metatables.TimeIndexMetaData,
        "register",
        classmethod(fake_register),
    )

    ensured = ensure_registered_storage_table(AssetSnapshots, context="DataNode")

    assert ensured is AssetSnapshots
    assert AssetSnapshots.get_time_index_metadata().uid == "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
    assert AssetSnapshots.get_meta_table_uid() == "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
    assert AssetSnapshots.get_data_source_uid() == "dddddddd-dddd-4ddd-8ddd-dddddddddddd"
    assert captured["request"].identifier == "AssetSnapshots"


def test_time_index_storage_name_hash_component_separates_identical_table_shapes():
    columns_a = [
        FakeColumn("time_index", DateTime(timezone=True), nullable=False),
        FakeColumn("random_number", String(255), nullable=False),
    ]
    columns_b = [
        FakeColumn("time_index", DateTime(timezone=True), nullable=False),
        FakeColumn("random_number", String(255), nullable=False),
    ]
    table_a = FakeTable("placeholder_a", columns=columns_a)
    table_b = FakeTable("placeholder_b", columns=columns_b)
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
    RandomNumber.__metatable_extra_hash_components__ = {"storage_name": "daily_random_number"}
    RandomAddition.__metatable_extra_hash_components__ = {"storage_name": "daily_random_addition"}

    table_a.name = metatable_configured_tablename(RandomNumber)
    table_b.name = metatable_configured_tablename(RandomAddition)

    assert table_a.name != table_b.name

    request = time_indexed_registration_request_from_sqlalchemy_model(
        RandomNumber,
        data_source_uid="dddddddd-dddd-4ddd-8ddd-dddddddddddd",
    )
    assert request.storage_hash == table_a.name
    assert request.identifier == "daily_random_number_project"
    assert request.namespace == "mainsequence.examples"


def test_platform_managed_metatable_matches_configured_tablename_with_sqlalchemy():
    pytest.importorskip("sqlalchemy")


    from sqlalchemy import Index, MetaData, String, Uuid
    from sqlalchemy.orm import DeclarativeBase, mapped_column

    naming_convention = {
        "ix": "%(table_name)s_%(column_0_name)s_idx",
        "fk": "%(table_name)s_%(column_0_name)s_fkey",
        "pk": "%(table_name)s_pkey",
    }

    class Base(DeclarativeBase):
        metadata = MetaData(naming_convention=naming_convention)

    class Account(PlatformManagedMetaTable, Base):
        __table_args__ = {"schema": "public"}

        __metatable_namespace__ = "example.assets"
        __metatable_identifier__ = "Account"

        uid: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True)
        name: Mapped[str] = mapped_column(String(255), nullable=False)

    class Asset(PlatformManagedMetaTable, Base):
        __table_args__ = (
            Index(None, "account_uid"),
            {"schema": "public"},
        )

        __metatable_namespace__ = "example.assets"
        __metatable_identifier__ = "Asset"

        uid: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True)
        account_uid: Mapped[uuid.UUID] = mapped_column(
            Uuid,
            MetaTableForeignKey(Account, column="uid", ondelete="RESTRICT"),
            nullable=False,
        )
        symbol: Mapped[str] = mapped_column(String(64), nullable=False)

    assert Account.__table__.name == metatable_configured_tablename(Account)
    assert Asset.__table__.name == metatable_configured_tablename(Asset)
    Account._bind_meta_table(
        SimpleNamespace(
            uid="bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb",
            data_source_uid="dddddddd-dddd-4ddd-8ddd-dddddddddddd",
            storage_hash=Account.__table__.name,
        )
    )

    request = platform_managed_registration_request_from_sqlalchemy_model(
        Asset,
        data_source_uid="dddddddd-dddd-4ddd-8ddd-dddddddddddd",
    )

    assert request.storage_hash == Asset.__table__.name
    assert request.table_contract.physical.table_name is None
    assert request.table_contract.indexes[0].name
    assert request.table_contract.foreign_keys[0].name is None


def test_metatable_foreign_key_rejects_non_platform_target_before_sqlalchemy_import():
    with pytest.raises(ValueError, match="PlatformManagedMetaTable authoring model class"):
        sqlalchemy_contracts.MetaTableForeignKey(object, column="uid")


def test_metatable_foreign_key_validates_target_column_before_sqlalchemy_import():
    id_column = FakeColumn("id", Uuid(), primary_key=True)
    account_table = FakeTable("account", columns=[id_column])
    account_table.c = {"id": id_column}
    Account = _platform_model_class("Account", account_table)

    with pytest.raises(ValueError, match="has no column 'uid'"):
        sqlalchemy_contracts.MetaTableForeignKey(Account, column="uid")


def test_metatable_foreign_key_rejects_names_before_sqlalchemy_import():
    uid_column = FakeColumn("uid", Uuid(), primary_key=True)
    account_table = FakeTable("account", columns=[uid_column])
    account_table.c = {"uid": uid_column}
    Account = _platform_model_class("Account", account_table)

    with pytest.raises(ValueError, match="does not accept foreign-key names"):
        sqlalchemy_contracts.MetaTableForeignKey(
            Account,
            column="uid",
            name="asset_account_uid_fkey",
        )


def test_metatable_foreign_key_metadata_drives_fk_contract_target_resolution():
    account_uid = FakeColumn("uid", Uuid(), primary_key=True)
    account_table = FakeTable("account", columns=[account_uid])
    Account = _platform_model_class("Account", account_table)

    asset_account_uid = FakeColumn("account_uid", Uuid(), nullable=False)
    physical_target_uid = FakeColumn("physical_uid", Uuid(), primary_key=True)
    FakeTable("physical_account", columns=[physical_target_uid])
    element = FakeForeignKeyElement(
        asset_account_uid,
        physical_target_uid,
        ondelete="RESTRICT",
    )
    element.info = {
        sqlalchemy_contracts._METATABLE_FOREIGN_KEY_INFO_KEY: {
            "target_model": Account,
            "target_column": "uid",
            "name": "asset_account_uid_fkey",
        }
    }
    asset_table = FakeTable(
        "asset",
        columns=[asset_account_uid],
        foreign_keys=[FakeForeignKeyConstraint("asset_account_uid_fkey", [element])],
    )
    Asset = _model_class("Asset", asset_table)

    contract = table_contract_from_sqlalchemy_model(
        Asset,
        target_meta_tables={
            Account: SimpleNamespace(uid="bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"),
        },
    )

    assert contract.foreign_keys[0].target_meta_table_uid == (
        "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"
    )
    assert contract.foreign_keys[0].target_columns == ["uid"]


def test_platform_managed_register_rebinds_sqlalchemy_table_to_backend_physical_name(
    monkeypatch,
):
    pytest.importorskip("sqlalchemy")


    from sqlalchemy import MetaData, String, Uuid, select
    from sqlalchemy.dialects import postgresql
    from sqlalchemy.orm import DeclarativeBase, mapped_column

    class Base(DeclarativeBase):
        metadata = MetaData()

    class Account(PlatformManagedMetaTable, Base):
        __metatable_namespace__ = "example.assets"
        __metatable_identifier__ = "Account"

        uid: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True)
        name: Mapped[str] = mapped_column(String(255), nullable=False)

    storage_hash = Account.__table__.name
    physical_table_name = "mt_aaaaaaaaaaaa4aaa8aaaaaaaaaaaaaaa"
    captured = {}

    def fake_register(cls, request, timeout=None):
        captured["request"] = request
        return SimpleNamespace(
            uid="aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
            data_source_uid="dddddddd-dddd-4ddd-8ddd-dddddddddddd",
            storage_hash=request.storage_hash,
            physical_table_name=physical_table_name,
        )

    monkeypatch.setattr(
        sqlalchemy_contracts.MetaTable,
        "register",
        classmethod(fake_register),
    )

    Account.register()

    assert captured["request"].storage_hash == storage_hash
    assert captured["request"].table_contract.physical.table_name is None
    assert Account.get_storage_hash() == storage_hash
    assert Account.get_physical_table_name() == physical_table_name
    assert Account.__table__.name == physical_table_name
    assert Account.__table__.fullname == f"public.{physical_table_name}"
    assert Base.metadata.tables[f"public.{physical_table_name}"] is Account.__table__
    assert f"public.{storage_hash}" not in Base.metadata.tables

    compiled_sql = str(
        select(Account.__table__).compile(dialect=postgresql.dialect(paramstyle="pyformat"))
    )
    assert f"FROM public.{physical_table_name}" in compiled_sql
    assert storage_hash not in compiled_sql


def test_bound_parent_table_fullname_resolves_fk_and_contract_uses_logical_target():
    pytest.importorskip("sqlalchemy")


    from sqlalchemy import MetaData, String, Uuid
    from sqlalchemy.orm import DeclarativeBase, mapped_column

    class Base(DeclarativeBase):
        metadata = MetaData()

    class Account(PlatformManagedMetaTable, Base):
        __metatable_namespace__ = "example.assets"
        __metatable_identifier__ = "Account"

        uid: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True)

    account_storage_hash = Account.__table__.name
    account_physical_table_name = "mt_aaaaaaaaaaaa4aaa8aaaaaaaaaaaaaaa"
    Account._bind_meta_table(
        SimpleNamespace(
            uid="aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
            data_source_uid="dddddddd-dddd-4ddd-8ddd-dddddddddddd",
            storage_hash=account_storage_hash,
            physical_table_name=account_physical_table_name,
        )
    )

    assert Account.__table__.fullname == f"public.{account_physical_table_name}"

    class Asset(PlatformManagedMetaTable, Base):
        __metatable_namespace__ = "example.assets"
        __metatable_identifier__ = "Asset"

        uid: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True)
        account_uid: Mapped[uuid.UUID] = mapped_column(
            Uuid,
            MetaTableForeignKey(
                Account,
                column="uid",
                ondelete="RESTRICT",
            ),
            nullable=False,
        )
        symbol: Mapped[str] = mapped_column(String(64), nullable=False)

    foreign_key = next(iter(Asset.__table__.foreign_keys))
    assert (
        foreign_key.info[sqlalchemy_contracts._METATABLE_FOREIGN_KEY_INFO_KEY]["target_model"]
        is Account
    )
    assert Asset.__table__.name == metatable_configured_tablename(Asset)

    request = Asset.build_registration_request(
        data_source_uid="dddddddd-dddd-4ddd-8ddd-dddddddddddd",
    )

    assert request.table_contract.foreign_keys[0].target_meta_table_uid == (
        "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
    )
    assert request.table_contract.foreign_keys[0].name is None


def test_time_index_metadata_matches_configured_tablename_with_sqlalchemy():
    pytest.importorskip("sqlalchemy")


    from sqlalchemy import DateTime, Index, MetaData, String, Uuid
    from sqlalchemy.orm import DeclarativeBase, mapped_column

    class Base(DeclarativeBase):
        metadata = MetaData()

    class Account(PlatformManagedMetaTable, Base):
        __metatable_namespace__ = "example.assets"
        __metatable_identifier__ = "Account"

        uid: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True)
        account_code: Mapped[str] = mapped_column(String(64), nullable=False, unique=True)
        name: Mapped[str] = mapped_column(String(255), nullable=False)

    class AccountHoldings(PlatformTimeIndexMetaData, Base):
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
            MetaTableForeignKey(Account, column="uid", ondelete="RESTRICT"),
            nullable=False,
        )
        unique_identifier: Mapped[str] = mapped_column(String(255), nullable=False)

    assert AccountHoldings.__table__.name == metatable_configured_tablename(AccountHoldings)
    assert list(AccountHoldings.__table__.primary_key.columns.keys()) == []
    assert [column.name for column in AccountHoldings.__mapper__.primary_key] == [
        "time_index",
        "account_uid",
        "unique_identifier",
    ]
    Account._bind_meta_table(
        SimpleNamespace(
            uid="bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb",
            data_source_uid="dddddddd-dddd-4ddd-8ddd-dddddddddddd",
            storage_hash=Account.__table__.name,
        )
    )

    request = AccountHoldings.build_registration_request(
        data_source_uid="dddddddd-dddd-4ddd-8ddd-dddddddddddd",
    )

    assert request.storage_hash == AccountHoldings.__table__.name
    assert request.time_index_name == "time_index"
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
    assert len(request.table_contract["foreign_keys"]) == 1
    foreign_key = request.table_contract["foreign_keys"][0]
    assert foreign_key["source_columns"] == ["account_uid"]
    assert foreign_key["target_meta_table_uid"] == "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"
    assert foreign_key["target_columns"] == ["uid"]
    assert foreign_key["on_delete"] == "restrict"


def test_platform_managed_requires_storage_hash_table_name():
    table = FakeTable(
        "asset",
        columns=[FakeColumn("uid", Uuid(), nullable=False, primary_key=True)],
    )
    Asset = _model_class("Asset", table)

    with pytest.raises(ValueError, match="metatable_tablename"):
        platform_managed_registration_request_from_sqlalchemy_model(
            Asset,
            data_source_uid="dddddddd-dddd-4ddd-8ddd-dddddddddddd",
        )


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
    assert request.storage_hash != "asset"
    assert request.table_contract.physical.table_name == "asset"
    assert request.introspect is True
