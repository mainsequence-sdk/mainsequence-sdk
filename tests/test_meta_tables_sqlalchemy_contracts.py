from __future__ import annotations

import pytest

from mainsequence.client.models_metatables import MetaTableRegistrationRequest
from mainsequence.tdag.meta_tables import (
    external_registered_registration_request_from_sqlalchemy_model,
    metatable_tablename,
    platform_managed_registration_request_from_sqlalchemy_model,
    table_contract_from_sqlalchemy_model,
)


class Uuid:
    def __str__(self):
        return "UUID"


class String:
    def __init__(self, length: int):
        self.length = length

    def __str__(self):
        return f"VARCHAR({self.length})"


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


def _model_class(name, table, *, namespace="example.assets", identifier=None):
    return type(
        name,
        (),
        {
            "__module__": "tests.client_tables",
            "__metatable_namespace__": namespace,
            "__metatable_identifier__": identifier or name,
            "__table__": table,
        },
    )


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
    assert request.table_contract.physical.table_name == table_name
    assert request.table_contract.columns[0].data_type == "uuid"
    assert request.table_contract.columns[0].primary_key is True
    assert request.table_contract.columns[1].data_type == "str"
    assert request.table_contract.columns[1].backend_type == "VARCHAR(255)"
    assert request.table_contract.columns[1].description == "Display name"


def test_sqlalchemy_contract_includes_indexes_and_foreign_keys():
    account_table_name = metatable_tablename(namespace="example.assets", identifier="Account")
    account_uid = FakeColumn("uid", Uuid(), nullable=False, primary_key=True)
    account_table = FakeTable(account_table_name, columns=[account_uid])

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
        target_meta_table_uid_by_fullname={
            account_table.fullname: "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb",
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
