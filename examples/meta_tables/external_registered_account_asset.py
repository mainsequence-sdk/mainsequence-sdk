from __future__ import annotations

import uuid

from sqlalchemy import ForeignKey, Index, String, Uuid, create_engine
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from examples.meta_tables.common import (
    DEFAULT_NAMESPACE,
    DEFAULT_SCHEMA,
    DEFAULT_TIMEOUT,
    PLACEHOLDER_ACCOUNT_META_TABLE_UID,
    env_flag,
    optional_env,
    print_json,
    required_env,
)
from mainsequence.client import MetaTable
from mainsequence.tdag.meta_tables import (
    external_registered_registration_request_from_sqlalchemy_model,
)

NAMESPACE = optional_env(
    "MAINSEQUENCE_META_TABLE_NAMESPACE",
    f"{DEFAULT_NAMESPACE}.external_registered",
)


class Base(DeclarativeBase):
    pass


class Account(Base):
    __tablename__ = optional_env("MAINSEQUENCE_META_TABLE_ACCOUNT_TABLE", "account")
    __table_args__ = {"schema": DEFAULT_SCHEMA}

    __metatable_namespace__ = NAMESPACE
    __metatable_identifier__ = "Account"

    uid: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True)
    name: Mapped[str] = mapped_column(String(255), nullable=False)


class Asset(Base):
    __tablename__ = optional_env("MAINSEQUENCE_META_TABLE_ASSET_TABLE", "asset")
    __table_args__ = (
        Index("asset_account_uid_idx", "account_uid"),
        {"schema": DEFAULT_SCHEMA},
    )

    __metatable_namespace__ = NAMESPACE
    __metatable_identifier__ = "Asset"

    uid: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True)
    account_uid: Mapped[uuid.UUID] = mapped_column(
        Uuid,
        ForeignKey(
            f"{Account.__table__.fullname}.uid",
            name="asset_account_uid_fkey",
            ondelete="RESTRICT",
        ),
        nullable=False,
    )
    symbol: Mapped[str] = mapped_column(String(64), nullable=False)


def create_physical_tables_for_demo(*, database_url: str) -> None:
    """Optional direct-DB demo step for external_registered mode.

    In a real application this is usually Alembic or your existing migration
    workflow. TS Manager is not creating these tables in external_registered
    mode.
    """

    engine = create_engine(database_url)
    Base.metadata.create_all(engine)


def build_account_request(*, data_source_uid: str):
    return external_registered_registration_request_from_sqlalchemy_model(
        Account,
        data_source_uid=data_source_uid,
        description="Example externally managed account table.",
        labels=["sdk-example", "meta-table", "external-registered"],
    )


def build_asset_request(*, data_source_uid: str, account_meta_table_uid: str):
    return external_registered_registration_request_from_sqlalchemy_model(
        Asset,
        data_source_uid=data_source_uid,
        description="Example externally managed asset table.",
        labels=["sdk-example", "meta-table", "external-registered"],
        target_meta_table_uid_by_fullname={
            Account.__table__.fullname: account_meta_table_uid,
        },
    )


def register_account_and_asset(*, data_source_uid: str) -> tuple[MetaTable, MetaTable]:
    account_request = build_account_request(data_source_uid=data_source_uid)
    account_meta_table = MetaTable.register(account_request, timeout=DEFAULT_TIMEOUT)

    asset_request = build_asset_request(
        data_source_uid=data_source_uid,
        account_meta_table_uid=account_meta_table.uid,
    )
    asset_meta_table = MetaTable.register(asset_request, timeout=DEFAULT_TIMEOUT)
    return account_meta_table, asset_meta_table


def main() -> None:
    data_source_uid = required_env("MAINSEQUENCE_META_TABLE_DATA_SOURCE_UID")
    should_create_tables = env_flag("MAINSEQUENCE_META_TABLE_CREATE_EXTERNAL_TABLES")
    should_register = env_flag("MAINSEQUENCE_META_TABLE_REGISTER", default=False)

    if should_create_tables:
        database_url = required_env("MAINSEQUENCE_META_TABLE_EXTERNAL_DATABASE_URL")
        create_physical_tables_for_demo(database_url=database_url)
        print("\nCreated physical Account and Asset tables with SQLAlchemy.")

    account_request = build_account_request(data_source_uid=data_source_uid)
    print_json("Account registration request", account_request)

    if should_register:
        account_meta_table, asset_meta_table = register_account_and_asset(
            data_source_uid=data_source_uid,
        )
        print_json("Registered Account MetaTable", account_meta_table)
        print_json("Registered Asset MetaTable", asset_meta_table)
        return

    asset_request = build_asset_request(
        data_source_uid=data_source_uid,
        account_meta_table_uid=PLACEHOLDER_ACCOUNT_META_TABLE_UID,
    )
    print_json("Asset registration request with placeholder FK target", asset_request)
    print(
        "\nDry run only. Set MAINSEQUENCE_META_TABLE_REGISTER=1 after your own "
        "migration workflow has created the physical tables."
    )


if __name__ == "__main__":
    main()
