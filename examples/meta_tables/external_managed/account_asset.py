from __future__ import annotations

import os
import uuid

from sqlalchemy import ForeignKey, Index, MetaData, String, Uuid, create_engine
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from examples.meta_tables.common import (
    DEFAULT_SCHEMA,
    DEFAULT_TIMEOUT,
    print_json,
)
from mainsequence.client import MetaTable
from mainsequence.meta_tables import (
    external_registered_registration_request_from_sqlalchemy_model,
    schema_table_name,
    sqlalchemy_naming_convention,
)

NAMESPACE = "sdk-examples"
PROJECT_NAME = "sdk_examples"
ACCOUNT_TABLE_NAME = schema_table_name(PROJECT_NAME, "account")
ASSET_TABLE_NAME = schema_table_name(PROJECT_NAME, "asset")


class Base(DeclarativeBase):
    metadata = MetaData(naming_convention=sqlalchemy_naming_convention())


class Account(Base):
    __tablename__ = ACCOUNT_TABLE_NAME

    __metatable_namespace__ = NAMESPACE
    __metatable_identifier__ = f"{PROJECT_NAME}.Account"

    uid: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True)
    name: Mapped[str] = mapped_column(String(255), nullable=False)


class Asset(Base):
    __tablename__ = ASSET_TABLE_NAME
    __table_args__ = (Index(None, "account_uid"),)

    __metatable_namespace__ = NAMESPACE
    __metatable_identifier__ = f"{PROJECT_NAME}.Asset"

    uid: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True)
    account_uid: Mapped[uuid.UUID] = mapped_column(
        Uuid,
        ForeignKey(
            f"{Account.__table__.fullname}.uid",
            ondelete="RESTRICT",
        ),
        nullable=False,
    )
    symbol: Mapped[str] = mapped_column(String(64), nullable=False)


def required_environment_value(name: str) -> str:
    value = (os.environ.get(name) or "").strip()
    if not value:
        raise ValueError(f"{name} is required.")
    return value


def create_physical_tables_for_demo(sqlalchemy_url: str) -> None:
    """Create the externally managed tables before registration."""

    engine = create_engine(sqlalchemy_url)
    try:
        Base.metadata.create_all(engine)
    finally:
        engine.dispose()


def main() -> None:
    data_source_uid = required_environment_value("MAINSEQUENCE_META_TABLE_DATA_SOURCE_UID")
    sqlalchemy_url = required_environment_value("MAINSEQUENCE_META_TABLE_SQLALCHEMY_URL")
    create_physical_tables_for_demo(sqlalchemy_url)
    print("\nCreated physical Account and Asset tables in the external database.")

    account_request = external_registered_registration_request_from_sqlalchemy_model(
        Account,
        data_source_uid=data_source_uid,
        schema=DEFAULT_SCHEMA,
        introspect=False,
        description="Example externally managed account table.",
        labels=["sdk-example", "external-managed"],
    )
    account_meta_table = MetaTable.register(account_request, timeout=DEFAULT_TIMEOUT)

    asset_request = external_registered_registration_request_from_sqlalchemy_model(
        Asset,
        data_source_uid=data_source_uid,
        schema=DEFAULT_SCHEMA,
        introspect=False,
        description="Example externally managed asset table.",
        labels=["sdk-example", "external-managed"],
    )
    asset_meta_table = MetaTable.register(asset_request, timeout=DEFAULT_TIMEOUT)
    print_json("Registered Account MetaTable", account_meta_table)
    print_json("Registered Asset MetaTable", asset_meta_table)


if __name__ == "__main__":
    main()
