from __future__ import annotations

import uuid

from sqlalchemy import ForeignKey, Index, MetaData, String, Uuid
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from examples.meta_tables.common import (
    DEFAULT_SCHEMA,
    print_json,
)
from mainsequence.meta_tables import (
    PlatformManagedMetaTable,
    schema_table_name,
    sqlalchemy_naming_convention,
)

NAMESPACE = "sdk-examples"
PROJECT_NAME = "sdk_examples"
ACCOUNT_TABLE_NAME = schema_table_name(PROJECT_NAME, "account")
ASSET_TABLE_NAME = schema_table_name(PROJECT_NAME, "asset")


class Base(DeclarativeBase):
    metadata = MetaData(naming_convention=sqlalchemy_naming_convention())


class Account(PlatformManagedMetaTable, Base):
    __tablename__ = ACCOUNT_TABLE_NAME
    __table_args__ = {"schema": DEFAULT_SCHEMA}

    __metatable_namespace__ = NAMESPACE
    __metatable_identifier__ = f"{PROJECT_NAME}.Account"

    uid: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True)
    name: Mapped[str] = mapped_column(String(255), nullable=False)


class Asset(PlatformManagedMetaTable, Base):
    __tablename__ = ASSET_TABLE_NAME
    __table_args__ = (
        Index(None, "account_uid"),
        {"schema": DEFAULT_SCHEMA},
    )

    __metatable_namespace__ = NAMESPACE
    __metatable_identifier__ = f"{PROJECT_NAME}.Asset"

    uid: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True)
    account_uid: Mapped[uuid.UUID] = mapped_column(
        Uuid,
        ForeignKey(f"{DEFAULT_SCHEMA}.{ACCOUNT_TABLE_NAME}.uid", ondelete="RESTRICT"),
        nullable=False,
    )
    symbol: Mapped[str] = mapped_column(String(64), nullable=False)


def main() -> None:
    print_json(
        "Migration-managed platform MetaTables",
        {
            "provider": "examples.meta_tables.migrations:migration",
            "models": [
                Account.__metatable_identifier__,
                Asset.__metatable_identifier__,
            ],
            "initial_apply": [
                "mainsequence migrations current --provider examples.meta_tables.migrations:migration",
                "mainsequence migrations upgrade --provider examples.meta_tables.migrations:migration head",
            ],
            "after_model_change": [
                "mainsequence migrations revision --provider examples.meta_tables.migrations:migration -m 'describe schema change'",
                "mainsequence migrations upgrade --provider examples.meta_tables.migrations:migration head",
            ],
        },
    )


if __name__ == "__main__":
    main()
