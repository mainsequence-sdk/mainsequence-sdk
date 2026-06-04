from __future__ import annotations

import uuid

from sqlalchemy import ForeignKey, Index, MetaData, String, Uuid
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from examples.meta_tables.common import (
    DEFAULT_SCHEMA,
    print_json,
)
from mainsequence.meta_tables import PlatformManagedMetaTable

NAMESPACE = "sdk-examples"
PROJECT_NAME = "sdk_examples"

NAMING_CONVENTION = {
    "ix": "%(table_name)s_%(column_0_name)s_idx",
    "pk": "%(table_name)s_pkey",
}


class Base(DeclarativeBase):
    metadata = MetaData(naming_convention=NAMING_CONVENTION)


class Account(PlatformManagedMetaTable, Base):
    __tablename__ = f"{PROJECT_NAME}__account"
    __table_args__ = {"schema": DEFAULT_SCHEMA}

    __metatable_namespace__ = NAMESPACE
    __metatable_identifier__ = f"{PROJECT_NAME}.Account"

    uid: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True)
    name: Mapped[str] = mapped_column(String(255), nullable=False)


class Asset(PlatformManagedMetaTable, Base):
    __tablename__ = f"{PROJECT_NAME}__asset"
    __table_args__ = (
        Index(None, "account_uid"),
        {"schema": DEFAULT_SCHEMA},
    )

    __metatable_namespace__ = NAMESPACE
    __metatable_identifier__ = f"{PROJECT_NAME}.Asset"

    uid: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True)
    account_uid: Mapped[uuid.UUID] = mapped_column(
        Uuid,
        ForeignKey(f"{DEFAULT_SCHEMA}.{PROJECT_NAME}__account.uid", ondelete="RESTRICT"),
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
