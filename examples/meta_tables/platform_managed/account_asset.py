from __future__ import annotations

import uuid

from sqlalchemy import ForeignKey, Index, MetaData, String, Uuid
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from examples.meta_tables.common import (
    DEFAULT_SCHEMA,
    DEFAULT_TIMEOUT,
    print_json,
)
from mainsequence.meta_tables import PlatformManagedMetaTable

NAMESPACE = "sdk-examples"

NAMING_CONVENTION = {
    "ix": "%(table_name)s_%(column_0_name)s_idx",
    "fk": "%(table_name)s_%(column_0_name)s_fkey",
    "pk": "%(table_name)s_pkey",
}


class Base(DeclarativeBase):
    metadata = MetaData(naming_convention=NAMING_CONVENTION)


class Account(PlatformManagedMetaTable, Base):
    __table_args__ = {"schema": DEFAULT_SCHEMA}

    __metatable_namespace__ = NAMESPACE
    __metatable_identifier__ = "Account"

    uid: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True)
    name: Mapped[str] = mapped_column(String(255), nullable=False)


class Asset(PlatformManagedMetaTable, Base):
    __table_args__ = (
        Index(None, "account_uid"),
        {"schema": DEFAULT_SCHEMA},
    )

    __metatable_namespace__ = NAMESPACE
    __metatable_identifier__ = "Asset"

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


def main() -> None:
    account_meta_table = Account.register(
        timeout=DEFAULT_TIMEOUT,
        description="Example platform-managed account table.",
        labels=["sdk-example"],
    )
    asset_meta_table = Asset.register(
        timeout=DEFAULT_TIMEOUT,
        description="Example platform-managed asset table.",
        labels=["sdk-example"],
    )
    print_json("Registered Account MetaTable", account_meta_table)
    print_json("Registered Asset MetaTable", asset_meta_table)


if __name__ == "__main__":
    main()
