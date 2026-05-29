from __future__ import annotations

import uuid

from sqlalchemy import ForeignKey, Index, MetaData, String, Uuid, create_engine
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from examples.meta_tables.common import (
    DEFAULT_SCHEMA,
    DEFAULT_TIMEOUT,
    print_json,
)
from mainsequence.client import DataSource, DynamicTableDataSource, MetaTable
from mainsequence.client.data_sources_interfaces import get_sqlite_interface_class
from mainsequence.meta_tables import (
    external_registered_registration_request_from_sqlalchemy_model,
)

NAMESPACE = "sdk-examples"

NAMING_CONVENTION = {
    "ix": "%(table_name)s_%(column_0_name)s_idx",
    "fk": "%(table_name)s_%(column_0_name)s_fkey",
    "pk": "%(table_name)s_pkey",
}


class Base(DeclarativeBase):
    metadata = MetaData(naming_convention=NAMING_CONVENTION)


class Account(Base):
    __tablename__ = "account"

    __metatable_namespace__ = NAMESPACE
    __metatable_identifier__ = "Account"

    uid: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True)
    name: Mapped[str] = mapped_column(String(255), nullable=False)


class Asset(Base):
    __tablename__ = "asset"
    __table_args__ = (Index(None, "account_uid"),)

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


def local_sqlite_database_url() -> str:
    sqlite_interface = get_sqlite_interface_class()()
    sqlite_file = sqlite_interface.db_file
    sqlite_interface.con.close()
    return f"sqlite:///{sqlite_file.as_posix()}"


def create_physical_tables_for_demo() -> None:
    """Create the externally managed SQLite tables before registration."""

    engine = create_engine(local_sqlite_database_url())
    try:
        Base.metadata.create_all(engine)
    finally:
        engine.dispose()


def create_local_sqlite_dynamic_data_source() -> DynamicTableDataSource:
    physical_data_source = DataSource.create_sqlite(time_out=DEFAULT_TIMEOUT)
    dynamic_data_source = DynamicTableDataSource.create_sqlite(
        data_source=physical_data_source,
        time_out=DEFAULT_TIMEOUT,
    )
    if not dynamic_data_source.uid:
        raise ValueError("SQLite DynamicTableDataSource response did not include a uid.")
    return dynamic_data_source


def main() -> None:
    dynamic_data_source = create_local_sqlite_dynamic_data_source()
    create_physical_tables_for_demo()
    print("\nCreated physical Account and Asset tables in local SQLite.")

    account_request = external_registered_registration_request_from_sqlalchemy_model(
        Account,
        data_source_uid=dynamic_data_source.uid,
        schema=DEFAULT_SCHEMA,
        introspect=False,
        description="Example externally managed account table.",
        labels=["sdk-example", "external-managed", "sqlite"],
    )
    account_meta_table = MetaTable.register(account_request, timeout=DEFAULT_TIMEOUT)

    asset_request = external_registered_registration_request_from_sqlalchemy_model(
        Asset,
        data_source_uid=dynamic_data_source.uid,
        schema=DEFAULT_SCHEMA,
        introspect=False,
        description="Example externally managed asset table.",
        labels=["sdk-example", "external-managed", "sqlite"],
        target_meta_table_uid_by_fullname={
            Account.__table__.fullname: account_meta_table.uid,
        },
    )
    asset_meta_table = MetaTable.register(asset_request, timeout=DEFAULT_TIMEOUT)
    print_json("Registered Account MetaTable", account_meta_table)
    print_json("Registered Asset MetaTable", asset_meta_table)


if __name__ == "__main__":
    main()
