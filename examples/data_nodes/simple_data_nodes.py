"""
These data nodes do not serve any practical purpose but only exemplify creation and best practices.



"""

import datetime
import os
import uuid
from typing import Union

import numpy as np
import pandas as pd
from pydantic import BaseModel, Field
from sqlalchemy import DateTime, Float, ForeignKey, Index, MetaData, String, Uuid
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

import mainsequence.client as msc
from mainsequence.client import MetaTable
from mainsequence.meta_tables import (
    PlatformManagedMetaTable,
    PlatformTimeIndexMetaData,
    build_compiled_sql_v1_operation,
)
from mainsequence.meta_tables.data_nodes import (
    APIDataNode,
    DataNode,
    DataNodeConfiguration,
    hash_namespace,
)

PROJECT_UID = os.getenv("MAIN_SEQUENCE_PROJECT_UID", "local").strip() or "local"


class Base(DeclarativeBase):
    metadata = MetaData()


class Account(PlatformManagedMetaTable, Base):
    __metatable_namespace__ = f"mainsequence.examples.{PROJECT_UID}.accounts"
    __metatable_identifier__ = f"account_{PROJECT_UID}"

    uid: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True)
    account_code: Mapped[str] = mapped_column(String(64), nullable=False, unique=True)
    name: Mapped[str] = mapped_column(String(255), nullable=False)


class DailyRandomNumberStorage(PlatformTimeIndexMetaData, Base):
    __metatable_namespace__ = f"mainsequence.examples.{PROJECT_UID}.daily_random_number"
    __metatable_identifier__ = f"daily_random_number_{PROJECT_UID}"
    __time_index_name__ = "time_index"
    __index_names__ = ["time_index"]

    time_index: Mapped[datetime.datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
    )
    random_number: Mapped[float] = mapped_column(Float, nullable=False)


class DailyRandomAdditionStorage(PlatformTimeIndexMetaData, Base):
    __metatable_namespace__ = f"mainsequence.examples.{PROJECT_UID}.daily_random_addition"
    __metatable_identifier__ = f"daily_random_addition_{PROJECT_UID}"
    __time_index_name__ = "time_index"
    __index_names__ = ["time_index"]

    time_index: Mapped[datetime.datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
    )
    random_number: Mapped[float] = mapped_column(Float, nullable=False)


class AccountHoldingsStorage(PlatformTimeIndexMetaData, Base):
    __table_args__ = (Index(None, "account_uid"),)
    __metatable_namespace__ = f"mainsequence.examples.{PROJECT_UID}.account_holdings"
    __metatable_identifier__ = f"account_holdings_{PROJECT_UID}"
    __time_index_name__ = "time_index"
    __index_names__ = ["time_index", "account_uid", "unique_identifier"]

    time_index: Mapped[datetime.datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
    )
    account_uid: Mapped[uuid.UUID] = mapped_column(
        Uuid,
        ForeignKey(
            f"{Account.__table__.fullname}.uid",
            ondelete="RESTRICT",
        ),
        nullable=False,
    )
    unique_identifier: Mapped[str] = mapped_column(
        String(64),
        nullable=False,
    )
    quantity: Mapped[float] = mapped_column(Float, nullable=False)


def register_account_table(data_source_uid: str) -> MetaTable:
    return Account.register(
        data_source_uid=data_source_uid,
        description="Example platform-managed account table.",
        labels=["example", "data-node"],
    )


def upsert_account(
    account_meta_table: MetaTable,
    *,
    account_uid: uuid.UUID,
    account_code: str,
    name: str,
) -> None:
    operation = build_compiled_sql_v1_operation(
        operation="insert",
        sql=f"""
            INSERT INTO {account_meta_table.physical_table_name}
                (uid, account_code, name)
            VALUES
                (%(uid)s, %(account_code)s, %(name)s)
            ON CONFLICT (uid) DO UPDATE SET
                account_code = EXCLUDED.account_code,
                name = EXCLUDED.name
            RETURNING uid
        """,
        parameters={
            "uid": str(account_uid),
            "account_code": account_code,
            "name": name,
        },
        scope={
            "tables": [
                {
                    "meta_table_uid": account_meta_table.uid,
                    "alias": "account",
                    "access": "write",
                }
            ]
        },
        limits={"max_rows": 1, "statement_timeout_ms": 15000},
    )
    MetaTable.execute_operation(operation)


def register_daily_random_number_storage_table(
    data_source_uid: str,
) -> type[PlatformTimeIndexMetaData]:
    DailyRandomNumberStorage.register(
        data_source_uid=data_source_uid,
        description="Example DataNode storage table for daily random numbers.",
        labels=["example", "data-node"],
    )
    return DailyRandomNumberStorage


def register_daily_random_addition_storage_table(
    data_source_uid: str,
) -> type[PlatformTimeIndexMetaData]:
    DailyRandomAdditionStorage.register(
        data_source_uid=data_source_uid,
        description="Example DataNode storage table for daily random additions.",
        labels=["example", "data-node"],
    )
    return DailyRandomAdditionStorage


def register_account_holdings_storage_table(
    data_source_uid: str,
    account_meta_table: MetaTable,
) -> type[PlatformTimeIndexMetaData]:
    AccountHoldingsStorage.register(
        data_source_uid=data_source_uid,
        description="Example DataNode storage table for account holdings.",
        labels=["example", "data-node"],
        target_meta_tables={Account: account_meta_table},
    )
    return AccountHoldingsStorage


def active_project_data_source_uid() -> str:
    data_source = msc.get_session_data_source()
    data_source_uid = getattr(data_source, "uid", None)
    if data_source_uid in (None, ""):
        raise RuntimeError("The active project data source must expose a uid.")
    return str(data_source_uid)


class VolatilityConfig(BaseModel):
    center: float = Field(
        ...,
        title="Standard Deviation",
        description="Standard deviation of the normal distribution (must be > 0).",
        examples=[0.1, 1.0, 2.5],
        gt=0,  # constraint: strictly positive
        le=1e6,  # example upper bound (optional)
        multiple_of=0.0001,  # example precision step (optional)
    )
    skew: bool


class RandomDataNodeConfig(DataNodeConfiguration):
    mean: float = Field(
        ...,
        title="Mean",
        description="Mean for the random normal distribution generator",
    )
    std: VolatilityConfig = Field(
        VolatilityConfig(center=1, skew=True),
        title="Vol Config",
        description="Vol Configuration",
    )


class DailyRandomAdditionConfig(DataNodeConfiguration):
    mean: float
    std: float


class DailyRandomAdditionAPIConfig(DataNodeConfiguration):
    mean: float
    std: float
    dependency_identifier: str


class AccountHoldingsConfig(DataNodeConfiguration):
    account_uid: uuid.UUID


class DailyRandomNumber(DataNode):
    """
    Example Data Node that generates one random number every day  every day
    """

    def __init__(
        self,
        config: RandomDataNodeConfig,
        storage_table: type[PlatformTimeIndexMetaData],
        *,
        hash_namespace: str | None = None,
    ):
        """
        :param mean:  the mean of the probability  distribution
        :param std: the std of the probability  distribution
        """
        self.config = config
        self.mean = config.mean
        self.std = config.std
        super().__init__(
            config=config,
            storage_table=storage_table,
            hash_namespace=hash_namespace,
        )

    def update(self) -> pd.DataFrame:
        """Draw daily samples from N(mean, std) since last run (UTC days)."""
        today = pd.Timestamp.now("UTC").normalize()
        last = self.update_statistics.max_time_index_value
        if last is not None and last >= today:
            return pd.DataFrame()
        return pd.DataFrame(
            {"random_number": [np.random.normal(self.mean, self.std.center)]},
            index=pd.DatetimeIndex([today], name="time_index", tz="UTC"),
        )

    def dependencies(self) -> dict[str, Union["DataNode", "APIDataNode"]]:
        """
        This nodes does not depend on any other data nodes.
        """
        return {}


class DailyRandomAddition(DataNode):
    def __init__(
        self,
        config: DailyRandomAdditionConfig,
        storage_table: type[PlatformTimeIndexMetaData],
        daily_random_number_storage_table: type[PlatformTimeIndexMetaData],
        *,
        hash_namespace: str | None = None,
    ):
        self.config = config
        self.mean = config.mean
        self.std = config.std
        self.daily_random_number_data_node = DailyRandomNumber(
            config=RandomDataNodeConfig(mean=0.0),
            storage_table=daily_random_number_storage_table,
            hash_namespace=hash_namespace,
        )
        super().__init__(
            config=config,
            storage_table=storage_table,
            hash_namespace=hash_namespace,
        )

    def dependencies(self):
        return {"number_generator": self.daily_random_number_data_node}

    def update(self) -> pd.DataFrame:
        """Draw daily samples from N(mean, std) since last run (UTC days)."""
        today = pd.Timestamp.now("UTC").normalize()
        last = self.update_statistics.max_time_index_value
        if last is not None and last >= today:
            return pd.DataFrame()
        random_number = np.random.normal(self.mean, self.std)
        dependency_noise = self.daily_random_number_data_node.get_df_between_dates(
            start_date=today, great_or_equal=True
        ).iloc[0]["random_number"]
        self.logger.info(f"random_number={random_number} dependency_noise={dependency_noise}")

        return pd.DataFrame(
            {"random_number": [random_number + dependency_noise]},
            index=pd.DatetimeIndex([today], name="time_index", tz="UTC"),
        )


class DailyRandomAdditionAPI(DataNode):
    def __init__(
        self,
        config: DailyRandomAdditionAPIConfig,
        storage_table: type[PlatformTimeIndexMetaData],
        *,
        hash_namespace: str | None = None,
    ):
        self.config = config
        self.mean = config.mean
        self.std = config.std

        self.daily_random_number_data_node = APIDataNode.build_from_identifier(
            identifier=config.dependency_identifier
        )
        super().__init__(
            config=config,
            storage_table=storage_table,
            hash_namespace=hash_namespace,
        )

    def dependencies(self):
        return {"number_generator": self.daily_random_number_data_node}

    def update(self) -> pd.DataFrame:
        """Draw daily samples from N(mean, std) since last run (UTC days)."""
        today = pd.Timestamp.now("UTC").normalize()
        last = self.update_statistics.max_time_index_value
        if last is not None and last >= today:
            return pd.DataFrame()
        random_number = np.random.normal(self.mean, self.std)
        dependency_noise = self.daily_random_number_data_node.get_df_between_dates(
            start_date=today, great_or_equal=True
        ).iloc[0]["random_number"]
        self.logger.info(f"random_number={random_number} dependency_noise={dependency_noise}")

        return pd.DataFrame(
            {"random_number": [random_number + dependency_noise]},
            index=pd.DatetimeIndex([today], name="time_index", tz="UTC"),
        )


class AccountHoldingsSnapshot(DataNode):
    def __init__(
        self,
        config: AccountHoldingsConfig,
        storage_table: type[PlatformTimeIndexMetaData],
        *,
        hash_namespace: str | None = None,
    ):
        self.account_uid = config.account_uid
        super().__init__(
            config=config,
            storage_table=storage_table,
            hash_namespace=hash_namespace,
        )

    def dependencies(self):
        return {}

    def update(self) -> pd.DataFrame:
        current_minute = pd.Timestamp.now("UTC").floor("min")
        last = self.update_statistics.max_time_index_value
        if last is not None and last >= current_minute:
            return pd.DataFrame()

        minute_offset = current_minute.hour * 60 + current_minute.minute
        rows = [
            (current_minute, self.account_uid, "AAPL", 12.0 + (minute_offset % 5)),
            (current_minute, self.account_uid, "MSFT", 8.0 + (minute_offset % 3)),
        ]
        index = pd.MultiIndex.from_tuples(
            [(row[0], row[1], row[2]) for row in rows],
            names=["time_index", "account_uid", "unique_identifier"],
        )
        return pd.DataFrame(
            {"quantity": [row[3] for row in rows]},
            index=index,
        )


def run_graph(
    label: str,
    *,
    number_storage_table: type[PlatformTimeIndexMetaData],
    addition_storage_table: type[PlatformTimeIndexMetaData],
):
    print(f"\n===== {label} =====")

    # 1) Standalone node
    dn = DailyRandomNumber(
        config=RandomDataNodeConfig(mean=0.0),
        storage_table=number_storage_table,
    )
    print(f"{label} DailyRandomNumber.update_hash  = {dn.update_hash}")
    print(f"{label} DailyRandomNumber.meta_table_uid = {dn.storage_table.get_meta_table_uid()}")
    dn.run(debug_mode=True, force_update=True)

    # This is the identifier we will use for the APIDataNode example.
    dep_identifier = number_storage_table.__metatable_identifier__
    print(f"{label} dependency identifier = {dep_identifier}")

    # 2) Node with DataNode dependency.
    add = DailyRandomAddition(
        config=DailyRandomAdditionConfig(mean=0.0, std=1.0),
        storage_table=addition_storage_table,
        daily_random_number_storage_table=number_storage_table,
    )
    print(f"{label} DailyRandomAddition.update_hash  = {add.update_hash}")
    print(f"{label} DailyRandomAddition.meta_table_uid = {add.storage_table.get_meta_table_uid()}")
    print(f"{label}   dep(update_hash) = {add.daily_random_number_data_node.update_hash}")
    print(
        f"{label}   dep(meta_table_uid)= "
        f"{add.daily_random_number_data_node.storage_table.get_meta_table_uid()}"
    )
    add.run(debug_mode=True, force_update=True)

    # 3) Node with API dependency (identifier-based)
    api = DailyRandomAdditionAPI(
        config=DailyRandomAdditionAPIConfig(
            mean=0.0,
            std=1.0,
            dependency_identifier=dep_identifier,
        ),
        storage_table=addition_storage_table,
    )
    print(f"{label} DailyRandomAdditionAPI.update_hash  = {api.update_hash}")
    print(
        f"{label} DailyRandomAdditionAPI.meta_table_uid = {api.storage_table.get_meta_table_uid()}"
    )
    api.run(debug_mode=True, force_update=True)


def run_account_holdings_example(
    *,
    account_meta_table: MetaTable,
    account_holdings_storage_table: type[PlatformTimeIndexMetaData],
):
    account_uid = uuid.UUID("00000000-0000-4000-8000-000000000001")
    upsert_account(
        account_meta_table,
        account_uid=account_uid,
        account_code="TUTORIAL",
        name="Tutorial Account",
    )
    node = AccountHoldingsSnapshot(
        config=AccountHoldingsConfig(account_uid=account_uid),
        storage_table=account_holdings_storage_table,
    )
    print(f"AccountHoldingsSnapshot.update_hash = {node.update_hash}")
    print(f"AccountHoldingsSnapshot.meta_table_uid = {node.storage_table.get_meta_table_uid()}")
    node.run(debug_mode=True, force_update=True)


def build_test_time_series():
    data_source_uid = active_project_data_source_uid()
    number_storage_table = register_daily_random_number_storage_table(data_source_uid)
    addition_storage_table = register_daily_random_addition_storage_table(data_source_uid)
    account_meta_table = register_account_table(data_source_uid)
    account_holdings_storage_table = register_account_holdings_storage_table(
        data_source_uid,
        account_meta_table,
    )

    # -------------------------
    # A) NORMAL / PRODUCTION
    # -------------------------
    run_graph(
        "PROD(no namespace)",
        number_storage_table=number_storage_table,
        addition_storage_table=addition_storage_table,
    )

    # -------------------------
    # B) TEST / NAMESPACED
    # -------------------------
    # Everything created inside gets a different update_hash while writing to
    # the same registered platform-managed table contract.
    with hash_namespace("pytest"):
        run_graph(
            "TEST(namespace=pytest)",
            number_storage_table=number_storage_table,
            addition_storage_table=addition_storage_table,
        )
        run_account_holdings_example(
            account_meta_table=account_meta_table,
            account_holdings_storage_table=account_holdings_storage_table,
        )


if __name__ == "__main__":
    build_test_time_series()
