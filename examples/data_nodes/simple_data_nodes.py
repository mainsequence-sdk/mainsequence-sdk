"""
These data nodes do not serve any practical purpose but only exemplify creation and best practices.



"""

import datetime
import os
import uuid
from typing import Any, Union

import numpy as np
import pandas as pd
from pydantic import BaseModel, Field
from sqlalchemy import DateTime, Float, ForeignKey, Index, MetaData, String, Uuid
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from mainsequence.client import MetaTable
from mainsequence.meta_tables import (
    PlatformManagedMetaTable,
    PlatformTimeIndexMetaTable,
    schema_table_name,
    sqlalchemy_naming_convention,
)
from mainsequence.meta_tables.compiled_sql.v1 import build_operation
from mainsequence.meta_tables.data_nodes import (
    APIDataNode,
    DataNode,
    DataNodeConfiguration,
    hash_namespace,
)

PROJECT_UID = os.getenv("MAIN_SEQUENCE_PROJECT_UID", "local").strip() or "local"
PROJECT_TABLE_SUFFIX = "".join(char if char.isalnum() else "_" for char in PROJECT_UID.lower())
PROJECT_TABLE_APP = "mainsequence_examples"
ACCOUNT_TABLE_NAME = schema_table_name(PROJECT_TABLE_APP, "account", PROJECT_TABLE_SUFFIX)
DAILY_RANDOM_NUMBER_TABLE_NAME = schema_table_name(
    PROJECT_TABLE_APP,
    "daily_random_number",
    PROJECT_TABLE_SUFFIX,
)
DAILY_RANDOM_ADDITION_TABLE_NAME = schema_table_name(
    PROJECT_TABLE_APP,
    "daily_random_addition",
    PROJECT_TABLE_SUFFIX,
)
ACCOUNT_HOLDINGS_TABLE_NAME = schema_table_name(
    PROJECT_TABLE_APP,
    "account_holdings",
    PROJECT_TABLE_SUFFIX,
)


class Base(DeclarativeBase):
    metadata = MetaData(naming_convention=sqlalchemy_naming_convention())


class Account(PlatformManagedMetaTable, Base):
    __tablename__ = ACCOUNT_TABLE_NAME
    __metatable_namespace__ = "mainsequence.examples"
    __metatable_identifier__ = f"account_{PROJECT_UID}"
    __metatable_description__ = "Tutorial account master rows used to scope account holdings."
    __metatable_labels__ = ["example", "data-node"]
    __metatable_extra_hash_components__ = {"storage_name": "account"}

    uid: Mapped[uuid.UUID] = mapped_column(
        Uuid,
        primary_key=True,
        info={
            "label": "Account UID",
            "description": "Stable account identifier referenced by holdings examples.",
        },
    )
    account_code: Mapped[str] = mapped_column(
        String(64),
        nullable=False,
        unique=True,
        info={
            "label": "Account Code",
            "description": "Human-readable account code used to upsert tutorial account rows.",
        },
    )
    name: Mapped[str] = mapped_column(
        String(255),
        nullable=False,
        info={
            "label": "Name",
            "description": "Display name for the tutorial account.",
        },
    )


class DailyRandomNumberStorage(PlatformTimeIndexMetaTable, Base):
    __tablename__ = DAILY_RANDOM_NUMBER_TABLE_NAME
    __metatable_namespace__ = "mainsequence.examples"
    __metatable_identifier__ = f"daily_random_number_{PROJECT_UID}"
    __metatable_description__ = "Daily random number observations produced by the tutorial node."
    __metatable_labels__ = ["example", "data-node"]
    __metatable_extra_hash_components__ = {"storage_name": "daily_random_number"}
    __time_index_name__ = "time_index"
    __index_names__ = ["time_index"]

    time_index: Mapped[datetime.datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        info={
            "label": "Time Index",
            "description": "UTC day for the generated random observation.",
        },
    )
    random_number: Mapped[float] = mapped_column(
        Float,
        nullable=False,
        info={
            "label": "Random Number",
            "description": "Random value generated for this UTC day.",
        },
    )


class DailyRandomAdditionStorage(PlatformTimeIndexMetaTable, Base):
    __tablename__ = DAILY_RANDOM_ADDITION_TABLE_NAME
    __metatable_namespace__ = "mainsequence.examples"
    __metatable_identifier__ = f"daily_random_addition_{PROJECT_UID}"
    __metatable_description__ = (
        "Daily random addition observations combining local noise with the random-number "
        "dependency."
    )
    __metatable_labels__ = ["example", "data-node"]
    __metatable_extra_hash_components__ = {"storage_name": "daily_random_addition"}
    __time_index_name__ = "time_index"
    __index_names__ = ["time_index"]

    time_index: Mapped[datetime.datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        info={
            "label": "Time Index",
            "description": "UTC day for the dependent random-addition observation.",
        },
    )
    random_number: Mapped[float] = mapped_column(
        Float,
        nullable=False,
        info={
            "label": "Random Number",
            "description": "Random value after adding dependency noise for this UTC day.",
        },
    )


class AccountHoldingsStorage(PlatformTimeIndexMetaTable, Base):
    __tablename__ = ACCOUNT_HOLDINGS_TABLE_NAME
    __table_args__ = (Index(None, "account_uid"),)
    __metatable_namespace__ = "mainsequence.examples"
    __metatable_identifier__ = f"account_holdings_{PROJECT_UID}"
    __metatable_description__ = (
        "Time-indexed tutorial holdings by account and instrument identifier."
    )
    __metatable_labels__ = ["example", "data-node"]
    __metatable_extra_hash_components__ = {"storage_name": "account_holdings"}
    __time_index_name__ = "time_index"
    __index_names__ = ["time_index", "account_uid", "unique_identifier"]

    time_index: Mapped[datetime.datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        info={
            "label": "Time Index",
            "description": "UTC minute for the holdings snapshot.",
        },
    )
    account_uid: Mapped[uuid.UUID] = mapped_column(
        Uuid,
        ForeignKey(
            f"public.{ACCOUNT_TABLE_NAME}.uid",
            ondelete="RESTRICT",
        ),
        nullable=False,
        info={
            "label": "Account UID",
            "description": "Account identifier that scopes this holdings snapshot.",
        },
    )
    unique_identifier: Mapped[str] = mapped_column(
        String(64),
        nullable=False,
        info={
            "label": "Unique Identifier",
            "description": "Instrument identifier held by the account.",
        },
    )
    quantity: Mapped[float] = mapped_column(
        Float,
        nullable=False,
        info={
            "label": "Quantity",
            "description": "Position quantity held for the instrument at the snapshot time.",
        },
    )


def upsert_account(
    account_meta_table: MetaTable,
    *,
    account_code: str,
    name: str,
) -> uuid.UUID:
    operation = build_operation(
        operation="insert",
        sql=f"""
            INSERT INTO {account_meta_table.physical_table_name}
                (account_code, name)
            VALUES
                (%(account_code)s, %(name)s)
            ON CONFLICT (account_code) DO UPDATE SET
                name = EXCLUDED.name
            RETURNING uid
        """,
        parameters={
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
    result = MetaTable.execute_operation(operation)
    rows = result.get("rows") if isinstance(result, dict) else None
    if not rows:
        raise RuntimeError("Account upsert did not return a backend-generated uid.")
    row: Any = rows[0]
    return uuid.UUID(str(row["uid"]))


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
    skew: bool = Field(
        ...,
        description="Whether to skew the generated volatility profile.",
        examples=[True, False],
    )


class RandomDataNodeConfig(DataNodeConfiguration):
    mean: float = Field(
        ...,
        title="Mean",
        description="Mean for the random normal distribution generator.",
        examples=[0.0, 1.0],
    )
    std: VolatilityConfig = Field(
        VolatilityConfig(center=1, skew=True),
        title="Vol Config",
        description="Volatility configuration for the random normal distribution.",
    )


class DailyRandomAdditionConfig(DataNodeConfiguration):
    mean: float = Field(
        ...,
        description="Mean for the random normal distribution used by the addition node.",
        examples=[0.0],
    )
    std: float = Field(
        ...,
        description="Standard deviation for the random normal distribution.",
        examples=[1.0],
    )
    daily_random_number_storage_table: type[PlatformTimeIndexMetaTable] = Field(
        ...,
        description="Storage table for the upstream daily random-number dependency.",
    )


class DailyRandomAdditionAPIConfig(DataNodeConfiguration):
    mean: float = Field(
        ...,
        description="Mean for the random normal distribution used by the API dependency node.",
        examples=[0.0],
    )
    std: float = Field(
        ...,
        description="Standard deviation for the random normal distribution.",
        examples=[1.0],
    )
    dependency_identifier: str = Field(
        ...,
        description="Published storage identifier used to resolve the upstream APIDataNode.",
        examples=[f"daily_random_number_{PROJECT_UID}"],
    )


class AccountHoldingsConfig(DataNodeConfiguration):
    account_uid: uuid.UUID = Field(
        ...,
        description="Account UID whose holdings snapshot should be produced.",
        examples=["00000000-0000-4000-8000-000000000001"],
    )


class DailyRandomNumber(DataNode):
    """
    Example Data Node that generates one random number every day  every day
    """

    def __init__(
        self,
        config: RandomDataNodeConfig,
        storage_table: type[PlatformTimeIndexMetaTable],
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
            index=pd.DatetimeIndex(
                [today],
                name="time_index",
                dtype="datetime64[ns, UTC]",
            ),
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
        storage_table: type[PlatformTimeIndexMetaTable],
        *,
        hash_namespace: str | None = None,
    ):
        self.config = config
        self.mean = config.mean
        self.std = config.std
        self.daily_random_number_data_node = DailyRandomNumber(
            config=RandomDataNodeConfig(mean=0.0),
            storage_table=config.daily_random_number_storage_table,
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
            index=pd.DatetimeIndex(
                [today],
                name="time_index",
                dtype="datetime64[ns, UTC]",
            ),
        )


class DailyRandomAdditionAPI(DataNode):
    def __init__(
        self,
        config: DailyRandomAdditionAPIConfig,
        storage_table: type[PlatformTimeIndexMetaTable],
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
            index=pd.DatetimeIndex(
                [today],
                name="time_index",
                dtype="datetime64[ns, UTC]",
            ),
        )


class AccountHoldingsSnapshot(DataNode):
    def __init__(
        self,
        config: AccountHoldingsConfig,
        storage_table: type[PlatformTimeIndexMetaTable],
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
        time_index = pd.DatetimeIndex(
            [row[0] for row in rows],
            dtype="datetime64[ns, UTC]",
        )
        index = pd.MultiIndex.from_arrays(
            [
                time_index,
                [row[1] for row in rows],
                [row[2] for row in rows],
            ],
            names=["time_index", "account_uid", "unique_identifier"],
        )
        return pd.DataFrame(
            {"quantity": [row[3] for row in rows]},
            index=index,
        )


def run_graph(
    label: str,
    *,
    number_storage_table: type[PlatformTimeIndexMetaTable],
    addition_storage_table: type[PlatformTimeIndexMetaTable],
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
        config=DailyRandomAdditionConfig(
            mean=0.0,
            std=1.0,
            daily_random_number_storage_table=number_storage_table,
        ),
        storage_table=addition_storage_table,
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
    account_holdings_storage_table: type[PlatformTimeIndexMetaTable],
):
    account_uid = upsert_account(
        account_meta_table,
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


def run_data_node_examples():
    account_meta_table = Account.register()

    # -------------------------
    # A) NORMAL / PRODUCTION
    # -------------------------
    run_graph(
        "PROD(no namespace)",
        number_storage_table=DailyRandomNumberStorage,
        addition_storage_table=DailyRandomAdditionStorage,
    )

    # -------------------------
    # B) TEST / NAMESPACED
    # -------------------------
    # Everything created inside gets a different update_hash while writing to
    # the same registered MetaTable contracts.
    with hash_namespace("pytest"):
        run_graph(
            "TEST(namespace=pytest)",
            number_storage_table=DailyRandomNumberStorage,
            addition_storage_table=DailyRandomAdditionStorage,
        )
        run_account_holdings_example(
            account_meta_table=account_meta_table,
            account_holdings_storage_table=AccountHoldingsStorage,
        )


if __name__ == "__main__":
    run_data_node_examples()
