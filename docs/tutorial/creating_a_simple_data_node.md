# Part 3: Creating a Data Node

## Quick Summary

In this part, you will:

- create your first DataNode and run it locally
- understand a DataNode as an opinionated MetaTable-backed update workflow
- register the canonical MetaTable contract used by the DataNode
- add a second DataNode that depends on the first one
- run launcher scripts from the terminal and inspect persisted tables from the CLI
- learn how DataNode update identity relates to the underlying table

DataNodes created in this part: **`DailyRandomNumber`**, **`DailyRandomAddition`**,
and **`AccountHoldingsSnapshot`**. Canonical platform-managed MetaTable example:
**`Account`**.

## 1. Create Your First DataNode

**Key concepts:** data DAGs, `DataNode`, MetaTable contracts, dependencies, update identity, and MetaTable-backed storage.

Main Sequence encourages you to model workflows as data DAGs (directed acyclic graphs), composing your work into small steps called **data nodes**, each performing a single transformation.

You already saw `MetaTable` as the canonical table abstraction. A `DataNode` is
the opinionated update layer for one of those table contracts: it defines how
data is produced, refreshed, and connected to other resources.

In this chapter, you will start with one standalone node, run it locally, and
then extend it with a dependent node.

Create a new file at `src\data_nodes\example_nodes.py` (Windows) or `src/data_nodes/example_nodes.py` (macOS/Linux), and define your first node, `DailyRandomNumber`, by subclassing `DataNode`.



```python
import datetime
import os
import uuid
from typing import Dict, Union

import numpy as np
import pandas as pd
from pydantic import BaseModel, Field
from sqlalchemy import DateTime, Float, ForeignKey, Index, MetaData, String, Uuid
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from mainsequence.client import MetaTable
from mainsequence.meta_tables import (
    APIDataNode,
    DataNode,
    DataNodeConfiguration,
)
from mainsequence.meta_tables import (
    PlatformManagedMetaTable,
    PlatformTimeIndexMetaData,
    build_compiled_sql_v1_operation,
)

PROJECT_UID = os.getenv("MAIN_SEQUENCE_PROJECT_UID", "local").strip() or "local"


class Base(DeclarativeBase):
    metadata = MetaData()


class DailyRandomNumberStorage(PlatformTimeIndexMetaData, Base):
    __metatable_namespace__ = "mainsequence.examples"
    __metatable_identifier__ = f"daily_random_number_{PROJECT_UID}"
    __metatable_extra_hash_components__ = {"storage_name": "daily_random_number"}
    __time_index_name__ = "time_index"
    __index_names__ = ["time_index"]

    time_index: Mapped[datetime.datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
    )
    random_number: Mapped[float] = mapped_column(Float, nullable=False)


class DailyRandomAdditionStorage(PlatformTimeIndexMetaData, Base):
    __metatable_namespace__ = "mainsequence.examples"
    __metatable_identifier__ = f"daily_random_addition_{PROJECT_UID}"
    __metatable_extra_hash_components__ = {"storage_name": "daily_random_addition"}
    __time_index_name__ = "time_index"
    __index_names__ = ["time_index"]

    time_index: Mapped[datetime.datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
    )
    random_number: Mapped[float] = mapped_column(Float, nullable=False)

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


class DailyRandomNumber(DataNode):
    """
    Example Data Node that generates one random number every day
    """

    def __init__(
        self,
        config: RandomDataNodeConfig,
        storage_table: type[PlatformTimeIndexMetaData],
        *,
        hash_namespace: str | None = None,
    ):
        """
        :param config: Configuration containing mean and volatility
        :param storage_table: Registered PlatformTimeIndexMetaData model used as storage
        """
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

    def dependencies(self) -> Dict[str, Union["DataNode", "APIDataNode"]]:
        """
        This node does not depend on any other data nodes.
        """
        return {}
```

Register the concrete `PlatformTimeIndexMetaData` model before constructing the
node. The `DataNode` constructor receives that registered model class as
`storage_table`; it does not create or resolve storage inside the update path.

The SQLAlchemy model is the first-class schema declaration for the table:
`time_index` is the index column and `random_number` is the value column. The
`PlatformTimeIndexMetaData.register(...)` call sends the canonical time-indexed
table contract to the backend and binds the returned MetaTable UID to the class.
The DataFrame returned by `update()` must match that table contract.

MetaTable foreign keys require a registered MetaTable target, so this first
tutorial keeps the runnable example focused on a single table. For the FK
authoring surface, see [Data Nodes Knowledge Guide](../knowledge/data_nodes.md).

!!! important
    `MetaTable.identifier` and namespace must be unique enough to find the table later. In tutorial code, generic names like `daily_random_number` are very likely to collide because someone else in your organization has probably already run the same tutorial.

    That is why this example includes `MAIN_SEQUENCE_PROJECT_UID` in the
    identifier. It gives each project a stable table identity while keeping all
    tutorial tables in the canonical `mainsequence.examples` namespace.
    The explicit `storage_name` hash component gives each storage model its own
    physical table name even when two storage models have the same column shape.

    `identifier` is published metadata, not hash identity. That means you can
    later repoint a published identifier to a different backing table during a migration
    without changing the table contract or the update identity.

    This is different from the `unique_identifier` field used later in MultiIndex entity tables. Here, you are naming the table itself, not an individual row entity.

    If you want to inspect existing table identifiers before choosing one, run:

    ```bash
    mainsequence meta-table list --filter identifier__contains=daily_random
    ```

    The `Identifier` column lists table identifiers, not row-level `unique_identifier` values.

In Pydantic v2, every `DataNodeConfiguration` field is updater-scope by
default and participates in `update_hash`.

If a field should be kept only for UI or descriptive purposes and must not
affect update identity, mark it with
`json_schema_extra={"hash_excluded": True}`.

Use `hash_excluded` only for descriptive metadata. If changing the field would
change output values, dependencies, or schema, it must remain a normal
configuration field.

### DataNode Recipe

Every `DataNode` follows the same basic recipe:

1. Extend the base class `mainsequence.meta_tables.DataNode`
2. Implement the constructor method `__init__()` and accept a registered `storage_table`
3. Implement the `dependencies()` method
4. Implement the `update()` method

#### The update() Method

The `update()` method has one hard requirement: it must return a `pandas.DataFrame`.

##### DataFrame structure requirements

- `update()` must always return a `pd.DataFrame()`
- the first index level must be named `time_index` and contain UTC-aware datetimes
- every additional index level is an identity dimension, such as `unique_identifier`,
  `account_uid`, or another stable business key
- all column names must be lowercase and no more than 63 characters long
- column types should be `float`, `int`, or `str`; date values should live in the index or be converted to numeric timestamps
- if there is new data to return, the DataFrame must contain rows; if there is no new data, return an empty `pd.DataFrame()`
- a single-index DataFrame must not contain duplicate index values; a MultiIndex DataFrame must not contain duplicate full index tuples
- `(time_index, unique_identifier)` is the standard two-index entity-table shape, but DataNodes can also use higher-dimensional indexes such as `(time_index, account_uid, unique_identifier)`
- `time_index` should be the observation point across the series in the dataset, so rows aligned on the same timestamp are comparable
- for bar data, `time_index` should usually be the right edge of the bar, not the bar start; for example, daily bars should typically use the session-close timestamp
- if dates are stored in columns, they should be represented as timestamps

#### Entity tables with more than one index

The simple node above writes one row per day, so its storage contract uses only
`["time_index"]`. Entity tables must include every column that identifies a row.

For example, an account holding row belongs to a canonical `Account`
MetaTable and is identified by:

```python
["time_index", "account_uid", "unique_identifier"]
```

That means `(2026-01-02, account-a, AAPL)` and `(2026-01-02, account-b, AAPL)`
are different rows, even though they share the same timestamp and security.
`Account` is the platform-managed parent table. `AccountHoldingsStorage` is the
time-indexed storage MetaTable. The foreign key in that MetaTable contract connects
`AccountHoldingsStorage.account_uid` to `Account.uid`, while
`PlatformTimeIndexMetaData` still uses the full `__index_names__` tuple as the
ORM identity and sends that tuple as `index_names`.

Add these models to `src/data_nodes/example_nodes.py` when your DataNode
publishes account/security observations:

```python
class Account(PlatformManagedMetaTable, Base):
    __metatable_namespace__ = "mainsequence.examples"
    __metatable_identifier__ = f"account_{PROJECT_UID}"
    __metatable_extra_hash_components__ = {"storage_name": "account"}

    uid: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True)
    account_code: Mapped[str] = mapped_column(String(64), nullable=False, unique=True)
    name: Mapped[str] = mapped_column(String(255), nullable=False)


class AccountHoldingsStorage(PlatformTimeIndexMetaData, Base):
    __table_args__ = (Index(None, "account_uid"),)
    __metatable_namespace__ = "mainsequence.examples"
    __metatable_identifier__ = f"account_holdings_{PROJECT_UID}"
    __metatable_extra_hash_components__ = {"storage_name": "account_holdings"}
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

```

Then return a `MultiIndex` DataFrame whose index names exactly match the storage
contract:

```python
class AccountHoldingsConfig(DataNodeConfiguration):
    account_uid: uuid.UUID


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
```

This node writes one account snapshot per UTC minute, so rerunning it after the
minute changes produces a new timestamp and different quantities.

Run it with the active project:

```python
import uuid

from mainsequence.meta_tables.data_nodes import hash_namespace

from src.data_nodes.example_nodes import (
    Account,
    AccountHoldingsConfig,
    AccountHoldingsSnapshot,
    AccountHoldingsStorage,
    upsert_account,
)


def main():
    account_meta_table = Account.register(
        description="Tutorial platform-managed account table.",
        labels=["tutorial", "data-node"],
    )
    AccountHoldingsStorage.register(
        description="Tutorial DataNode storage table for account holdings.",
        labels=["tutorial", "data-node"],
        target_meta_tables={Account: account_meta_table},
    )
    account_uid = uuid.UUID("00000000-0000-4000-8000-000000000001")
    upsert_account(
        account_meta_table,
        account_uid=account_uid,
        account_code="TUTORIAL",
        name="Tutorial Account",
    )

    with hash_namespace("tutorial_account_holdings"):
        node = AccountHoldingsSnapshot(
            config=AccountHoldingsConfig(account_uid=account_uid),
            storage_table=AccountHoldingsStorage,
        )
        node.run(debug_mode=True, force_update=True)


if __name__ == "__main__":
    main()
```


Next, create `scripts\random_number_launcher.py` to run the node:

```python
from src.data_nodes.example_nodes import (
    DailyRandomNumber,
    DailyRandomNumberStorage,
    RandomDataNodeConfig,
)


def main():
    DailyRandomNumberStorage.register(
        description="Tutorial DataNode storage table for daily random numbers.",
        labels=["tutorial", "data-node"],
    )

    daily_node = DailyRandomNumber(
        config=RandomDataNodeConfig(mean=0.0),
        storage_table=DailyRandomNumberStorage,
    )
    daily_node.run()


if __name__ == "__main__":
    main()
```

### Test the node with an isolated update namespace first

Before you start running a new `DataNode` repeatedly, use a namespace for the
update process while you validate the update behavior.

Why this matters:

- it isolates your first test runs from production update-process records
- it gives you a safe way to validate schema and update behavior
- it keeps experimentation separate from production-like update identity

Register the storage model first, then use `hash_namespace(...)` while you are
developing or testing:

```python
from mainsequence.meta_tables.data_nodes import hash_namespace

from src.data_nodes.example_nodes import (
    DailyRandomNumber,
    DailyRandomNumberStorage,
    RandomDataNodeConfig,
)


def main():
    DailyRandomNumberStorage.register(
        description="Tutorial DataNode storage table for daily random numbers.",
        labels=["tutorial", "data-node"],
    )

    with hash_namespace("tutorial_daily_random_number"):
        daily_node = DailyRandomNumber(
            config=RandomDataNodeConfig(mean=0.0),
            storage_table=DailyRandomNumberStorage,
        )
        daily_node.run(debug_mode=True, force_update=True)


if __name__ == "__main__":
    main()
```

This should be your default habit when you are validating a new node for the
first time. The storage model remains the canonical table contract, and the hash
namespace isolates the update-process identity.

For real projects, also keep a small smoke test under `tests/`, for example `tests/test_daily_random_number.py`:

```python
from mainsequence.meta_tables.data_nodes import hash_namespace

from src.data_nodes.example_nodes import (
    DailyRandomNumber,
    DailyRandomNumberStorage,
    RandomDataNodeConfig,
)


def test_daily_random_number_smoke():
    DailyRandomNumberStorage.register(
        description="Test storage table for daily random numbers.",
        labels=["test", "data-node"],
    )

    with hash_namespace("pytest_daily_random_number_smoke"):
        node = DailyRandomNumber(
            config=RandomDataNodeConfig(mean=0.0),
            storage_table=DailyRandomNumberStorage,
        )
        err, df = node.run(debug_mode=True, force_update=True)

    assert err is False
    assert df is not None
```

Once that test run behaves as expected, you can run the same node without the
update namespace when you are ready to publish or share the real dataset.

Run the launcher directly from the terminal:

```bash
python scripts/random_number_launcher.py
```

If your shell uses `python3` instead of `python`, run:

```bash
python3 scripts/random_number_launcher.py
```

### Verify From the CLI

Confirm that the launcher created update records:

```bash
mainsequence project data-node-updates list
```

Then locate the published table by its identifier:

```bash
mainsequence meta-table list --filter identifier__contains=daily_random_number
```

If you want the full table record, inspect it directly:

```bash
mainsequence meta-table detail <META_TABLE_UID>
```

If your local project auth has expired or your `.env` does not yet contain fresh project JWTs, refresh them first:

```bash
mainsequence project refresh_token --path .
```

The CLI output lists the update ID, update hash, data node storage, and update details for the current project. Run it again after `random_daily_addition_launcher.py` or after the updated `random_number_launcher.py` to confirm that additional update processes were created.

### Add a Dependent Data Node

Now extend the workflow with a node that depends on `DailyRandomNumber`. Add the following to `src\data_nodes\example_nodes.py`:

```python
class DailyRandomAdditionConfig(DataNodeConfiguration):
    mean: float
    std: float


class DailyRandomAddition(DataNode):
    def __init__(
        self,
        config: DailyRandomAdditionConfig,
        storage_table: type[PlatformTimeIndexMetaData],
        daily_random_number_storage_table: type[PlatformTimeIndexMetaData],
        *,
        hash_namespace: str | None = None,
    ):
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
```

This adds a **dependent** node, `DailyRandomAddition`, that reads the output of `DailyRandomNumber` and uses it in its own update logic.

Create a launcher at `scripts\random_daily_addition_launcher.py`:

```python
from src.data_nodes.example_nodes import (
    DailyRandomAddition,
    DailyRandomAdditionConfig,
    DailyRandomAdditionStorage,
    DailyRandomNumberStorage,
)


DailyRandomNumberStorage.register(
    description="Tutorial DataNode storage table for daily random numbers.",
    labels=["tutorial", "data-node"],
)
DailyRandomAdditionStorage.register(
    description="Tutorial DataNode storage table for daily random additions.",
    labels=["tutorial", "data-node"],
)


daily_node = DailyRandomAddition(
    config=DailyRandomAdditionConfig(mean=0.0, std=1.0),
    storage_table=DailyRandomAdditionStorage,
    daily_random_number_storage_table=DailyRandomNumberStorage,
)
daily_node.run(debug_mode=True, force_update=True)
```

Run the new launcher from the terminal:

```bash
python scripts/random_daily_addition_launcher.py
```

If your shell uses `python3`, run:

```bash
python3 scripts/random_daily_addition_launcher.py
```

Both tutorial storage tables have friendly identifiers because their
`PlatformTimeIndexMetaData` classes were registered before the DataNodes were
constructed. Use
`mainsequence project data-node-updates list` for update records and
`mainsequence meta-table list --filter identifier__contains=daily_random`
for the backing tables.

The important thing to verify here is that the dependent node ran successfully and created a new update process in the current project.

## 4. DataNode Update Identity And MetaTable Storage

A `PlatformTimeIndexMetaData` class is the storage contract for a DataNode
table. A `DataNode` is the update process that produces or refreshes data for
that MetaTable-backed table.

Those concerns are intentionally separate:

- the table contract says where data lives, what columns exist, and who can read or write it
- the update process says which code and updater-scope configuration produced a particular run

During the migration to MetaTable-owned storage, some CLI and API fields still
surface legacy names such as `update_hash` and `storage_hash`. Treat
`update_hash` as the update-process identity, and treat storage identity as the
MetaTable-backed table contract.

Why separate them? Sometimes you want multiple updater configurations to publish
into the same table contract. While the simple example here is contrived, this
pattern becomes useful with multi-index and dimensional tables.

Now update your **daily random number launcher** to run two update processes with different volatility configurations but the **same** table contract.

To do this, modify `scripts\random_number_launcher.py` to be as follows:

```python
from src.data_nodes.example_nodes import (
    DailyRandomNumber,
    DailyRandomNumberStorage,
    RandomDataNodeConfig,
    VolatilityConfig,
)

low_vol = VolatilityConfig(center=0.5, skew=False)
high_vol = VolatilityConfig(center=2.0, skew=True)
DailyRandomNumberStorage.register(
    description="Tutorial DataNode storage table for daily random numbers.",
    labels=["tutorial", "data-node"],
)


daily_node_low = DailyRandomNumber(
    config=RandomDataNodeConfig(mean=0.0, std=low_vol),
    storage_table=DailyRandomNumberStorage,
)
daily_node_high = DailyRandomNumber(
    config=RandomDataNodeConfig(mean=0.0, std=high_vol),
    storage_table=DailyRandomNumberStorage,
)

daily_node_low.run(debug_mode=True, force_update=True)
daily_node_high.run(debug_mode=True, force_update=True)
```

Here we create two `DailyRandomNumber` nodes with different `std` (Volatility)
configurations but the same `storage_table`. Both nodes write to the same table
contract while keeping separate update-process identities. The tutorial table
identifier stays stable because it comes from the registered
`PlatformTimeIndexMetaData` class, not from `std`.

Run the updated launcher from the terminal as before:

```bash
python scripts/random_number_launcher.py
```

If your shell uses `python3`, run:

```bash
python3 scripts/random_number_launcher.py
```

Then inspect the result from the CLI:

```bash
mainsequence project data-node-updates list
mainsequence meta-table list --filter identifier__contains=daily_random_number
```

You should see that you still have one tutorial table identifier, but additional update processes were created for the different updater configurations.


You can also monitor the data nodes updates via the cli by running:
```shell
mainsequence project data-node-updates list

                                    Project Data Node Updates                                     
                                                                                                  
  ID     Update Hash                                          Data Node Storage   Update Details  
 ──────────────────────────────────────────────────────────────────────────────────────────────── 
  8005   dailyrandomnumber_009e3dfd8059e97933414c8e54b13af1   5016                -               
  8004   dailyrandomnumber_f32b575aa53142a50fa10c2fbff4d658   5016                -     

```

At this point, you have built your first `DataNode`s in Main Sequence. In the next part of the tutorial, you will move from local execution to shared access control and then to orchestration.

For further reference on DataNode concepts and best practices, see [Data Nodes Knowledge Guide](../knowledge/data_nodes.md).
