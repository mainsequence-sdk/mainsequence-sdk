import datetime
import json
from typing import Annotated

from pydantic import Field

from mainsequence.client import DataNodeStorage, SimpleTableStorage
from mainsequence.tdag.simple_tables import (
    ForeignKey,
    Index,
    Ops,
    SimpleTable,
    SimpleTableUpdater,
    SimpleTableUpdaterConfiguration,
    and_,
    or_,
)


class CustomerRecord(SimpleTable):
    id: int
    customer_code: Annotated[str, Index(unique=True), Ops(filter=True, order=True)] = Field(
        ...,
        title="Customer Code",
        description="Stable customer identifier.",
    )
    name: Annotated[str, Ops(filter=True, order=True)] = Field(
        ...,
        title="Name",
        description="Human-readable customer name.",
    )
    region: Annotated[str, Index(), Ops(filter=True)] = Field(
        ...,
        title="Region",
        description="Commercial region for the customer.",
    )


class CustomerBalanceRecord(SimpleTable):
    id: int
    customer_id: Annotated[
        int,
        ForeignKey(CustomerRecord, on_delete="cascade"),
        Index(),
        Ops(filter=True),
    ] = Field(
        ...,
        title="Customer Id",
        description="Foreign key to the customer table.",
    )
    as_of_date: Annotated[datetime.date, Ops(filter=True, order=True)] = Field(
        ...,
        title="As Of Date",
        description="Balance snapshot date.",
    )
    balance_usd: Annotated[float, Ops(filter=True, order=True)] = Field(
        ...,
        title="Balance USD",
        description="Customer balance in USD.",
    )


class CustomersUpdaterConfiguration(SimpleTableUpdaterConfiguration):
    run_label: str = Field(
        default="Simple-table customer seed",
        description="Human-readable run label for logs and demos.",
        json_schema_extra={"runtime_only": True},
    )


class CustomerBalancesUpdaterConfiguration(SimpleTableUpdaterConfiguration):
    run_label: str = Field(
        default="Simple-table balance seed",
        description="Human-readable run label for logs and demos.",
        json_schema_extra={"runtime_only": True},
    )


class CustomersUpdater(SimpleTableUpdater):
    SIMPLE_TABLE_SCHEMA = CustomerRecord

    @staticmethod
    def build_seed_rows() -> list[dict[str, object]]:
        return [
            {"id": 100, "customer_code": "ACME", "name": "Acme Capital", "region": "US"},
            {"id": 101, "customer_code": "BETA", "name": "Beta Treasury", "region": "EU"},
        ]

    def update(self) -> tuple[list[CustomerRecord], bool]:
        return (
            [CustomerRecord.model_validate(row) for row in self.build_seed_rows()],
            True,
        )


class CustomerBalancesUpdater(SimpleTableUpdater):
    SIMPLE_TABLE_SCHEMA = CustomerBalanceRecord

    def __init__(
        self,
        configuration: CustomerBalancesUpdaterConfiguration,
        *,
        hash_namespace: str | None = None,
        test_node: bool = False,
    ):
        self.customers_updater = CustomersUpdater(configuration=CustomersUpdaterConfiguration())
        super().__init__(
            configuration=configuration,
            hash_namespace=hash_namespace,
            test_node=test_node,
        )

    @staticmethod
    def build_seed_rows() -> list[dict[str, object]]:
        return [
            {
                "id": 1,
                "customer_id": 100,
                "as_of_date": datetime.date(2026, 3, 22),
                "balance_usd": 125_000.0,
            },
            {
                "id": 2,
                "customer_id": 101,
                "as_of_date": datetime.date(2026, 3, 22),
                "balance_usd": 85_500.0,
            },
        ]

    def update(self) -> tuple[list[CustomerBalanceRecord], bool]:
        return (
            [CustomerBalanceRecord.model_validate(row) for row in self.build_seed_rows()],
            True,
        )

    def dependencies(self) -> dict[str, SimpleTableUpdater]:
        return {"customers": self.customers_updater}


def _read_simple_table_records(
    table_model: type[SimpleTable],
    *,
    storage_hash: str,
    filter_expr=None,
    limit: int = 250,
) -> list[SimpleTable]:
    request = table_model.request(
        storage_hash=storage_hash,
        filter=filter_expr,
        limit=limit,
    )
    df = DataNodeStorage.get_data_from_filter(request, batch_limit=limit)
    if df.empty:
        return []

    df = df.reset_index(drop=False)
    rename_map = {
        column: column.removeprefix("base__")
        for column in df.columns
        if column.startswith("base__")
    }
    if rename_map:
        df = df.rename(columns=rename_map)

    model_fields = set(table_model.model_fields.keys())
    records: list[SimpleTable] = []
    for row in df.to_dict(orient="records"):
        normalized = {key: value for key, value in row.items() if key in model_fields}
        records.append(table_model.model_validate(normalized))
    return records


def _records_as_json(records: list[SimpleTable]) -> list[dict[str, object]]:
    return [
        record.model_dump(mode="json")
        for record in sorted(records, key=lambda item: item.id)
    ]


def _assert_records_equal(
    *,
    actual: list[SimpleTable],
    expected: list[SimpleTable],
    step_name: str,
) -> None:
    actual_json = _records_as_json(actual)
    expected_json = _records_as_json(expected)
    if actual_json != expected_json:
        raise AssertionError(
            f"{step_name} failed.\nExpected:\n{json.dumps(expected_json, indent=2)}\n"
            f"Actual:\n{json.dumps(actual_json, indent=2)}"
        )


def _delete_records_if_present(
    updater: SimpleTableUpdater,
    table_model: type[SimpleTable],
    *,
    storage_hash: str,
    record_ids: list[int],
) -> None:
    existing_records = _read_simple_table_records(
        table_model,
        storage_hash=storage_hash,
        filter_expr=table_model.filters.id.in_(record_ids),
    )
    for record in existing_records:
        updater.delete(record.id)


def build_test_simple_tables() -> None:
    """
    Requires a configured MainSequence backend/auth environment.
    """

    balances_updater = CustomerBalancesUpdater(configuration=CustomerBalancesUpdaterConfiguration())
    customers_updater = balances_updater.customers_updater

    customers_updater.verify_and_build_remote_objects()
    balances_updater.verify_and_build_remote_objects()

    customer_seed = [
        CustomerRecord.model_validate(row)
        for row in CustomersUpdater.build_seed_rows()
    ]
    balance_seed = [
        CustomerBalanceRecord.model_validate(row)
        for row in CustomerBalancesUpdater.build_seed_rows()
    ]

    customer_storage_hash = customers_updater.hashes()[1]
    balance_storage_hash = balances_updater.hashes()[1]

    # Keep the example repeatable across runs by removing the specific demo rows first.
    _delete_records_if_present(
        balances_updater,
        CustomerBalanceRecord,
        storage_hash=balance_storage_hash,
        record_ids=[record.id for record in balance_seed],
    )
    _delete_records_if_present(
        customers_updater,
        CustomerRecord,
        storage_hash=customer_storage_hash,
        record_ids=[record.id for record in customer_seed],
    )

    for record in customer_seed:
        customers_updater.insert(record)

    inserted_customers = _read_simple_table_records(
        CustomerRecord,
        storage_hash=customer_storage_hash,
        filter_expr=CustomerRecord.filters.id.in_([record.id for record in customer_seed]),
    )
    _assert_records_equal(
        actual=inserted_customers,
        expected=customer_seed,
        step_name="Customer insert verification",
    )

    for record in balance_seed:
        balances_updater.insert(record)

    inserted_balances = _read_simple_table_records(
        CustomerBalanceRecord,
        storage_hash=balance_storage_hash,
        filter_expr=CustomerBalanceRecord.filters.id.in_([record.id for record in balance_seed]),
    )
    _assert_records_equal(
        actual=inserted_balances,
        expected=balance_seed,
        step_name="Balance insert verification",
    )

    SimpleTableStorage.upsert_records_into_table(
        simple_table_id=balances_updater.data_node_storage.id,
        records=[
            {"id": 1, "balance_usd": 150_000.0},
            {"id": 2, "balance_usd": 91_250.0},
        ],
    )

    upserted_balance_seed = [
        CustomerBalanceRecord(
            id=1,
            customer_id=100,
            as_of_date=datetime.date(2026, 3, 22),
            balance_usd=150_000.0,
        ),
        CustomerBalanceRecord(
            id=2,
            customer_id=101,
            as_of_date=datetime.date(2026, 3, 22),
            balance_usd=91_250.0,
        ),
    ]
    upserted_balances = _read_simple_table_records(
        CustomerBalanceRecord,
        storage_hash=balance_storage_hash,
        filter_expr=CustomerBalanceRecord.filters.id.in_([1, 2]),
    )
    _assert_records_equal(
        actual=upserted_balances,
        expected=upserted_balance_seed,
        step_name="Balance upsert verification",
    )

    for record in upserted_balance_seed:
        balances_updater.delete(record.id)

    remaining_balances = _read_simple_table_records(
        CustomerBalanceRecord,
        storage_hash=balance_storage_hash,
        filter_expr=CustomerBalanceRecord.filters.id.in_([record.id for record in upserted_balance_seed]),
    )
    if remaining_balances:
        raise AssertionError(
            "Balance delete verification failed.\nRemaining rows:\n"
            + json.dumps(_records_as_json(remaining_balances), indent=2)
        )

    for record in customer_seed:
        customers_updater.delete(record.id)

    remaining_customers = _read_simple_table_records(
        CustomerRecord,
        storage_hash=customer_storage_hash,
        filter_expr=CustomerRecord.filters.id.in_([record.id for record in customer_seed]),
    )
    if remaining_customers:
        raise AssertionError(
            "Customer delete verification failed.\nRemaining rows:\n"
            + json.dumps(_records_as_json(remaining_customers), indent=2)
        )

    typed_filter = CustomerBalanceRecord.filters.balance_usd.gte(100_000.0)
    request_payload = CustomerBalanceRecord.request(
        storage_hash=balance_storage_hash,
        filter=typed_filter,
        limit=25,
    ).model_dump(mode="json")
    complex_filter = and_(
        CustomerBalanceRecord.filters.as_of_date.between(
            datetime.date(2026, 3, 1),
            datetime.date(2026, 3, 31),
        ),
        or_(
            CustomerBalanceRecord.filters.customer_id.in_([100, 101, 102]),
            CustomerBalanceRecord.filters.balance_usd.gte(250_000.0),
        ),
        CustomerBalanceRecord.filters.balance_usd.not_in([0.0]),
    )
    complex_request_payload = CustomerBalanceRecord.request(
        storage_hash=balance_storage_hash,
        filter=complex_filter,
        limit=25,
    ).model_dump(mode="json")

    print("Configuration hashes:")
    print({
        "customers": {
            "update_hash": customers_updater.hashes()[0],
            "storage_hash": customers_updater.hashes()[1],
        },
        "balances": {
            "update_hash": balances_updater.hashes()[0],
            "storage_hash": balances_updater.hashes()[1],
        },
    })
    print("Typed filter request payload:")
    print(json.dumps(request_payload, indent=2, default=str))
    print("Complex filter request payload:")
    print(json.dumps(complex_request_payload, indent=2, default=str))
    print("Inserted customer rows:")
    print(json.dumps(_records_as_json(inserted_customers), indent=2))
    print("Inserted balance rows:")
    print(json.dumps(_records_as_json(inserted_balances), indent=2))
    print("Upserted balance rows:")
    print(json.dumps(_records_as_json(upserted_balances), indent=2))
    print("Delete verification passed for customer and balance demo rows.")


if __name__ == "__main__":
    build_test_simple_tables()
