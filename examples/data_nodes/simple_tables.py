import datetime
import json
from typing import Annotated

from pydantic import Field

from mainsequence.client import SimpleTableStorage
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


class CustomerDebtRecord(SimpleTable):
    balance_id: Annotated[
        int,
        ForeignKey(CustomerBalanceRecord, on_delete="cascade"),
        Index(),
        Ops(filter=True),
    ] = Field(
        ...,
        title="Balance Id",
        description="Foreign key to the balance snapshot row.",
    )
    debt_type: Annotated[str, Ops(filter=True, order=True)] = Field(
        ...,
        title="Debt Type",
        description="Debt category associated with the balance row.",
    )
    debt_usd: Annotated[float, Ops(filter=True, order=True)] = Field(
        ...,
        title="Debt USD",
        description="Debt amount in USD.",
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


class CustomerDebtsUpdaterConfiguration(SimpleTableUpdaterConfiguration):
    run_label: str = Field(
        default="Simple-table debt seed",
        description="Human-readable run label for logs and demos.",
        json_schema_extra={"runtime_only": True},
    )


class CustomersUpdater(SimpleTableUpdater):
    SIMPLE_TABLE_SCHEMA = CustomerRecord

    @staticmethod
    def build_seed_rows() -> list[dict[str, object]]:
        return [
            {"customer_code": "ACME", "name": "Acme Capital", "region": "US"},
            {"customer_code": "BETA", "name": "Beta Treasury", "region": "EU"},
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
        customers_updater: CustomersUpdater | None = None,
        hash_namespace: str | None = None,
        test_node: bool = False,
    ):
        self.customers_updater = customers_updater or CustomersUpdater(
            configuration=CustomersUpdaterConfiguration()
        )
        super().__init__(
            configuration=configuration,
            hash_namespace=hash_namespace,
            test_node=test_node,
        )

    @staticmethod
    def build_seed_spec() -> list[dict[str, object]]:
        return [
            {
                "customer_code": "ACME",
                "as_of_date": datetime.date(2026, 3, 22),
                "balance_usd": 125_000.0,
            },
            {
                "customer_code": "BETA",
                "as_of_date": datetime.date(2026, 3, 22),
                "balance_usd": 85_500.0,
            },
        ]

    @classmethod
    def build_seed_rows(
        cls,
        *,
        customer_id_by_code: dict[str, int],
    ) -> list[dict[str, object]]:
        return [
            {
                "customer_id": customer_id_by_code[row["customer_code"]],
                "as_of_date": row["as_of_date"],
                "balance_usd": row["balance_usd"],
            }
            for row in cls.build_seed_spec()
        ]

    def update(self) -> tuple[list[CustomerBalanceRecord], bool]:
        customer_codes = [
            row["customer_code"]
            for row in self.build_seed_spec()
        ]
        customer_rows = self.customers_updater.execute_filter(
            CustomerRecord.filters.customer_code.in_(customer_codes),
            limit=len(customer_codes),
        )
        customer_id_by_code = {
            row.customer_code: row.id
            for row in customer_rows
            if row.id is not None
        }
        return (
            [
                CustomerBalanceRecord.model_validate(row)
                for row in self.build_seed_rows(customer_id_by_code=customer_id_by_code)
            ],
            True,
        )

    def dependencies(self) -> dict[str, SimpleTableUpdater]:
        return {"customers": self.customers_updater}


class CustomerDebtsUpdater(SimpleTableUpdater):
    SIMPLE_TABLE_SCHEMA = CustomerDebtRecord

    def __init__(
        self,
        configuration: CustomerDebtsUpdaterConfiguration,
        *,
        balances_updater: CustomerBalancesUpdater | None = None,
        hash_namespace: str | None = None,
        test_node: bool = False,
    ):
        self.balances_updater = balances_updater or CustomerBalancesUpdater(
            configuration=CustomerBalancesUpdaterConfiguration()
        )
        super().__init__(
            configuration=configuration,
            hash_namespace=hash_namespace,
            test_node=test_node,
        )

    @staticmethod
    def build_seed_spec() -> list[dict[str, object]]:
        return [
            {
                "customer_code": "ACME",
                "as_of_date": datetime.date(2026, 3, 22),
                "debt_type": "margin",
                "debt_usd": 15_000.0,
            },
            {
                "customer_code": "BETA",
                "as_of_date": datetime.date(2026, 3, 22),
                "debt_type": "fx_credit",
                "debt_usd": 6_500.0,
            },
        ]

    @classmethod
    def build_seed_rows(
        cls,
        *,
        customer_id_by_code: dict[str, int],
        balance_id_by_customer_and_date: dict[tuple[int, datetime.date], int],
    ) -> list[dict[str, object]]:
        rows: list[dict[str, object]] = []
        for row in cls.build_seed_spec():
            customer_id = customer_id_by_code[row["customer_code"]]
            balance_id = balance_id_by_customer_and_date[(customer_id, row["as_of_date"])]
            rows.append(
                {
                    "balance_id": balance_id,
                    "debt_type": row["debt_type"],
                    "debt_usd": row["debt_usd"],
                }
            )
        return rows

    def update(self) -> tuple[list[CustomerDebtRecord], bool]:
        debt_specs = self.build_seed_spec()
        customer_codes = [row["customer_code"] for row in debt_specs]
        customer_rows = self.balances_updater.customers_updater.execute_filter(
            CustomerRecord.filters.customer_code.in_(customer_codes),
            limit=len(customer_codes),
        )
        customer_id_by_code = {
            row.customer_code: row.id
            for row in customer_rows
            if row.id is not None
        }
        customer_ids = list(customer_id_by_code.values())
        as_of_dates = sorted({row["as_of_date"] for row in debt_specs})
        balance_rows = self.balances_updater.execute_filter(
            and_(
                CustomerBalanceRecord.filters.customer_id.in_(customer_ids),
                CustomerBalanceRecord.filters.as_of_date.in_(as_of_dates),
            ),
            limit=max(len(customer_ids), 1) * max(len(as_of_dates), 1),
        )
        balance_id_by_customer_and_date = {
            (row.customer_id, row.as_of_date): row.id
            for row in balance_rows
            if row.id is not None
        }
        return (
            [
                CustomerDebtRecord.model_validate(row)
                for row in self.build_seed_rows(
                    customer_id_by_code=customer_id_by_code,
                    balance_id_by_customer_and_date=balance_id_by_customer_and_date,
                )
            ],
            True,
        )

    def dependencies(self) -> dict[str, SimpleTableUpdater]:
        return {"balances": self.balances_updater}


def _read_simple_table_records(
    updater: SimpleTableUpdater,
    filter_expr=None,
    limit: int = 250,
) -> list[SimpleTable]:
    return updater.execute_filter(
        filter_expr,
        limit=limit,
        offset=0,
    )


def _records_as_json(
    records: list[SimpleTable],
    *,
    include_id: bool = False,
) -> list[dict[str, object]]:
    normalized_records: list[dict[str, object]] = []
    for record in records:
        dumped = record.model_dump(mode="json")
        if not include_id:
            dumped.pop("id", None)
        normalized_records.append(dumped)
    return sorted(normalized_records, key=lambda item: json.dumps(item, sort_keys=True))


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
    *,
    filter_expr,
) -> None:
    if filter_expr is None:
        return
    existing_records = _read_simple_table_records(
        updater,
        filter_expr=filter_expr,
    )
    for record in existing_records:
        updater.delete(record.id)


def _require_record_ids(
    records: list[SimpleTable],
    *,
    step_name: str,
) -> list[int]:
    record_ids = [record.id for record in records if record.id is not None]
    if len(record_ids) != len(records):
        raise AssertionError(f"{step_name} failed because one or more rows did not include a backend id.")
    return record_ids


def build_test_simple_tables() -> None:
    """
    Requires a configured MainSequence backend/auth environment.
    """

    customers_updater = CustomersUpdater(configuration=CustomersUpdaterConfiguration())
    balances_updater = CustomerBalancesUpdater(
        configuration=CustomerBalancesUpdaterConfiguration(),
        customers_updater=customers_updater,
    )
    debts_updater = CustomerDebtsUpdater(
        configuration=CustomerDebtsUpdaterConfiguration(),
        balances_updater=balances_updater,
    )

    customer_seed = [
        CustomerRecord.model_validate(row)
        for row in CustomersUpdater.build_seed_rows()
    ]
    balance_specs = CustomerBalancesUpdater.build_seed_spec()
    debt_specs = CustomerDebtsUpdater.build_seed_spec()
    customer_codes = [record.customer_code for record in customer_seed]
    balance_dates = sorted({row["as_of_date"] for row in balance_specs})
    debt_types = sorted({row["debt_type"] for row in debt_specs})

    # Keep the example repeatable across runs by removing the specific demo rows first.
    existing_customers = _read_simple_table_records(
        customers_updater,
        filter_expr=CustomerRecord.filters.customer_code.in_(customer_codes),
    )
    existing_customer_ids = _require_record_ids(
        existing_customers,
        step_name="Existing customer lookup",
    ) if existing_customers else []
    existing_balances = (
        _read_simple_table_records(
            balances_updater,
            filter_expr=and_(
                CustomerBalanceRecord.filters.customer_id.in_(existing_customer_ids),
                CustomerBalanceRecord.filters.as_of_date.in_(balance_dates),
            ),
        )
        if existing_customer_ids
        else []
    )
    existing_balance_ids = _require_record_ids(
        existing_balances,
        step_name="Existing balance lookup",
    ) if existing_balances else []
    _delete_records_if_present(
        debts_updater,
        filter_expr=and_(
            CustomerDebtRecord.filters.balance_id.in_(existing_balance_ids),
            CustomerDebtRecord.filters.debt_type.in_(debt_types),
        ) if existing_balance_ids else None,
    )
    _delete_records_if_present(
        balances_updater,
        filter_expr=and_(
            CustomerBalanceRecord.filters.customer_id.in_(existing_customer_ids),
            CustomerBalanceRecord.filters.as_of_date.in_(balance_dates),
        ) if existing_customer_ids else None,
    )
    _delete_records_if_present(
        customers_updater,
        filter_expr=CustomerRecord.filters.customer_code.in_(customer_codes),
    )

    for record in customer_seed:
        customers_updater.insert(record)

    inserted_customers = _read_simple_table_records(
        customers_updater,
        filter_expr=CustomerRecord.filters.customer_code.in_(customer_codes),
    )
    _assert_records_equal(
        actual=inserted_customers,
        expected=customer_seed,
        step_name="Customer insert verification",
    )
    customer_id_by_code = {
        record.customer_code: record.id
        for record in inserted_customers
        if record.id is not None
    }
    balance_seed = [
        CustomerBalanceRecord.model_validate(row)
        for row in CustomerBalancesUpdater.build_seed_rows(customer_id_by_code=customer_id_by_code)
    ]

    for record in balance_seed:
        balances_updater.insert(record)

    inserted_balances = _read_simple_table_records(
        balances_updater,
        filter_expr=and_(
            CustomerBalanceRecord.filters.customer_id.in_(list(customer_id_by_code.values())),
            CustomerBalanceRecord.filters.as_of_date.in_(balance_dates),
        ),
    )
    _assert_records_equal(
        actual=inserted_balances,
        expected=balance_seed,
        step_name="Balance insert verification",
    )
    balance_rows_with_ids = {
        (record.customer_id, record.as_of_date): record.id
        for record in inserted_balances
        if record.id is not None
    }
    debt_seed = [
        CustomerDebtRecord.model_validate(row)
        for row in CustomerDebtsUpdater.build_seed_rows(
            customer_id_by_code=customer_id_by_code,
            balance_id_by_customer_and_date=balance_rows_with_ids,
        )
    ]
    balance_ids = _require_record_ids(inserted_balances, step_name="Inserted balance verification")

    for record in debt_seed:
        debts_updater.insert(record)

    inserted_debts = _read_simple_table_records(
        debts_updater,
        filter_expr=and_(
            CustomerDebtRecord.filters.balance_id.in_(balance_ids),
            CustomerDebtRecord.filters.debt_type.in_(debt_types),
        ),
    )
    _assert_records_equal(
        actual=inserted_debts,
        expected=debt_seed,
        step_name="Debt insert verification",
    )
    inserted_balance_by_customer_id = {
        record.customer_id: record
        for record in inserted_balances
        if record.id is not None
    }

    SimpleTableStorage.upsert_records_into_table(
        simple_table_id=balances_updater.data_node_storage.id,
        records=[
            {"id": inserted_balance_by_customer_id[customer_id_by_code["ACME"]].id, "balance_usd": 150_000.0},
            {"id": inserted_balance_by_customer_id[customer_id_by_code["BETA"]].id, "balance_usd": 91_250.0},
        ],
    )

    upserted_balance_seed = [
        CustomerBalanceRecord(
            customer_id=customer_id_by_code["ACME"],
            as_of_date=datetime.date(2026, 3, 22),
            balance_usd=150_000.0,
        ),
        CustomerBalanceRecord(
            customer_id=customer_id_by_code["BETA"],
            as_of_date=datetime.date(2026, 3, 22),
            balance_usd=91_250.0,
        ),
    ]
    upserted_balances = _read_simple_table_records(
        balances_updater,
        filter_expr=and_(
            CustomerBalanceRecord.filters.customer_id.in_(list(customer_id_by_code.values())),
            CustomerBalanceRecord.filters.as_of_date.in_(balance_dates),
        ),
    )
    _assert_records_equal(
        actual=upserted_balances,
        expected=upserted_balance_seed,
        step_name="Balance upsert verification",
    )

    for record in inserted_debts:
        debts_updater.delete(record.id)

    remaining_debts = _read_simple_table_records(
        debts_updater,
        filter_expr=and_(
            CustomerDebtRecord.filters.balance_id.in_(balance_ids),
            CustomerDebtRecord.filters.debt_type.in_(debt_types),
        ),
    )
    if remaining_debts:
        raise AssertionError(
            "Debt delete verification failed.\nRemaining rows:\n"
            + json.dumps(_records_as_json(remaining_debts, include_id=True), indent=2)
        )

    for record in upserted_balances:
        balances_updater.delete(record.id)

    remaining_balances = _read_simple_table_records(
        balances_updater,
        filter_expr=and_(
            CustomerBalanceRecord.filters.customer_id.in_(list(customer_id_by_code.values())),
            CustomerBalanceRecord.filters.as_of_date.in_(balance_dates),
        ),
    )
    if remaining_balances:
        raise AssertionError(
            "Balance delete verification failed.\nRemaining rows:\n"
            + json.dumps(_records_as_json(remaining_balances, include_id=True), indent=2)
        )

    for record in inserted_customers:
        customers_updater.delete(record.id)

    remaining_customers = _read_simple_table_records(
        customers_updater,
        filter_expr=CustomerRecord.filters.customer_code.in_(customer_codes),
    )
    if remaining_customers:
        raise AssertionError(
            "Customer delete verification failed.\nRemaining rows:\n"
            + json.dumps(_records_as_json(remaining_customers, include_id=True), indent=2)
        )

    resolved_balances = balances_updater.resolve_table()
    resolved_customers = customers_updater.resolve_table()
    resolved_debts = debts_updater.resolve_table()
    typed_filter = CustomerBalanceRecord.filters.balance_usd.gte(100_000.0)
    request_payload = resolved_balances.request(
        filter=typed_filter,
        limit=25,
    ).model_dump(mode="json")
    customer_join = resolved_balances.join("customer", target=resolved_customers)
    debt_join = resolved_balances.join("debt", target=resolved_debts)
    complex_filter = and_(
        CustomerBalanceRecord.filters.as_of_date.between(
            datetime.date(2026, 3, 1),
            datetime.date(2026, 3, 31),
        ),
        or_(
            customer_join.filters.region.eq("US"),
            debt_join.filters.debt_type.eq("margin"),
        ),
        or_(
            debt_join.filters.debt_usd.gte(10_000.0),
            CustomerBalanceRecord.filters.balance_usd.gte(140_000.0),
        ),
    )
    complex_request_payload = resolved_balances.request(
        joins=[customer_join, debt_join],
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
        "debts": {
            "update_hash": debts_updater.hashes()[0],
            "storage_hash": debts_updater.hashes()[1],
        },
    })
    print("Typed filter request payload:")
    print(json.dumps(request_payload, indent=2, default=str))
    print("Complex filter request payload:")
    print(json.dumps(complex_request_payload, indent=2, default=str))
    print("Inserted customer rows:")
    print(json.dumps(_records_as_json(inserted_customers, include_id=True), indent=2))
    print("Inserted balance rows:")
    print(json.dumps(_records_as_json(inserted_balances, include_id=True), indent=2))
    print("Inserted debt rows:")
    print(json.dumps(_records_as_json(inserted_debts, include_id=True), indent=2))
    print("Upserted balance rows:")
    print(json.dumps(_records_as_json(upserted_balances, include_id=True), indent=2))
    print("Delete verification passed for customer, balance, and debt demo rows.")


if __name__ == "__main__":
    build_test_simple_tables()
