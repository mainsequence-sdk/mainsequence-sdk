import datetime
import json
from typing import Annotated

from pydantic import Field

from mainsequence.tdag.simple_tables import (
    ForeignKey,
    Index,
    Ops,
    SimpleTable,
    SimpleTableUpdater,
    SimpleTableUpdaterConfiguration,
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


def build_test_simple_tables() -> None:
    """
    Requires a configured MainSequence backend/auth environment.
    """

    balances_updater = CustomerBalancesUpdater(configuration=CustomerBalancesUpdaterConfiguration())
    inserted_balances = balances_updater.run()

    typed_filter = CustomerBalanceRecord.filters.balance_usd.gte(100_000.0)
    request_payload = CustomerBalanceRecord.request(
        node_unique_identifier="customer-balances-demo",
        filter=typed_filter,
        limit=25,
    ).model_dump(mode="json")

    print("Balance configuration hashes:")
    print(
        {
            "update_hash": balances_updater.hashes()[0],
            "storage_hash": balances_updater.hashes()[1],
        }
    )
    print("Typed filter request payload:")
    print(json.dumps(request_payload, indent=2, default=str))
    print("Inserted or updated balance rows:")
    print(inserted_balances)


if __name__ == "__main__":
    build_test_simple_tables()
