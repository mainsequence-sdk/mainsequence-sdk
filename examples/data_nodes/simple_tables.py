import datetime
import os
from typing import Annotated

import pandas as pd
from pydantic import Field

from mainsequence.tdag.simple_tables import (
    BaseNode,
    Index,
    SimpleTable,
    SimpleTableNode,
    SimpleTableNodeConfiguration,
)

PROJECT_ID = os.getenv("MAIN_SEQUENCE_PROJECT_ID", "local").strip() or "local"


class InMemorySimpleTableBackend:
    def __init__(self):
        self._records_by_key: dict[tuple[object, ...], SimpleTable] = {}
        self._records_no_index: list[SimpleTable] = []

    def upsert(
        self,
        records: list[SimpleTable],
        *,
        index_fields: list[str],
    ) -> None:
        if index_fields:
            for record in records:
                payload = record.model_dump(mode="python")
                key = tuple(payload[field_name] for field_name in index_fields)
                self._records_by_key[key] = record
            return
        self._records_no_index.extend(records)

    def list_records(self) -> list[SimpleTable]:
        if self._records_by_key:
            return list(self._records_by_key.values())
        return list(self._records_no_index)

    def to_frame(self) -> pd.DataFrame:
        records = self.list_records()
        if not records:
            return pd.DataFrame()
        return pd.DataFrame([record.model_dump(mode="python") for record in records])


class ClientOrderRecord(SimpleTable):
    order_id: Annotated[str, Index(unique=True)] = Field(
        ...,
        title="Order Id",
        description="Stable unique identifier for the order.",
    )
    client_name: str = Field(
        ...,
        title="Client Name",
        description="Human-readable client name.",
    )
    notional: float = Field(
        ...,
        title="Notional",
        description="Order notional amount in USD.",
    )
    status: str = Field(
        ...,
        title="Status",
        description="Order lifecycle status.",
    )


class OrderSummaryRecord(SimpleTable):
    summary_id: Annotated[str, Index(unique=True)] = Field(
        ...,
        title="Summary Id",
        description="Stable key for the computed summary row.",
    )
    as_of_date: datetime.date = Field(
        ...,
        title="As Of Date",
        description="Date represented by the summary row.",
    )
    total_notional: float = Field(
        ...,
        title="Total Notional",
        description="Total notional aggregated from client orders.",
    )
    filled_order_count: int = Field(
        ...,
        title="Filled Order Count",
        description="Number of filled orders in the source table.",
    )


class ClientOrdersConfig(SimpleTableNodeConfiguration):
    seed_orders: list[dict[str, object]] = Field(
        default_factory=lambda: [
            {
                "order_id": f"{PROJECT_ID}_alice",
                "client_name": "Alice",
                "notional": 125_000.0,
                "status": "filled",
            },
            {
                "order_id": f"{PROJECT_ID}_bob",
                "client_name": "Bob",
                "notional": 85_000.0,
                "status": "pending",
            },
        ]
    )


class OrderSummaryConfig(SimpleTableNodeConfiguration):
    summary_id: str = f"{PROJECT_ID}_latest_orders"


class ClientOrdersNode(SimpleTableNode):
    def __init__(
        self,
        configuration: ClientOrdersConfig,
        backend: InMemorySimpleTableBackend | None = None,
    ):
        super().__init__(
            simple_table_schema=ClientOrderRecord,
            configuration=configuration,
        )
        self.backend = backend or InMemorySimpleTableBackend()

    def persist_records(self, records: list[ClientOrderRecord]) -> None:
        self.backend.upsert(records, index_fields=self.get_index_field_names())

    def list_orders(self) -> list[ClientOrderRecord]:
        return [
            record
            for record in self.backend.list_records()
            if isinstance(record, ClientOrderRecord)
        ]

    def to_frame(self) -> pd.DataFrame:
        return self.backend.to_frame()

    def update(self) -> list[ClientOrderRecord]:
        return self.insert_records(records=self.configuration.seed_orders)


class OrderSummaryNode(SimpleTableNode):
    def __init__(
        self,
        configuration: OrderSummaryConfig,
        orders_node: ClientOrdersNode,
        backend: InMemorySimpleTableBackend | None = None,
    ):
        self.orders_node = orders_node
        super().__init__(
            simple_table_schema=OrderSummaryRecord,
            configuration=configuration,
        )
        self.backend = backend or InMemorySimpleTableBackend()

    def dependencies(self) -> dict[str, BaseNode]:
        return {"orders": self.orders_node}

    def persist_records(self, records: list[OrderSummaryRecord]) -> None:
        self.backend.upsert(records, index_fields=self.get_index_field_names())

    def list_summaries(self) -> list[OrderSummaryRecord]:
        return [
            record
            for record in self.backend.list_records()
            if isinstance(record, OrderSummaryRecord)
        ]

    def to_frame(self) -> pd.DataFrame:
        return self.backend.to_frame()

    def update(self) -> list[OrderSummaryRecord]:
        orders = self.orders_node.list_orders()
        total_notional = float(sum(record.notional for record in orders))
        filled_order_count = sum(1 for record in orders if record.status == "filled")

        return self.insert_records(
            records=[
                OrderSummaryRecord(
                    summary_id=self.configuration.summary_id,
                    as_of_date=datetime.date.today(),
                    total_notional=total_notional,
                    filled_order_count=filled_order_count,
                )
            ]
        )


def build_test_simple_tables() -> None:
    orders_node = ClientOrdersNode(configuration=ClientOrdersConfig())
    summary_node = OrderSummaryNode(
        configuration=OrderSummaryConfig(),
        orders_node=orders_node,
    )

    summary_result = summary_node.run()

    print("Orders records:")
    print(orders_node.list_orders())
    print("Order summary result:")
    print(summary_result)
    print("Order summary records:")
    print(summary_node.list_summaries())
    print("Order summary frame:")
    print(summary_node.to_frame())


if __name__ == "__main__":
    build_test_simple_tables()
