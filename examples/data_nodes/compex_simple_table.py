import datetime
from typing import Annotated

import pandas as pd
from pydantic import Field

from examples.data_nodes.simple_tables import (
    CustomersUpdater,
    CustomersUpdaterConfiguration,
)
from mainsequence.tdag.data_nodes import DataNode, DataNodeConfiguration
from mainsequence.tdag.simple_tables import (
    ForeignKey,
    Index,
    Ops,
    SimpleTable,
    SimpleTableUpdater,
    SimpleTableUpdaterConfiguration,
)


class ScenarioShockConfig(DataNodeConfiguration):
    multiplier: float = Field(
        default=1.15,
        description="Stress multiplier applied to base customer limits.",
    )


class ScenarioShockNode(DataNode):
    def __init__(self, config: ScenarioShockConfig, *args, **kwargs):
        self.shock_config = config
        super().__init__(config=config, *args, **kwargs)

    def update(self) -> pd.DataFrame:
        today = pd.Timestamp.now("UTC").normalize()
        last_update = self.update_statistics.max_time_index_value
        if last_update is not None and last_update >= today:
            return pd.DataFrame()

        return pd.DataFrame(
            {"shock_multiplier": [self.shock_config.multiplier]},
            index=pd.DatetimeIndex([today], name="time_index", tz="UTC"),
        )


class CustomerScenarioLimitRecord(SimpleTable):
    customer_id: Annotated[
        int,
        ForeignKey("customers", on_delete="cascade"),
        Index(),
        Ops(filter=True),
    ] = Field(
        ...,
        title="Customer Id",
        description="Foreign key to the canonical customer simple table.",
    )
    scenario_date: Annotated[datetime.date, Ops(filter=True, order=True)] = Field(
        ...,
        title="Scenario Date",
        description="Date the stress scenario was generated.",
    )
    scenario_label: Annotated[str, Ops(filter=True, order=True)] = Field(
        ...,
        title="Scenario Label",
        description="Logical label identifying the scenario definition.",
    )
    stressed_limit_usd: Annotated[float, Ops(filter=True, order=True)] = Field(
        ...,
        title="Stressed Limit USD",
        description="Limit after applying the dependency DataNode shock.",
    )


class CustomerScenarioLimitUpdaterConfiguration(SimpleTableUpdaterConfiguration):
    scenario_label: str = Field(
        default="daily_shock",
        description="Scenario label written into the generated simple-table rows.",
        json_schema_extra={"runtime_only": True},
    )


class CustomerScenarioLimitUpdater(SimpleTableUpdater):
    SIMPLE_TABLE_SCHEMA = CustomerScenarioLimitRecord

    def __init__(
        self,
        configuration: CustomerScenarioLimitUpdaterConfiguration,
        *,
        customers_updater: CustomersUpdater | None = None,
        shock_node: ScenarioShockNode | None = None,
        hash_namespace: str | None = None,
        test_node: bool = False,
    ):
        self.customers_updater = customers_updater or CustomersUpdater(
            configuration=CustomersUpdaterConfiguration()
        )
        self.shock_node = shock_node or ScenarioShockNode(config=ScenarioShockConfig())
        super().__init__(
            configuration=configuration,
            hash_namespace=hash_namespace,
            test_node=test_node,
        )

    def dependencies(self) -> dict[str, DataNode]:
        return {
            "customers": self.customers_updater,
            "shock_multiplier": self.shock_node,
        }

    def update(self) -> tuple[list[CustomerScenarioLimitRecord], bool]:
        today = pd.Timestamp.now("UTC").normalize()
        shock_df = self.shock_node.get_df_between_dates(
            start_date=today,
            great_or_equal=True,
        )
        if shock_df.empty:
            raise RuntimeError("ScenarioShockNode did not produce a multiplier for the current UTC day.")

        shock_multiplier = float(shock_df.iloc[-1]["shock_multiplier"])
        customer_rows = self.customers_updater.execute_filter(limit=500)
        base_limit_by_region = {
            "US": 100_000.0,
            "EU": 80_000.0,
        }

        rows: list[CustomerScenarioLimitRecord] = []
        for customer in customer_rows:
            if customer.id is None:
                continue
            base_limit = base_limit_by_region.get(customer.region, 60_000.0)
            rows.append(
                CustomerScenarioLimitRecord(
                    customer_id=customer.id,
                    scenario_date=today.date(),
                    scenario_label=self.config.scenario_label,
                    stressed_limit_usd=round(base_limit * shock_multiplier, 2),
                )
            )

        return rows, True


class ScenarioLimitSummaryConfig(DataNodeConfiguration):
    report_label: str = Field(
        default="customer_scenario_limit_summary",
        description="Label used for the summary DataNode output.",
    )


class ScenarioLimitSummaryNode(DataNode):
    def __init__(
        self,
        config: ScenarioLimitSummaryConfig,
        *,
        scenario_limit_updater: CustomerScenarioLimitUpdater | None = None,
        hash_namespace: str | None = None,
        test_node: bool = False,
    ):
        self.scenario_limit_updater = scenario_limit_updater or CustomerScenarioLimitUpdater(
            configuration=CustomerScenarioLimitUpdaterConfiguration()
        )
        super().__init__(
            config=config,
            hash_namespace=hash_namespace,
            test_node=test_node,
        )

    def dependencies(self) -> dict[str, DataNode]:
        return {"scenario_limits": self.scenario_limit_updater}

    def update(self) -> pd.DataFrame:
        today = pd.Timestamp.now("UTC").normalize()
        scenario_rows = self.scenario_limit_updater.execute_filter(
            CustomerScenarioLimitRecord.filters.scenario_date.eq(today.date()),
            limit=500,
        )
        total_limit = sum(row.stressed_limit_usd for row in scenario_rows)
        summary = pd.DataFrame(
            {
                "report_label": [self.config.report_label],
                "customer_count": [len(scenario_rows)],
                "total_stressed_limit_usd": [total_limit],
            },
            index=pd.DatetimeIndex([today], name="time_index", tz="UTC"),
        )
        return summary


def build_compex_simple_table() -> None:
    customers_updater = CustomersUpdater(configuration=CustomersUpdaterConfiguration())
    shock_node = ScenarioShockNode(config=ScenarioShockConfig(multiplier=1.15))
    scenario_limit_updater = CustomerScenarioLimitUpdater(
        configuration=CustomerScenarioLimitUpdaterConfiguration(scenario_label="daily_stress"),
        customers_updater=customers_updater,
        shock_node=shock_node,
    )
    summary_node = ScenarioLimitSummaryNode(
        config=ScenarioLimitSummaryConfig(),
        scenario_limit_updater=scenario_limit_updater,
    )

    print("Mixed dependency graph hashes:")
    print(
        {
            "customers_updater": {
                "update_hash": customers_updater.update_hash,
                "storage_hash": customers_updater.storage_hash,
            },
            "shock_node": {
                "update_hash": shock_node.update_hash,
                "storage_hash": shock_node.storage_hash,
            },
            "scenario_limit_updater": {
                "update_hash": scenario_limit_updater.update_hash,
                "storage_hash": scenario_limit_updater.storage_hash,
            },
            "summary_node": {
                "update_hash": summary_node.update_hash,
                "storage_hash": summary_node.storage_hash,
            },
        }
    )

    summary_node.run(debug_mode=True, force_update=True)

    today = pd.Timestamp.now("UTC").normalize().date()
    scenario_rows = scenario_limit_updater.execute_filter(
        CustomerScenarioLimitRecord.filters.scenario_date.eq(today),
        limit=500,
    )
    summary_df = summary_node.get_df_between_dates(
        start_date=pd.Timestamp.now("UTC").normalize(),
        great_or_equal=True,
    )

    print("Scenario rows generated through mixed DataNode/SimpleTable dependencies:")
    print([row.model_dump(mode="json") for row in scenario_rows])
    print("Summary DataNode output:")
    print(summary_df)


if __name__ == "__main__":
    build_compex_simple_table()
