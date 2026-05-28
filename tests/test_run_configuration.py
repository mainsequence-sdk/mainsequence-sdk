import os

os.environ.setdefault("MAINSEQUENCE_ACCESS_TOKEN", "test-access-token")
os.environ.setdefault("MAINSEQUENCE_REFRESH_TOKEN", "test-refresh-token")

import pandas as pd
import pytest
from pydantic import ValidationError

from mainsequence.client.command_center import Workspace
from mainsequence.client.models_metatables import MetaTable
from mainsequence.client.models_tdag import (
    DataNodeStorage,
    DataNodeUpdate,
    DataNodeUpdateDetails,
    Project,
)
from mainsequence.tdag import DataNode, DataNodeConfiguration, SourceTableForeignKey
from mainsequence.tdag.data_nodes.models import RecordDefinition
from mainsequence.tdag.data_nodes.run_operations import UpdateRunner


def test_data_node_update_accepts_local_time_serie_update_details_in_run_configuration():
    payload = {
        "uid": "data-node-update-44",
        "update_hash": "issue-44-update-hash",
        "data_node_storage": "data-node-storage-1",
        "build_configuration": {},
        "run_configuration": {
            "update_schedule": "*/1 * * * *",
            "local_time_serie_update_details": 8053,
        },
        "update_details": {
            "related_table_uid": "data-node-update-44",
            "run_configuration": {
                "update_schedule": "*/1 * * * *",
                "local_time_serie_update_details": 8053,
            },
        },
    }

    update = DataNodeUpdate(**payload)

    assert update.run_configuration is not None
    assert update.run_configuration.local_time_serie_update_details == 8053
    assert isinstance(update.update_details, DataNodeUpdateDetails)
    assert update.update_details.run_configuration is not None
    assert update.update_details.run_configuration.local_time_serie_update_details == 8053


def test_data_node_storage_accepts_namespace():
    storage = DataNodeStorage(
        uid="data-node-storage-12",
        storage_hash="prices_storage_hash",
        namespace="pytest_case_123",
        data_source=1,
        source_class_name="PricesNode",
        creation_date="2026-04-13T00:00:00Z",
    )

    assert storage.namespace == "pytest_case_123"


def test_data_node_update_accepts_labels():
    update = DataNodeUpdate(
        uid="data-node-update-44",
        update_hash="issue-44-update-hash",
        data_node_storage="data-node-storage-1",
        build_configuration={},
        labels=["pricing", "daily"],
    )

    assert update.labels == ["pricing", "daily"]


def test_label_fields_exist_on_workspace_project_and_storage_models():
    workspace = Workspace(
        uid="11111111-1111-4111-8111-111111111111",
        title="Workspace",
        schemaVersion=1,
        layoutKind="custom",
        createdAt="2026-04-20T00:00:00Z",
        updatedAt="2026-04-20T00:00:00Z",
        labels=["desk"],
    )
    project = Project(
        uid="project-uid-1",
        project_name="Project",
        is_initialized=True,
        labels=["research"],
    )
    data_node_storage = DataNodeStorage(
        uid="data-node-storage-12",
        storage_hash="prices_storage_hash",
        data_source=1,
        source_class_name="PricesNode",
        creation_date="2026-04-13T00:00:00Z",
        labels=["vendor-data"],
    )
    assert workspace.labels == ["desk"]
    assert project.labels == ["research"]
    assert data_node_storage.labels == ["vendor-data"]


def test_record_definition_rejects_column_names_longer_than_63_characters():
    with pytest.raises(ValidationError, match="at most 63 characters"):
        RecordDefinition(
            column_name="a" * 64,
            dtype="float64",
        )


def test_data_node_update_output_rejects_column_names_longer_than_63_characters():
    frame = pd.DataFrame(
        {"a" * 64: [1.0]},
        index=pd.DatetimeIndex([pd.Timestamp("2026-04-13T00:00:00Z")], name="time_index"),
    )

    with pytest.raises(ValueError, match="63 characters or fewer"):
        UpdateRunner.validate_data_frame(frame, storage_class_type="timescale")


def test_data_node_update_output_allows_datetime_payload_columns():
    frame = pd.DataFrame(
        {"event_time": [pd.Timestamp("2026-04-13T12:00:00Z")]},
        index=pd.DatetimeIndex([pd.Timestamp("2026-04-13T00:00:00Z")], name="time_index"),
    )

    UpdateRunner.validate_data_frame(frame, storage_class_type="timescale")


def test_data_node_update_output_accepts_declared_temporal_payload_columns():
    frame = pd.DataFrame(
        {
            "event_date": pd.to_datetime(["2026-04-13"]),
            "event_time": [pd.Timestamp("2026-04-13T12:00:00Z")],
        },
        index=pd.DatetimeIndex([pd.Timestamp("2026-04-13T00:00:00Z")], name="time_index"),
    )
    assert str(frame["event_date"].dtype) == "datetime64[ns]"

    UpdateRunner.validate_data_frame(
        frame,
        storage_class_type="timescale",
        records=[
            RecordDefinition(column_name="event_date", dtype="date"),
            RecordDefinition(column_name="event_time", dtype="datetime64[ns, UTC]"),
        ],
    )


def test_data_node_update_output_rejects_remote_naive_datetime_payload_columns():
    frame = pd.DataFrame(
        {"event_time": [pd.Timestamp("2026-04-13T12:00:00")]},
        index=pd.DatetimeIndex([pd.Timestamp("2026-04-13T00:00:00Z")], name="time_index"),
    )

    with pytest.raises((TypeError, ValueError), match="naive|timezone"):
        UpdateRunner.validate_data_frame(
            frame,
            storage_class_type="timescale",
            records=[RecordDefinition(column_name="event_time", dtype="datetime64[ns]")],
        )


def test_data_node_update_output_rejects_declared_dtype_mismatch():
    frame = pd.DataFrame(
        {"value": ["1.0"]},
        index=pd.DatetimeIndex([pd.Timestamp("2026-04-13T00:00:00Z")], name="time_index"),
    )

    with pytest.raises(TypeError, match="declared as float64"):
        UpdateRunner.validate_data_frame(
            frame,
            storage_class_type="timescale",
            records=[RecordDefinition(column_name="value", dtype="float64")],
        )


def test_data_node_update_output_rejects_non_jsonb_record_values():
    frame = pd.DataFrame(
        {"venue_specific_properties": [object()]},
        index=pd.DatetimeIndex([pd.Timestamp("2026-04-13T00:00:00Z")], name="time_index"),
    )

    with pytest.raises(TypeError, match="non-JSON-serializable"):
        UpdateRunner.validate_data_frame(
            frame,
            storage_class_type="timescale",
            records=[
                RecordDefinition(
                    column_name="venue_specific_properties",
                    dtype="jsonb",
                )
            ],
        )


def test_data_node_update_output_accepts_declared_string_for_python_string_columns():
    frame = pd.DataFrame(
        {"name": ["Asset A", None]},
        index=pd.DatetimeIndex([pd.Timestamp("2026-04-13T00:00:00Z")] * 2, name="time_index"),
    )

    UpdateRunner.validate_data_frame(
        frame,
        storage_class_type="timescale",
        records=[RecordDefinition(column_name="name", dtype="string")],
    )


def test_data_node_update_output_rejects_non_string_values_for_declared_string():
    frame = pd.DataFrame(
        {"name": ["Asset A", 123]},
        index=pd.DatetimeIndex([pd.Timestamp("2026-04-13T00:00:00Z")] * 2, name="time_index"),
    )

    with pytest.raises(TypeError, match="non-string value"):
        UpdateRunner.validate_data_frame(
            frame,
            storage_class_type="timescale",
            records=[RecordDefinition(column_name="name", dtype="string")],
        )


def test_source_table_initialization_schema_uses_records_and_index_config():
    class Config(DataNodeConfiguration):
        index_names: list[str]
        time_index_name: str = "time_index"

    class SchemaNode(DataNode):
        def dependencies(self):
            return {}

        def update(self):
            return pd.DataFrame()

    node = SchemaNode.__new__(SchemaNode)
    node.config = Config(
        index_names=["time_index", "asset_uid"],
        records=[
            RecordDefinition(column_name="time_index", dtype="datetime64[ns, UTC]"),
            RecordDefinition(column_name="asset_uid", dtype="uuid"),
            RecordDefinition(
                column_name="venue_specific_properties",
                dtype="jsonb",
                label="Venue Specific Properties",
                description="JSON payload for exchange-specific metadata.",
            ),
        ],
    )

    schema = node.get_source_table_initialization_schema()

    assert schema["time_index_name"] == "time_index"
    assert schema["index_names"] == ["time_index", "asset_uid"]
    assert schema["column_dtypes_map"] == {
        "time_index": "timestamp with time zone",
        "asset_uid": "uuid",
        "venue_specific_properties": "jsonb",
    }
    assert schema["columns_metadata"][2].column_name == "venue_specific_properties"
    assert schema["columns_metadata"][2].dtype == "jsonb"


def test_source_table_initialization_schema_requires_index_config():
    class SchemaNode(DataNode):
        def dependencies(self):
            return {}

        def update(self):
            return pd.DataFrame()

    node = SchemaNode.__new__(SchemaNode)
    node.config = DataNodeConfiguration(
        records=[
            RecordDefinition(column_name="time_index", dtype="datetime64[ns, UTC]"),
            RecordDefinition(column_name="value", dtype="float64"),
        ],
    )

    with pytest.raises(ValueError, match="without explicit index_names"):
        node.get_source_table_initialization_schema()


def test_source_table_initialization_schema_rejects_missing_index_records():
    class Config(DataNodeConfiguration):
        index_names: list[str]

    class SchemaNode(DataNode):
        def dependencies(self):
            return {}

        def update(self):
            return pd.DataFrame()

    node = SchemaNode.__new__(SchemaNode)
    node.config = Config(
        index_names=["time_index", "asset_uid"],
        records=[
            RecordDefinition(column_name="time_index", dtype="datetime64[ns, UTC]"),
            RecordDefinition(column_name="value", dtype="float64"),
        ],
    )

    with pytest.raises(ValueError, match="index_names must be declared in records"):
        node.get_source_table_initialization_schema()


def test_source_table_initialization_schema_rejects_duplicate_records():
    class Config(DataNodeConfiguration):
        index_names: list[str]

    class SchemaNode(DataNode):
        def dependencies(self):
            return {}

        def update(self):
            return pd.DataFrame()

    node = SchemaNode.__new__(SchemaNode)
    node.config = Config(
        index_names=["time_index"],
        records=[
            RecordDefinition(column_name="time_index", dtype="datetime64[ns, UTC]"),
            RecordDefinition(column_name="time_index", dtype="datetime64[ns, UTC]"),
        ],
    )

    with pytest.raises(ValueError, match="Duplicate DataNode record column names"):
        node.get_source_table_initialization_schema()


def test_source_table_foreign_key_requires_records_for_contract():
    asset_uid = RecordDefinition(column_name="asset_uid", dtype="uuid")
    foreign_key = SourceTableForeignKey(
        target=MetaTable.model_construct(uid="asset-meta-table-uid"),
        source_columns=[asset_uid],
        target_columns=["uid"],
    )

    with pytest.raises(ValueError, match="require DataNodeConfiguration.records"):
        foreign_key.to_contract(records=None)


def test_source_table_foreign_key_rejects_missing_source_columns():
    asset_uid = RecordDefinition(column_name="asset_uid", dtype="uuid")
    account_uid = RecordDefinition(column_name="account_uid", dtype="uuid")
    foreign_key = SourceTableForeignKey(
        target=MetaTable.model_construct(uid="asset-meta-table-uid"),
        source_columns=[asset_uid],
        target_columns=["uid"],
    )

    with pytest.raises(ValueError, match="Missing: \\['asset_uid'\\]"):
        foreign_key.to_contract(records=[account_uid])


def test_source_table_foreign_key_requires_target_columns():
    asset_uid = RecordDefinition(column_name="asset_uid", dtype="uuid")

    with pytest.raises(ValidationError, match="at least 1 item"):
        SourceTableForeignKey(
            target=MetaTable.model_construct(uid="asset-meta-table-uid"),
            source_columns=[asset_uid],
            target_columns=[],
        )


def test_source_table_foreign_key_rejects_incompatible_column_arity():
    asset_uid = RecordDefinition(column_name="asset_uid", dtype="uuid")
    foreign_key = SourceTableForeignKey(
        target=MetaTable.model_construct(uid="asset-meta-table-uid"),
        source_columns=[asset_uid],
        target_columns=["uid", "other_uid"],
    )

    with pytest.raises(ValueError, match="same number of columns"):
        foreign_key.to_contract(records=[asset_uid])


def test_source_table_foreign_key_errors_for_unresolved_target(monkeypatch):
    class TargetTable:
        name = "asset"

    class Target:
        __table__ = TargetTable()

    asset_uid = RecordDefinition(column_name="asset_uid", dtype="uuid")
    foreign_key = SourceTableForeignKey(
        target=Target,
        source_columns=[asset_uid],
        target_columns=["uid"],
    )
    monkeypatch.setattr(MetaTable, "filter", classmethod(lambda cls, **kwargs: []))

    with pytest.raises(ValueError, match="Could not resolve registered MetaTable target"):
        foreign_key.to_contract(records=[asset_uid], data_source_uid="data-source-uid")


def test_data_node_update_returning_none_is_invalid():
    class NoneReturningNode(DataNode):
        def dependencies(self):
            return {}

        def update(self):
            return None

    node = NoneReturningNode.__new__(NoneReturningNode)
    node._logger = type("LoggerStub", (), {"debug": lambda *args, **kwargs: None})()

    with pytest.raises(Exception, match="needs to return a data frame"):
        node._execute_local_update(historical_update=None)
