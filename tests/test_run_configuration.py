import os

os.environ.setdefault("MAINSEQUENCE_ACCESS_TOKEN", "test-access-token")
os.environ.setdefault("MAINSEQUENCE_REFRESH_TOKEN", "test-refresh-token")

import pandas as pd
import pytest
from pydantic import ValidationError

from mainsequence.client.command_center import Workspace
from mainsequence.client.models_tdag import (
    DataNodeStorage,
    DataNodeUpdate,
    DataNodeUpdateDetails,
    Project,
)
from mainsequence.tdag import DataNode
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
        id=1,
        title="Workspace",
        schemaVersion=1,
        layoutKind="custom",
        createdAt="2026-04-20T00:00:00Z",
        updatedAt="2026-04-20T00:00:00Z",
        labels=["desk"],
    )
    project = Project(
        id=1,
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
