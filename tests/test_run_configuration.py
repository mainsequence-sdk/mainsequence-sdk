import os
from types import SimpleNamespace

os.environ.setdefault("MAINSEQUENCE_ACCESS_TOKEN", "test-access-token")
os.environ.setdefault("MAINSEQUENCE_REFRESH_TOKEN", "test-refresh-token")

import pandas as pd
import pytest
from pydantic import ValidationError

import mainsequence.tdag.data_nodes.data_nodes as data_nodes_mod
from mainsequence.client.command_center import Workspace
from mainsequence.client.models_metatables import MetaTable
from mainsequence.client.models_tdag import (
    DataNodeStorage,
    DataNodeUpdate,
    DataNodeUpdateDetails,
    Project,
)
from mainsequence.tdag import DataNode, DataNodeConfiguration, SourceTableForeignKey
from mainsequence.tdag.base_persist_managers import BasePersistManager
from mainsequence.tdag.data_nodes.models import RecordDefinition
from mainsequence.tdag.data_nodes.run_operations import UpdateRunner


def _meta_table(
    storage_hash: str = "prices_storage_hash",
    *,
    uid: str = "meta-table-uid",
    data_source_uid: str = "data-source-uid",
    columns: list[dict] | None = None,
) -> MetaTable:
    return MetaTable(
        uid=uid,
        data_source_uid=data_source_uid,
        storage_hash=storage_hash,
        management_mode="platform_managed",
        physical_table_name=storage_hash,
        table_contract={
            "version": "relational-table.v1",
            "physical": {"table_name": storage_hash},
            "columns": columns or [],
        },
    )


def test_data_node_storage_inherits_meta_table_but_keeps_dynamic_table_endpoint():
    assert issubclass(DataNodeStorage, MetaTable)
    for inherited_field in (
        "storage_hash",
        "management_mode",
        "physical_table_name",
        "labels",
        "creation_date",
    ):
        assert inherited_field not in DataNodeStorage.__annotations__

    storage = DataNodeStorage(
        uid="data-node-storage-12",
        storage_hash="prices_storage_hash",
        data_source=1,
        source_class_name="PricesNode",
        creation_date="2026-04-13T00:00:00Z",
    )

    assert isinstance(storage, MetaTable)
    assert storage.management_mode == "platform_managed"
    assert storage.physical_table_name == "prices_storage_hash"
    assert DataNodeStorage.get_object_url().endswith("/ts_manager/dynamic_table")


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
        physical_table_name="prices_physical_table",
        namespace="pytest_case_123",
        data_source=1,
        source_class_name="PricesNode",
        creation_date="2026-04-13T00:00:00Z",
    )

    assert storage.namespace == "pytest_case_123"
    assert storage.physical_table_name == "prices_physical_table"


@pytest.mark.parametrize(
    "removed_field",
    [
        "build_configuration",
        "time_serie_source_code_git_hash",
        "time_serie_source_code",
        "data_frequency_id",
        "table_name",
    ],
)
def test_data_node_storage_rejects_removed_backend_fields(removed_field):
    payload = {
        "uid": "data-node-storage-1",
        "storage_hash": "hash",
        "source_class_name": "ExampleNode",
        "data_source": 1,
        "creation_date": "2026-04-13T00:00:00Z",
        removed_field: "removed",
    }

    with pytest.raises(ValidationError):
        DataNodeStorage(**payload)


def test_persist_manager_requires_explicit_storage_table():
    class UpdateResource:
        @staticmethod
        def get_or_none(**kwargs):
            return None

    class ExplicitStoragePersistManager(BasePersistManager):
        UPDATE_CLASS = UpdateResource

    manager = ExplicitStoragePersistManager(
        data_source=SimpleNamespace(uid="data-source-uid"),
        update_hash="prices-update-hash",
    )

    with pytest.raises(ValueError, match="explicit storage_table"):
        manager.local_persist_exist_set_config(
            local_configuration={},
            open_to_public=False,
        )


def test_persist_manager_validates_storage_table_without_creating_storage():
    storage_table = _meta_table(storage_hash="canonical_prices_table")
    created_update_payloads = []

    class UpdateResource:
        @staticmethod
        def get_or_none(**kwargs):
            return None

        @staticmethod
        def get_or_create(**kwargs):
            created_update_payloads.append(kwargs)
            return SimpleNamespace(
                build_configuration=kwargs["build_configuration"],
                data_node_storage=storage_table,
            )

    class ExplicitStoragePersistManager(BasePersistManager):
        UPDATE_CLASS = UpdateResource

    manager = ExplicitStoragePersistManager(
        data_source=SimpleNamespace(uid="data-source-uid"),
        update_hash="prices-update-hash",
        data_node_storage=storage_table,
    )

    manager.local_persist_exist_set_config(
        local_configuration={"config": {"identifier": "prices"}},
        open_to_public=False,
    )

    assert created_update_payloads == [
        {
            "update_hash": "prices-update-hash",
            "build_configuration": {"config": {"identifier": "prices"}},
            "data_source_uid": "data-source-uid",
            "meta_table_uid": "meta-table-uid",
        }
    ]
    assert manager.data_node_storage is storage_table


def test_persist_manager_preserves_explicit_storage_table_during_update_lookup():
    class UpdateResource:
        @staticmethod
        def get_or_none(**kwargs):
            return None

    class ExplicitStoragePersistManager(BasePersistManager):
        UPDATE_CLASS = UpdateResource

    storage_table = _meta_table(storage_hash="canonical_prices_table")
    manager = ExplicitStoragePersistManager(
        data_source=SimpleNamespace(uid="data-source-uid"),
        update_hash="prices-update-hash",
        data_node_storage=storage_table,
    )

    assert manager.data_node_update is None
    assert manager.data_node_storage is storage_table


def test_data_node_accepts_storage_table_runtime_argument(monkeypatch):
    monkeypatch.setattr(
        data_nodes_mod,
        "get_data_source_from_orm",
        lambda: SimpleNamespace(uid="data-source-uid", related_resource_class_type=None),
    )

    class Config(DataNodeConfiguration):
        identifier: str

    class StorageTableNode(DataNode):
        def __init__(
            self,
            config: Config,
            storage_table: MetaTable | None = None,
        ):
            super().__init__(config=config, storage_table=storage_table)

        def dependencies(self):
            return {}

        def update(self):
            return pd.DataFrame()

    storage_table = _meta_table(storage_hash="canonical_prices_table")
    node = StorageTableNode(Config(identifier="prices"), storage_table=storage_table)

    assert node.storage_table is storage_table
    assert node.storage_table.storage_hash == "canonical_prices_table"
    assert "storage_hash" not in node.__dict__
    assert "storage_table" not in node.build_configuration
    assert "storage_table" not in node.local_initial_configuration
    assert "storage_table" not in node.remote_initial_configuration


def test_data_node_passes_storage_table_to_persist_manager(monkeypatch):
    monkeypatch.setattr(
        data_nodes_mod,
        "get_data_source_from_orm",
        lambda: SimpleNamespace(uid="data-source-uid", related_resource_class_type=None),
    )

    captured = {}

    def fake_get_from_data_type(**kwargs):
        captured.update(kwargs)
        return SimpleNamespace(
            data_node_update=None,
            data_node_storage=kwargs.get("data_node_storage"),
        )

    monkeypatch.setattr(
        data_nodes_mod.PersistManager,
        "get_from_data_type",
        staticmethod(fake_get_from_data_type),
    )

    class Config(DataNodeConfiguration):
        identifier: str

    class StorageTableNode(DataNode):
        def __init__(self, config: Config, storage_table: MetaTable | None = None):
            super().__init__(config=config, storage_table=storage_table)

        def dependencies(self):
            return {}

        def update(self):
            return pd.DataFrame()

    storage_table = _meta_table(storage_hash="canonical_prices_table")
    node = StorageTableNode(Config(identifier="prices"), storage_table=storage_table)

    assert node.local_persist_manager.data_node_storage is storage_table
    assert captured["data_node_storage"] is storage_table


def test_data_node_rejects_storage_table_from_different_data_source(monkeypatch):
    monkeypatch.setattr(
        data_nodes_mod,
        "get_data_source_from_orm",
        lambda: SimpleNamespace(uid="active-data-source", related_resource_class_type=None),
    )

    class Config(DataNodeConfiguration):
        identifier: str

    class StorageTableNode(DataNode):
        def __init__(self, config: Config, storage_table: MetaTable | None = None):
            super().__init__(config=config, storage_table=storage_table)

        def dependencies(self):
            return {}

        def update(self):
            return pd.DataFrame()

    storage_table = _meta_table(data_source_uid="other-data-source")

    with pytest.raises(ValueError, match="storage_table.data_source_uid"):
        StorageTableNode(Config(identifier="prices"), storage_table=storage_table)


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
        meta_table=_meta_table(
            columns=[
                {"name": "event_date", "data_type": "date"},
                {"name": "event_time", "data_type": "datetime64[ns, UTC]"},
            ],
        ),
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
            meta_table=_meta_table(
                columns=[{"name": "event_time", "data_type": "datetime64[ns]"}],
            ),
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
            meta_table=_meta_table(columns=[{"name": "value", "data_type": "float64"}]),
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
            meta_table=_meta_table(
                columns=[{"name": "venue_specific_properties", "data_type": "jsonb"}],
            ),
        )


def test_data_node_update_output_accepts_declared_string_for_python_string_columns():
    frame = pd.DataFrame(
        {"name": ["Asset A", None]},
        index=pd.DatetimeIndex([pd.Timestamp("2026-04-13T00:00:00Z")] * 2, name="time_index"),
    )

    UpdateRunner.validate_data_frame(
        frame,
        storage_class_type="timescale",
        meta_table=_meta_table(columns=[{"name": "name", "data_type": "string"}]),
    )


def test_data_node_update_output_accepts_declared_object_for_pandas_str_dtype():
    with pd.option_context("future.infer_string", True):
        frame = pd.DataFrame(
            {"unique_identifier": ["asset-1", None]},
            index=pd.DatetimeIndex(
                [pd.Timestamp("2026-04-13T00:00:00Z")] * 2,
                name="time_index",
            ),
        )

    assert str(frame["unique_identifier"].dtype) == "str"
    UpdateRunner.validate_data_frame(
        frame,
        storage_class_type="timescale",
        meta_table=_meta_table(
            columns=[{"name": "unique_identifier", "data_type": "object"}],
        ),
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
            meta_table=_meta_table(columns=[{"name": "name", "data_type": "string"}]),
        )


def test_data_node_update_output_validates_against_storage_table_contract():
    class SchemaNode(DataNode):
        def dependencies(self):
            return {}

        def update(self):
            return pd.DataFrame()

    frame = pd.DataFrame(
        {"value": ["1.0"]},
        index=pd.DatetimeIndex([pd.Timestamp("2026-04-13T00:00:00Z")], name="time_index"),
    )
    node = SchemaNode.__new__(SchemaNode)
    node._data_source = SimpleNamespace(
        related_resource=SimpleNamespace(class_type="timescale"),
    )
    node.storage_table = _meta_table(columns=[{"name": "value", "data_type": "float64"}])

    with pytest.raises(TypeError, match="declared as float64"):
        node._validate_update_output(frame)


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
