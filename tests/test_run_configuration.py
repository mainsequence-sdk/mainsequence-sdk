import os
import threading
from types import SimpleNamespace

os.environ.setdefault("MAINSEQUENCE_ACCESS_TOKEN", "test-access-token")
os.environ.setdefault("MAINSEQUENCE_REFRESH_TOKEN", "test-refresh-token")

import pandas as pd
import pytest
from pydantic import ValidationError

import mainsequence.client.models_foundry as models_foundry
import mainsequence.client.models_metatables as models_metatables
import mainsequence.meta_tables.data_nodes.data_nodes as data_nodes_mod
from mainsequence.client.command_center import Workspace
from mainsequence.client.models_foundry import (
    Project,
)
from mainsequence.client.models_metatables import (
    DataNodeUpdate,
    DataNodeUpdateDetails,
    MetaTable,
    TimeIndexMetaData,
)
from mainsequence.meta_tables import (
    DataNode,
    DataNodeConfiguration,
    PlatformTimeIndexMetaData,
)
from mainsequence.meta_tables.data_nodes.persist_managers import BasePersistManager
from mainsequence.meta_tables.data_nodes.run_operations import UpdateRunner


def _meta_table(
    storage_hash: str = "prices_storage_hash",
    *,
    uid: str = "meta-table-uid",
    data_source_uid: str = "data-source-uid",
    data_source: dict | None = None,
    columns: list[dict] | None = None,
) -> MetaTable:
    return MetaTable(
        uid=uid,
        data_source_uid=data_source_uid,
        data_source=data_source,
        storage_hash=storage_hash,
        management_mode="platform_managed",
        physical_table_name=storage_hash,
        table_contract={
            "version": "relational-table.v1",
            "physical": {"table_name": storage_hash},
            "columns": columns or [],
        },
    )


def _platform_storage_model(meta_table: MetaTable) -> type[PlatformTimeIndexMetaData]:
    class RuntimeStorageTable(PlatformTimeIndexMetaData):
        pass

    RuntimeStorageTable.bind_meta_table(meta_table)
    return RuntimeStorageTable


def test_data_node_storage_inherits_meta_table_but_keeps_dynamic_table_endpoint():
    assert issubclass(TimeIndexMetaData, MetaTable)
    for inherited_field in (
        "storage_hash",
        "management_mode",
        "physical_table_name",
        "labels",
        "creation_date",
    ):
        assert inherited_field not in TimeIndexMetaData.__annotations__

    storage = TimeIndexMetaData(
        uid="data-node-storage-12",
        storage_hash="prices_storage_hash",
        management_mode="platform_managed",
        physical_table_name="prices_storage_hash",
        data_source=1,
        source_class_name="PricesNode",
        creation_date="2026-04-13T00:00:00Z",
    )

    assert isinstance(storage, MetaTable)
    assert storage.management_mode == "platform_managed"
    assert storage.physical_table_name == "prices_storage_hash"
    assert TimeIndexMetaData.get_object_url().endswith("/ts_manager/dynamic_table")


def test_metatable_update_models_are_not_exported_from_models_foundry():
    moved_names = [
        "TimeIndexMetaData",
        "TimeIndexedProfile",
        "TimeIndexMetaTableRegistrationRequest",
        "DataNodeUpdate",
        "DataNodeUpdateDetails",
        "RunConfiguration",
        "Scheduler",
        "UpdateStatistics",
        "DataSource",
        "DynamicTableDataSource",
        "SessionDataSource",
    ]

    for name in moved_names:
        assert hasattr(models_metatables, name)
        assert not hasattr(models_foundry, name)


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


def test_data_node_update_details_patches_by_data_node_update_uid(monkeypatch):
    captured = {}

    class FakeResponse:
        status_code = 200

        @staticmethod
        def json():
            return {"related_table_uid": "data-node-update-44"}

    def _fake_make_request(*, s, loaders, r_type, url, payload, time_out=None):
        captured.update(
            {
                "r_type": r_type,
                "url": url,
                "payload": payload,
                "time_out": time_out,
            }
        )
        return FakeResponse()

    monkeypatch.setattr(models_metatables, "make_request", _fake_make_request)

    details = DataNodeUpdateDetails.patch_for_data_node_update_uid(
        "data-node-update-44",
        update_priority=7,
        timeout=12,
    )

    assert isinstance(details, DataNodeUpdateDetails)
    assert details.related_table_uid == "data-node-update-44"
    assert captured["r_type"] == "PATCH"
    assert captured["url"].endswith(
        "/ts_manager/local_time_serie_update_details/data-node-update-44/"
    )
    assert captured["payload"] == {"json": {"update_priority": 7}}
    assert captured["time_out"] == 12


def test_persist_manager_build_update_details_uses_update_details_resource():
    patched = []
    patched_event = threading.Event()

    class UpdateDetailsResource:
        @classmethod
        def patch_for_data_node_update_uid(cls, data_node_update_uid, **kwargs):
            patched.append((data_node_update_uid, kwargs))
            patched_event.set()

    class StorageActionTrap:
        def build_or_update_update_details(self, **_kwargs):
            raise AssertionError("storage-table update-details action should not be used")

    class UpdateDetailsPersistManager(BasePersistManager):
        UPDATE_DETAILS_CLASS = UpdateDetailsResource

    storage_table = _platform_storage_model(_meta_table())

    manager = UpdateDetailsPersistManager(
        update_hash="prices-update-hash",
        storage_table=storage_table,
        data_node_update=SimpleNamespace(
            uid="data-node-update-44",
            data_node_storage=StorageActionTrap(),
        ),
    )
    manager.set_data_node_update_lazy_callback = lambda _future: None

    manager.build_update_details(source_class_name="PricesNode")

    assert patched_event.wait(2)
    assert patched == [("data-node-update-44", {})]


def test_persist_manager_passes_storage_contract_schema_to_update():
    captured = {}

    class UpdateResource:
        build_configuration = {}

        def upsert_data_into_table(self, **kwargs):
            captured.update(kwargs)
            return self

    storage_metadata = TimeIndexMetaData.model_construct(
        uid="data-node-storage-44",
        data_source_uid="data-source-uid",
        data_source=SimpleNamespace(
            related_resource=SimpleNamespace(class_type="postgres"),
        ),
        time_indexed_profile=models_metatables.TimeIndexedProfile(
            related_table_uid="data-node-storage-44",
            time_index_name="time_index",
            index_names=["time_index", "account_uid", "unique_identifier"],
            column_dtypes_map={
                "time_index": "timestamp with time zone",
                "account_uid": "uuid",
                "unique_identifier": "string",
                "quantity": "float64",
            },
            storage_layout={
                "time_index": "time_index",
                "identity_dimensions": ["account_uid", "unique_identifier"],
            },
            physical_index_plan={
                "uniqueness": {
                    "columns": ["time_index", "account_uid", "unique_identifier"],
                },
            },
        ),
    )
    storage_table = _platform_storage_model(storage_metadata)
    manager = BasePersistManager(
        update_hash="account-holdings-update-hash",
        storage_table=storage_table,
        data_node_update=UpdateResource(),
    )
    df = pd.DataFrame(
        {"quantity": [12.0]},
        index=pd.MultiIndex.from_tuples(
            [("2026-05-30T12:00:00Z", "account-a", "AAPL")],
            names=["time_index", "account_uid", "unique_identifier"],
        ),
    )

    assert manager.persist_updated_data(df) is True
    assert captured["source_table_schema"] == {
        "time_index_name": "time_index",
        "index_names": ["time_index", "account_uid", "unique_identifier"],
        "column_dtypes_map": {
            "time_index": "timestamp with time zone",
            "account_uid": "uuid",
            "unique_identifier": "string",
            "quantity": "float64",
        },
    }


def test_data_node_storage_accepts_namespace():
    storage = TimeIndexMetaData(
        uid="data-node-storage-12",
        storage_hash="prices_storage_hash",
        management_mode="platform_managed",
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
        "management_mode": "platform_managed",
        "physical_table_name": "hash",
        "source_class_name": "ExampleNode",
        "data_source": 1,
        "creation_date": "2026-04-13T00:00:00Z",
        removed_field: "removed",
    }

    with pytest.raises(ValidationError):
        TimeIndexMetaData(**payload)


def test_persist_manager_requires_storage_table_constructor_argument():
    class UpdateResource:
        @staticmethod
        def get_or_none(**kwargs):
            return None

    class ExplicitStoragePersistManager(BasePersistManager):
        UPDATE_CLASS = UpdateResource

    with pytest.raises(TypeError, match="storage_table"):
        ExplicitStoragePersistManager(
            update_hash="prices-update-hash",
        )


def test_persist_manager_validates_storage_table_without_creating_storage():
    meta_table = _meta_table(storage_hash="canonical_prices_table")
    storage_table = _platform_storage_model(meta_table)
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
                data_node_storage=meta_table,
            )

    class ExplicitStoragePersistManager(BasePersistManager):
        UPDATE_CLASS = UpdateResource

    manager = ExplicitStoragePersistManager(
        update_hash="prices-update-hash",
        storage_table=storage_table,
    )

    manager.local_persist_exist_set_config(
        local_configuration={"config": {"identifier": "prices"}},
    )

    assert created_update_payloads == [
        {
            "update_hash": "prices-update-hash",
            "build_configuration": {"config": {"identifier": "prices"}},
            "meta_table_uid": "meta-table-uid",
        }
    ]
    assert manager.storage_table is storage_table
    assert manager.storage_metadata is meta_table


def test_persist_manager_rejects_unbound_platform_time_index_storage_table():
    class UnboundStorageTable(PlatformTimeIndexMetaData):
        pass

    with pytest.raises(ValueError, match="registered or bound"):
        BasePersistManager(
            update_hash="prices-update-hash",
            storage_table=UnboundStorageTable,
        )


def test_persist_manager_uses_platform_managed_storage_identity():
    meta_table = _meta_table(
        uid="platform-meta-table-uid",
        data_source_uid="platform-data-source-uid",
        storage_hash="canonical_prices_table",
    )
    storage_table = _platform_storage_model(meta_table)
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
                data_node_storage=meta_table,
            )

    class ExplicitStoragePersistManager(BasePersistManager):
        UPDATE_CLASS = UpdateResource

    manager = ExplicitStoragePersistManager(
        update_hash="prices-update-hash",
        storage_table=storage_table,
    )

    manager.local_persist_exist_set_config(
        local_configuration={"config": {"identifier": "prices"}},
    )

    assert created_update_payloads == [
        {
            "update_hash": "prices-update-hash",
            "build_configuration": {"config": {"identifier": "prices"}},
            "meta_table_uid": "platform-meta-table-uid",
        }
    ]


def test_persist_manager_preserves_storage_table_during_update_lookup():
    stale_response_storage = _meta_table(
        uid="stale-meta-table-uid",
        data_source_uid="stale-data-source-uid",
        storage_hash="stale_prices_table",
    )

    class UpdateResource:
        @staticmethod
        def get_or_none(**kwargs):
            return SimpleNamespace(
                build_configuration={},
                data_node_storage=stale_response_storage,
            )

    class ExplicitStoragePersistManager(BasePersistManager):
        UPDATE_CLASS = UpdateResource

    meta_table = _meta_table(storage_hash="canonical_prices_table")
    storage_table = _platform_storage_model(meta_table)
    manager = ExplicitStoragePersistManager(
        update_hash="prices-update-hash",
        storage_table=storage_table,
    )

    assert manager.data_node_update.data_node_storage is stale_response_storage
    assert manager.storage_table is storage_table
    assert manager.storage_metadata is meta_table


def test_data_node_accepts_platform_time_index_storage_table_runtime_argument():
    class Config(DataNodeConfiguration):
        identifier: str

    class StorageTableNode(DataNode):
        def __init__(
            self,
            config: Config,
            storage_table: type[PlatformTimeIndexMetaData],
        ):
            super().__init__(config=config, storage_table=storage_table)

        def dependencies(self):
            return {}

        def update(self):
            return pd.DataFrame()

    storage_table = _platform_storage_model(_meta_table(storage_hash="canonical_prices_table"))
    node = StorageTableNode(Config(identifier="prices"), storage_table=storage_table)

    assert node.storage_table is storage_table
    assert node.storage_metadata.storage_hash == "canonical_prices_table"
    assert "storage_hash" not in node.__dict__
    assert "storage_table" not in node.build_configuration
    assert "storage_table" not in node.local_initial_configuration
    assert "storage_table" not in node.remote_initial_configuration


def test_data_node_rejects_test_node_constructor_shortcut():
    class Config(DataNodeConfiguration):
        identifier: str

    class StorageTableNode(DataNode):
        def __init__(
            self,
            config: Config,
            storage_table: type[PlatformTimeIndexMetaData],
        ):
            super().__init__(config=config, storage_table=storage_table)

        def dependencies(self):
            return {}

        def update(self):
            return pd.DataFrame()

    storage_table = _platform_storage_model(_meta_table(storage_hash="canonical_prices_table"))

    with pytest.raises(TypeError, match="test_node has been removed"):
        StorageTableNode(
            Config(identifier="prices"),
            storage_table=storage_table,
            test_node=True,
        )


def test_data_node_requires_storage_table_constructor_argument():
    class Config(DataNodeConfiguration):
        identifier: str

    class StorageTableNode(DataNode):
        def __init__(self, config: Config):
            super().__init__(config=config)

        def dependencies(self):
            return {}

        def update(self):
            return pd.DataFrame()

    with pytest.raises(TypeError, match="storage_table"):
        StorageTableNode(Config(identifier="prices"))


def test_data_node_passes_storage_table_to_persist_manager(monkeypatch):
    captured = {}

    def fake_get_from_storage_table(storage_table, **kwargs):
        captured.update(kwargs)
        captured["storage_table_arg"] = storage_table
        return SimpleNamespace(
            data_node_update=None,
            storage_table=storage_table,
        )

    monkeypatch.setattr(
        data_nodes_mod.PersistManager,
        "get_from_storage_table",
        staticmethod(fake_get_from_storage_table),
    )

    class Config(DataNodeConfiguration):
        identifier: str

    class StorageTableNode(DataNode):
        def __init__(
            self,
            config: Config,
            storage_table: type[PlatformTimeIndexMetaData],
        ):
            super().__init__(config=config, storage_table=storage_table)

        def dependencies(self):
            return {}

        def update(self):
            return pd.DataFrame()

    storage_table = _platform_storage_model(_meta_table(storage_hash="canonical_prices_table"))
    node = StorageTableNode(Config(identifier="prices"), storage_table=storage_table)

    assert node.local_persist_manager.storage_table is storage_table
    assert captured["storage_table_arg"] is storage_table


def test_data_node_data_source_uid_is_derived_from_storage_table():
    class StorageTableNode(DataNode):
        def dependencies(self):
            return {}

        def update(self):
            return pd.DataFrame()

    node = StorageTableNode.__new__(StorageTableNode)
    node.storage_table = _platform_storage_model(
        _meta_table(data_source_uid="canonical-data-source")
    )

    assert node.data_source_uid == "canonical-data-source"


def test_data_node_rejects_client_meta_table_storage_argument():
    class StorageTableNode(DataNode):
        def dependencies(self):
            return {}

        def update(self):
            return pd.DataFrame()

    node = StorageTableNode.__new__(StorageTableNode)

    with pytest.raises(TypeError, match="PlatformTimeIndexMetaData"):
        node.storage_table = _meta_table()


def test_data_node_rejects_unbound_platform_time_index_storage_table():
    class StorageTableNode(DataNode):
        def dependencies(self):
            return {}

        def update(self):
            return pd.DataFrame()

    class UnboundStorageTable(PlatformTimeIndexMetaData):
        pass

    node = StorageTableNode.__new__(StorageTableNode)

    with pytest.raises(ValueError, match="registered or bound"):
        node.storage_table = UnboundStorageTable


def test_data_node_rejects_none_storage_table():
    class StorageTableNode(DataNode):
        def dependencies(self):
            return {}

        def update(self):
            return pd.DataFrame()

    node = StorageTableNode.__new__(StorageTableNode)

    with pytest.raises(TypeError, match="required"):
        node.storage_table = None


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
    data_node_storage = TimeIndexMetaData(
        uid="data-node-storage-12",
        storage_hash="prices_storage_hash",
        management_mode="platform_managed",
        physical_table_name="prices_storage_hash",
        data_source=1,
        source_class_name="PricesNode",
        creation_date="2026-04-13T00:00:00Z",
        labels=["vendor-data"],
    )
    assert workspace.labels == ["desk"]
    assert project.labels == ["research"]
    assert data_node_storage.labels == ["vendor-data"]


def test_data_node_configuration_rejects_records_field():
    with pytest.raises(ValidationError, match="Extra inputs are not permitted"):
        DataNodeConfiguration(
            records=[
                {
                    "column_name": "close",
                    "dtype": "float64",
                }
            ]
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
    node.storage_table = _platform_storage_model(
        _meta_table(
            columns=[{"name": "value", "data_type": "float64"}],
            data_source={
                "uid": "data-source-uid",
                "related_resource_class_type": "timescale",
                "related_resource": {"class_type": "timescale"},
            },
        )
    )

    with pytest.raises(TypeError, match="declared as float64"):
        node._validate_update_output(frame)


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
