import os
import threading
from types import SimpleNamespace

os.environ.setdefault("MAINSEQUENCE_ACCESS_TOKEN", "test-access-token")
os.environ.setdefault("MAINSEQUENCE_REFRESH_TOKEN", "test-refresh-token")

import pandas as pd
import pytest
from pydantic import ValidationError

import mainsequence.client.metatables as models_metatables
import mainsequence.client.models_foundry as models_foundry
import mainsequence.meta_tables.time_index_table_updates.updaters as updaters_module
from mainsequence.client.metatables import (
    MetaTable,
    TimeIndexMetaTable,
    TimeIndexTableUpdate,
    TimeIndexTableUpdateDetails,
)
from mainsequence.client.models_foundry import (
    Project,
)
from mainsequence.meta_tables import (
    PlatformTimeIndexMetaTable,
    TimeIndexTableUpdateConfig,
    TimeIndexTableUpdater,
)
from mainsequence.meta_tables.time_index_table_updates.managers import (
    BaseTimeIndexTableUpdateManager,
)
from mainsequence.meta_tables.time_index_table_updates.runner import UpdateRunner


def _canonical_build_configuration(**values: object) -> dict[str, object]:
    configuration: dict[str, object] = {
        "configuration_schema_version": 2,
        "table_updater_class_import_path": {
            "module": "tests.test_time_index_table_update_configuration_client",
            "qualname": "ExampleUpdater",
        },
    }
    configuration.update(values)
    return configuration


def _meta_table(
    physical_table_name: str = "prices_table",
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
        management_mode="platform_managed",
        physical_table_name=physical_table_name,
        table_contract={
            "version": "relational-table.v1",
            "physical": {"table_name": physical_table_name},
            "columns": columns or [],
        },
    )


def _platform_output_model(meta_table: MetaTable) -> type[PlatformTimeIndexMetaTable]:
    class RuntimeOutputTable(PlatformTimeIndexMetaTable):
        pass

    if not isinstance(meta_table, TimeIndexMetaTable):
        meta_table = TimeIndexMetaTable.model_construct(**meta_table.model_dump())
    RuntimeOutputTable._bind_meta_table(meta_table)
    return RuntimeOutputTable


def test_output_table_uses_time_index_meta_table_endpoint():
    assert issubclass(TimeIndexMetaTable, MetaTable)
    for inherited_field in (
        "storage_hash",
        "management_mode",
        "physical_table_name",
        "labels",
        "creation_date",
    ):
        assert inherited_field not in TimeIndexMetaTable.__annotations__

    storage = TimeIndexMetaTable(
        uid="time-index-table-storage-12",
        storage_hash="prices_storage_hash",
        management_mode="platform_managed",
        physical_table_name="prices_storage_hash",
        data_source={
            "uid": "data-source-uid",
            "data_source_uid": "data-source-uid",
            "class_type": "timescale_db",
        },
        source_class_name="PricesNode",
        creation_date="2026-04-13T00:00:00Z",
    )

    assert isinstance(storage, MetaTable)
    assert storage.management_mode == "platform_managed"
    assert storage.physical_table_name == "prices_storage_hash"
    assert TimeIndexMetaTable.get_object_url().endswith("/time-index-meta-tables")


def test_metatable_update_models_are_not_exported_from_models_foundry():
    moved_names = [
        "TimeIndexMetaTable",
        "TimeIndexedProfile",
        "TimeIndexMetaTableRegistrationRequest",
        "TimeIndexTableUpdate",
        "TimeIndexTableUpdateDetails",
        "TimeIndexTableUpdateConfiguration",
        "Scheduler",
        "UpdateStatistics",
        "DataSource",
        "SessionDataSource",
    ]

    for name in moved_names:
        assert hasattr(models_metatables, name)
        assert not hasattr(models_foundry, name)
    assert not hasattr(models_metatables, "DynamicTableDataSource")


def test_table_update_accepts_canonical_update_details_uid_in_run_configuration():
    payload = {
        "uid": "time-index-table-update-44",
        "update_hash": "issue-44-update-hash",
        "output_table": "time-index-table-storage-1",
        "build_configuration": _canonical_build_configuration(),
        "run_configuration": {
            "update_schedule": "*/1 * * * *",
            "table_update_details_uid": "update-details-8053",
        },
        "update_details": {
            "table_update_uid": "time-index-table-update-44",
            "run_configuration": {
                "update_schedule": "*/1 * * * *",
                "table_update_details_uid": "update-details-8053",
            },
        },
    }

    update = TimeIndexTableUpdate(**payload)

    assert update.run_configuration is not None
    assert update.run_configuration.table_update_details_uid == "update-details-8053"
    assert isinstance(update.update_details, TimeIndexTableUpdateDetails)
    assert update.update_details.run_configuration is not None
    assert update.update_details.run_configuration.table_update_details_uid == "update-details-8053"


def test_table_update_details_patches_by_table_update_uid(monkeypatch):
    captured = {}

    class FakeResponse:
        status_code = 200

        @staticmethod
        def json():
            return {"table_update_uid": "time-index-table-update-44"}

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

    details = TimeIndexTableUpdateDetails.patch_for_table_update_uid(
        "time-index-table-update-44",
        update_priority=7,
        timeout=12,
    )

    assert isinstance(details, TimeIndexTableUpdateDetails)
    assert details.table_update_uid == "time-index-table-update-44"
    assert captured["r_type"] == "PATCH"
    assert captured["url"].endswith("/time-index-table-update-details/time-index-table-update-44/")
    assert captured["payload"] == {"json": {"update_priority": 7}}
    assert captured["time_out"] == 12


def test_update_manager_build_update_details_uses_update_details_resource():
    patched = []
    patched_event = threading.Event()

    class UpdateDetailsResource:
        @classmethod
        def patch_for_table_update_uid(cls, table_update_uid, **kwargs):
            patched.append((table_update_uid, kwargs))
            patched_event.set()

    class OutputTableActionTrap:
        def build_or_update_update_details(self, **_kwargs):
            raise AssertionError("storage-table update-details action should not be used")

    class UpdateDetailsTimeIndexTableUpdateManager(BaseTimeIndexTableUpdateManager):
        UPDATE_DETAILS_CLASS = UpdateDetailsResource

    output_table = _platform_output_model(_meta_table())

    manager = UpdateDetailsTimeIndexTableUpdateManager(
        update_hash="prices-update-hash",
        output_table=output_table,
        table_update=SimpleNamespace(
            uid="time-index-table-update-44",
            output_table=OutputTableActionTrap(),
        ),
    )
    manager.set_table_update_lazy_callback = lambda _future: None

    manager.build_update_details(source_class_name="PricesNode")

    assert patched_event.wait(2)
    assert patched == [("time-index-table-update-44", {})]


def test_update_manager_does_not_pass_storage_contract_schema_override_to_update():
    captured = {}

    class UpdateResource:
        build_configuration = _canonical_build_configuration()

        def upsert_data_into_table(self, **kwargs):
            captured.update(kwargs)
            return self

    output_metadata = TimeIndexMetaTable.model_construct(
        uid="time-index-table-storage-44",
        data_source_uid="data-source-uid",
        data_source=SimpleNamespace(class_type="postgresql"),
        time_indexed_profile=models_metatables.TimeIndexedProfile(
            time_index_meta_table_uid="time-index-table-storage-44",
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
    output_table = _platform_output_model(output_metadata)
    manager = BaseTimeIndexTableUpdateManager(
        update_hash="account-holdings-update-hash",
        output_table=output_table,
        table_update=UpdateResource(),
    )
    df = pd.DataFrame(
        {"quantity": [12.0]},
        index=pd.MultiIndex.from_tuples(
            [("2026-05-30T12:00:00Z", "account-a", "AAPL")],
            names=["time_index", "account_uid", "unique_identifier"],
        ),
    )

    assert manager.persist_updated_data(df) is True
    assert "source_table_schema" not in captured


def test_output_table_accepts_namespace():
    storage = TimeIndexMetaTable(
        uid="time-index-table-storage-12",
        storage_hash="prices_storage_hash",
        management_mode="platform_managed",
        physical_table_name="prices_physical_table",
        namespace="pytest_case_123",
        data_source={
            "uid": "data-source-uid",
            "data_source_uid": "data-source-uid",
            "class_type": "timescale_db",
        },
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
def test_output_table_rejects_removed_backend_fields(removed_field):
    payload = {
        "uid": "time-index-table-storage-1",
        "storage_hash": "hash",
        "management_mode": "platform_managed",
        "physical_table_name": "hash",
        "source_class_name": "ExampleNode",
        "data_source": {
            "uid": "data-source-uid",
            "data_source_uid": "data-source-uid",
            "class_type": "timescale_db",
        },
        "creation_date": "2026-04-13T00:00:00Z",
        removed_field: "removed",
    }

    with pytest.raises(ValidationError):
        TimeIndexMetaTable(**payload)


def test_update_manager_requires_output_table_constructor_argument():
    class UpdateResource:
        @staticmethod
        def get_or_none(**kwargs):
            return None

    class ExplicitOutputTimeIndexTableUpdateManager(BaseTimeIndexTableUpdateManager):
        UPDATE_CLASS = UpdateResource

    with pytest.raises(TypeError, match="output_table"):
        ExplicitOutputTimeIndexTableUpdateManager(
            update_hash="prices-update-hash",
        )


def test_update_manager_validates_output_table_without_creating_storage():
    meta_table = _meta_table(physical_table_name="canonical_prices_table")
    output_table = _platform_output_model(meta_table)
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
                output_table=meta_table,
            )

    class ExplicitOutputTimeIndexTableUpdateManager(BaseTimeIndexTableUpdateManager):
        UPDATE_CLASS = UpdateResource

    manager = ExplicitOutputTimeIndexTableUpdateManager(
        update_hash="prices-update-hash",
        output_table=output_table,
    )

    manager.ensure_table_update(
        local_configuration=_canonical_build_configuration(config={"identifier": "prices"}),
    )

    assert created_update_payloads == [
        {
            "update_hash": "prices-update-hash",
            "build_configuration": _canonical_build_configuration(config={"identifier": "prices"}),
            "output_table_uid": "meta-table-uid",
        }
    ]
    assert manager.output_table is output_table
    assert manager.output_metadata.uid == meta_table.uid
    assert isinstance(manager.output_metadata, TimeIndexMetaTable)


def test_update_manager_rejects_unbound_platform_time_index_output_table():
    class UnboundOutputTable(PlatformTimeIndexMetaTable):
        pass

    with pytest.raises(ValueError, match="not bound to backend TimeIndexMetaTable"):
        BaseTimeIndexTableUpdateManager(
            update_hash="prices-update-hash",
            output_table=UnboundOutputTable,
        )


def test_update_manager_uses_platform_managed_storage_identity():
    meta_table = _meta_table(
        uid="platform-meta-table-uid",
        data_source_uid="platform-data-source-uid",
        physical_table_name="canonical_prices_table",
    )
    output_table = _platform_output_model(meta_table)
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
                output_table=meta_table,
            )

    class ExplicitOutputTimeIndexTableUpdateManager(BaseTimeIndexTableUpdateManager):
        UPDATE_CLASS = UpdateResource

    manager = ExplicitOutputTimeIndexTableUpdateManager(
        update_hash="prices-update-hash",
        output_table=output_table,
    )

    manager.ensure_table_update(
        local_configuration=_canonical_build_configuration(config={"identifier": "prices"}),
    )

    assert created_update_payloads == [
        {
            "update_hash": "prices-update-hash",
            "build_configuration": _canonical_build_configuration(config={"identifier": "prices"}),
            "output_table_uid": "platform-meta-table-uid",
        }
    ]


def test_update_manager_update_lookup_uses_storage_uid_not_data_source_uid():
    meta_table = _meta_table(
        uid="platform-meta-table-uid",
        data_source_uid="platform-data-source-uid",
        physical_table_name="canonical_prices_table",
    )
    output_table = _platform_output_model(meta_table)

    class ExplicitOutputTimeIndexTableUpdateManager(BaseTimeIndexTableUpdateManager):
        UPDATE_CLASS = object

    manager = ExplicitOutputTimeIndexTableUpdateManager(
        update_hash=None,
        output_table=output_table,
    )
    manager.update_hash = "prices-update-hash"

    lookup = manager._build_update_get_or_none_kwargs(include_relations_detail=True)

    assert lookup == {
        "update_hash": "prices-update-hash",
        "include_relations_detail": True,
        "output_table__uid": "platform-meta-table-uid",
    }
    assert "output_table__data_source__uid" not in lookup


def test_update_manager_missing_update_warning_names_table_update():
    warnings = []

    class UpdateResource:
        @staticmethod
        def get_or_none(**kwargs):
            return None

    class ExplicitOutputTimeIndexTableUpdateManager(BaseTimeIndexTableUpdateManager):
        UPDATE_CLASS = UpdateResource

    meta_table = _meta_table(
        uid="platform-meta-table-uid",
        data_source_uid="platform-data-source-uid",
        physical_table_name="canonical_prices_table",
    )
    output_table = _platform_output_model(meta_table)
    manager = ExplicitOutputTimeIndexTableUpdateManager(
        update_hash=None,
        output_table=output_table,
    )
    manager.update_hash = "importvalmer_f59da5843b77a34711b2e5e7cb94927b"
    manager.logger = SimpleNamespace(warning=warnings.append)

    manager.set_table_update_lazy(force_registry=True)
    assert manager._table_update_future is not None
    assert manager._table_update_future.result(timeout=5) is None

    assert warnings == [
        "TimeIndexTableUpdate importvalmer_f59da5843b77a34711b2e5e7cb94927b "
        "for MetaTable platform-meta-table-uid "
        "(physical_schema=public, physical_table_name=canonical_prices_table) "
        "not found in backend"
    ]
    assert "TimeSeries" not in warnings[0]
    assert "platform-data-source-uid" not in warnings[0]


def test_update_manager_preserves_output_table_during_update_lookup():
    stale_response_storage = _meta_table(
        uid="stale-meta-table-uid",
        data_source_uid="stale-data-source-uid",
        physical_table_name="stale_prices_table",
    )

    class UpdateResource:
        @staticmethod
        def get_or_none(**kwargs):
            return SimpleNamespace(
                build_configuration=_canonical_build_configuration(),
                output_table=stale_response_storage,
            )

    class ExplicitOutputTimeIndexTableUpdateManager(BaseTimeIndexTableUpdateManager):
        UPDATE_CLASS = UpdateResource

    meta_table = _meta_table(physical_table_name="canonical_prices_table")
    output_table = _platform_output_model(meta_table)
    manager = ExplicitOutputTimeIndexTableUpdateManager(
        update_hash="prices-update-hash",
        output_table=output_table,
    )

    assert manager.table_update.output_table is stale_response_storage
    assert manager.output_table is output_table
    assert manager.output_metadata.uid == meta_table.uid
    assert isinstance(manager.output_metadata, TimeIndexMetaTable)


def test_time_index_table_accepts_platform_time_index_output_table_runtime_argument():
    class Config(TimeIndexTableUpdateConfig):
        identifier: str

    class OutputTableUpdater(TimeIndexTableUpdater):
        def __init__(
            self,
            config: Config,
            output_table: type[PlatformTimeIndexMetaTable],
        ):
            super().__init__(config=config, output_table=output_table)

        def dependencies(self):
            return {}

        def update(self):
            return pd.DataFrame()

    output_table = _platform_output_model(_meta_table(physical_table_name="canonical_prices_table"))
    node = OutputTableUpdater(Config(identifier="prices"), output_table=output_table)

    assert node.output_table is output_table
    assert node.output_metadata.physical_table_name == "canonical_prices_table"
    assert "storage_hash" not in node.__dict__
    assert "output_table" not in node.build_configuration
    assert "output_table" not in node.local_initial_configuration
    assert "output_table" not in node.remote_initial_configuration


def test_time_index_table_rejects_test_node_constructor_shortcut():
    class Config(TimeIndexTableUpdateConfig):
        identifier: str

    class OutputTableUpdater(TimeIndexTableUpdater):
        def __init__(
            self,
            config: Config,
            output_table: type[PlatformTimeIndexMetaTable],
        ):
            super().__init__(config=config, output_table=output_table)

        def dependencies(self):
            return {}

        def update(self):
            return pd.DataFrame()

    output_table = _platform_output_model(_meta_table(physical_table_name="canonical_prices_table"))

    with pytest.raises(TypeError, match="test_node has been removed"):
        OutputTableUpdater(
            Config(identifier="prices"),
            output_table=output_table,
            test_node=True,
        )


def test_time_index_table_requires_output_table_constructor_argument():
    class Config(TimeIndexTableUpdateConfig):
        identifier: str

    class OutputTableUpdater(TimeIndexTableUpdater):
        def __init__(self, config: Config):
            super().__init__(config=config)

        def dependencies(self):
            return {}

        def update(self):
            return pd.DataFrame()

    with pytest.raises(TypeError, match="output_table"):
        OutputTableUpdater(Config(identifier="prices"))


def test_time_index_table_passes_output_table_to_update_manager(monkeypatch):
    captured = {}

    def fake_get_from_output_table(output_table, **kwargs):
        captured.update(kwargs)
        captured["output_table_arg"] = output_table
        return SimpleNamespace(
            table_update=None,
            output_table=output_table,
        )

    monkeypatch.setattr(
        updaters_module.TimeIndexTableUpdateManager,
        "get_from_output_table",
        staticmethod(fake_get_from_output_table),
    )

    class Config(TimeIndexTableUpdateConfig):
        identifier: str

    class OutputTableUpdater(TimeIndexTableUpdater):
        def __init__(
            self,
            config: Config,
            output_table: type[PlatformTimeIndexMetaTable],
        ):
            super().__init__(config=config, output_table=output_table)

        def dependencies(self):
            return {}

        def update(self):
            return pd.DataFrame()

    output_table = _platform_output_model(_meta_table(physical_table_name="canonical_prices_table"))
    node = OutputTableUpdater(Config(identifier="prices"), output_table=output_table)

    assert node.update_manager.output_table is output_table
    assert captured["output_table_arg"] is output_table


def test_time_index_table_data_source_uid_is_derived_from_output_table():
    class OutputTableUpdater(TimeIndexTableUpdater):
        def dependencies(self):
            return {}

        def update(self):
            return pd.DataFrame()

    node = OutputTableUpdater.__new__(OutputTableUpdater)
    node.output_table = _platform_output_model(_meta_table(data_source_uid="canonical-data-source"))

    assert node.data_source_uid == "canonical-data-source"


def test_time_index_table_rejects_client_meta_table_storage_argument():
    class OutputTableUpdater(TimeIndexTableUpdater):
        def dependencies(self):
            return {}

        def update(self):
            return pd.DataFrame()

    node = OutputTableUpdater.__new__(OutputTableUpdater)

    with pytest.raises(TypeError, match="PlatformTimeIndexMetaTable"):
        node.output_table = _meta_table()


def test_time_index_table_rejects_unbound_platform_time_index_output_table():
    class OutputTableUpdater(TimeIndexTableUpdater):
        def dependencies(self):
            return {}

        def update(self):
            return pd.DataFrame()

    class UnboundOutputTable(PlatformTimeIndexMetaTable):
        pass

    node = OutputTableUpdater.__new__(OutputTableUpdater)

    with pytest.raises(ValueError, match="not bound to backend TimeIndexMetaTable"):
        node.output_table = UnboundOutputTable


def test_time_index_table_rejects_none_output_table():
    class OutputTableUpdater(TimeIndexTableUpdater):
        def dependencies(self):
            return {}

        def update(self):
            return pd.DataFrame()

    node = OutputTableUpdater.__new__(OutputTableUpdater)

    with pytest.raises(TypeError, match="required"):
        node.output_table = None


def test_table_update_accepts_labels():
    update = TimeIndexTableUpdate(
        uid="time-index-table-update-44",
        update_hash="issue-44-update-hash",
        output_table="time-index-table-storage-1",
        build_configuration=_canonical_build_configuration(),
        labels=["pricing", "daily"],
    )

    assert update.labels == ["pricing", "daily"]


def test_label_fields_exist_on_project_and_storage_models():
    project = Project(
        uid="project-uid-1",
        project_name="Project",
        project_type="python",
        primary_language="python",
        framework="mainsequence",
        labels=["research"],
    )
    output_table = TimeIndexMetaTable(
        uid="time-index-table-storage-12",
        storage_hash="prices_storage_hash",
        management_mode="platform_managed",
        physical_table_name="prices_storage_hash",
        data_source={
            "uid": "data-source-uid",
            "data_source_uid": "data-source-uid",
            "class_type": "timescale_db",
        },
        source_class_name="PricesNode",
        creation_date="2026-04-13T00:00:00Z",
        labels=["vendor-data"],
    )
    assert project.labels == ["research"]
    assert output_table.labels == ["vendor-data"]


def test_time_index_table_configuration_rejects_records_field():
    with pytest.raises(ValidationError, match="Extra inputs are not permitted"):
        TimeIndexTableUpdateConfig(
            records=[
                {
                    "column_name": "close",
                    "dtype": "float64",
                }
            ]
        )


def test_table_update_output_rejects_column_names_longer_than_63_characters():
    frame = pd.DataFrame(
        {"a" * 64: [1.0]},
        index=pd.DatetimeIndex([pd.Timestamp("2026-04-13T00:00:00Z")], name="time_index"),
    )

    with pytest.raises(ValueError, match="63 characters or fewer"):
        UpdateRunner.validate_data_frame(frame, storage_class_type="timescale")


def test_table_update_output_allows_datetime_payload_columns():
    frame = pd.DataFrame(
        {"event_time": [pd.Timestamp("2026-04-13T12:00:00Z")]},
        index=pd.DatetimeIndex([pd.Timestamp("2026-04-13T00:00:00Z")], name="time_index"),
    )

    UpdateRunner.validate_data_frame(frame, storage_class_type="timescale")


def test_table_update_output_accepts_declared_temporal_payload_columns():
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


def test_table_update_output_rejects_remote_naive_datetime_payload_columns():
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


def test_table_update_output_rejects_declared_dtype_mismatch():
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


def test_table_update_output_rejects_non_jsonb_record_values():
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


def test_table_update_output_accepts_declared_string_for_python_string_columns():
    frame = pd.DataFrame(
        {"name": ["Asset A", None]},
        index=pd.DatetimeIndex([pd.Timestamp("2026-04-13T00:00:00Z")] * 2, name="time_index"),
    )

    UpdateRunner.validate_data_frame(
        frame,
        storage_class_type="timescale",
        meta_table=_meta_table(columns=[{"name": "name", "data_type": "string"}]),
    )


def test_table_update_output_accepts_declared_object_for_pandas_str_dtype():
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


def test_table_update_output_accepts_declared_character_varying_for_pandas_string_dtype():
    frame = pd.DataFrame(
        {"asset_identifier": pd.Series(["asset-1", None], dtype="string")},
        index=pd.DatetimeIndex(
            [pd.Timestamp("2026-04-13T00:00:00Z")] * 2,
            name="time_index",
        ),
    )

    UpdateRunner.validate_data_frame(
        frame,
        storage_class_type="timescale",
        meta_table=_meta_table(
            columns=[{"name": "asset_identifier", "data_type": "character varying"}],
        ),
    )


def test_table_update_output_rejects_non_string_values_for_declared_string():
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


def test_table_update_output_validates_against_output_table_contract():
    class SchemaUpdater(TimeIndexTableUpdater):
        def dependencies(self):
            return {}

        def update(self):
            return pd.DataFrame()

    frame = pd.DataFrame(
        {"value": ["1.0"]},
        index=pd.DatetimeIndex([pd.Timestamp("2026-04-13T00:00:00Z")], name="time_index"),
    )
    node = SchemaUpdater.__new__(SchemaUpdater)
    node.output_table = _platform_output_model(
        _meta_table(
            columns=[{"name": "value", "data_type": "float64"}],
            data_source={
                "uid": "data-source-uid",
                "class_type": "timescale_db",
            },
        )
    )

    with pytest.raises(TypeError, match="declared as float64"):
        node._validate_update_output(frame)


def test_table_update_returning_none_is_invalid():
    class NoneReturningUpdater(TimeIndexTableUpdater):
        def dependencies(self):
            return {}

        def update(self):
            return None

    node = NoneReturningUpdater.__new__(NoneReturningUpdater)
    node._logger = type("LoggerStub", (), {"debug": lambda *args, **kwargs: None})()

    with pytest.raises(Exception, match="needs to return a data frame"):
        node._execute_local_update(table_update_run=None)


def test_table_update_filters_when_table_update_run_stats_have_max_time(monkeypatch):
    class FilteringUpdater(TimeIndexTableUpdater):
        def dependencies(self):
            return {}

        def update(self):
            return pd.DataFrame(
                {"value": [1, 2]},
                index=pd.DatetimeIndex(
                    [
                        pd.Timestamp("2026-04-13T00:00:00Z"),
                        pd.Timestamp("2026-04-14T00:00:00Z"),
                    ],
                    name="time_index",
                ),
            )

    class LoggerStub:
        def debug(self, *args, **kwargs):
            return None

        def info(self, *args, **kwargs):
            return None

        def warning(self, *args, **kwargs):
            return None

    class TimeIndexTableUpdateManagerStub:
        def __init__(self):
            self.persisted = []

        def persist_updated_data(self, *, temp_df, overwrite=False):
            self.persisted.append((temp_df.copy(), overwrite))
            return True

    class UpdateStatisticsStub:
        def __init__(self):
            self.filter_calls = 0

        def filter_df_by_latest_value(self, df):
            self.filter_calls += 1
            return df.iloc[[1]]

    monkeypatch.setattr(updaters_module, "SessionDataSource", SimpleNamespace(is_local_db=False))

    node = FilteringUpdater.__new__(FilteringUpdater)
    node._logger = LoggerStub()
    node.update_statistics = UpdateStatisticsStub()
    node._update_manager = TimeIndexTableUpdateManagerStub()
    node._validate_update_output = lambda temp_df: None
    table_update_run = SimpleNamespace(
        update_statistics=SimpleNamespace(max_time_index_value=pd.Timestamp("2026-04-13T00:00:00Z"))
    )

    result = node._execute_local_update(table_update_run=table_update_run)

    assert node.update_statistics.filter_calls == 1
    assert result["value"].tolist() == [2]
    [(persisted_df, overwrite)] = node.update_manager.persisted
    assert persisted_df["value"].tolist() == [2]
    assert overwrite is True


def test_table_update_does_not_filter_without_historical_max_time(monkeypatch):
    class FilteringUpdater(TimeIndexTableUpdater):
        def dependencies(self):
            return {}

        def update(self):
            return pd.DataFrame(
                {"value": [1, 2]},
                index=pd.DatetimeIndex(
                    [
                        pd.Timestamp("2026-04-13T00:00:00Z"),
                        pd.Timestamp("2026-04-14T00:00:00Z"),
                    ],
                    name="time_index",
                ),
            )

    class LoggerStub:
        def debug(self, *args, **kwargs):
            return None

        def info(self, *args, **kwargs):
            return None

        def warning(self, *args, **kwargs):
            return None

    class TimeIndexTableUpdateManagerStub:
        def __init__(self):
            self.persisted = []

        def persist_updated_data(self, *, temp_df, overwrite=False):
            self.persisted.append((temp_df.copy(), overwrite))
            return True

    class UpdateStatisticsStub:
        def __init__(self):
            self.filter_calls = 0

        def filter_df_by_latest_value(self, df):
            self.filter_calls += 1
            return df.iloc[[1]]

    monkeypatch.setattr(updaters_module, "SessionDataSource", SimpleNamespace(is_local_db=False))

    node = FilteringUpdater.__new__(FilteringUpdater)
    node._logger = LoggerStub()
    node.update_statistics = UpdateStatisticsStub()
    node._update_manager = TimeIndexTableUpdateManagerStub()
    node._validate_update_output = lambda temp_df: None
    table_update_run = SimpleNamespace(update_statistics=SimpleNamespace(max_time_index_value=None))

    result = node._execute_local_update(table_update_run=table_update_run)

    assert node.update_statistics.filter_calls == 0
    assert result["value"].tolist() == [1, 2]
    [(persisted_df, overwrite)] = node.update_manager.persisted
    assert persisted_df["value"].tolist() == [1, 2]
    assert overwrite is False
