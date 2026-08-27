from __future__ import annotations

import datetime
import os
import uuid

import pytest
from pydantic import BaseModel, Field

os.environ.setdefault("MAINSEQUENCE_ACCESS_TOKEN", "test-access-token")
os.environ.setdefault("MAINSEQUENCE_REFRESH_TOKEN", "test-refresh-token")

import mainsequence.meta_tables.time_index_table_updates.configuration as configuration
from mainsequence.client.metatables import TimeIndexMetaTable
from mainsequence.meta_tables import (
    PlatformTimeIndexMetaTable,
    TimeIndexTableUpdateConfig,
    TimeIndexTableUpdater,
)


def _hashes(payload):
    serialized_payload = configuration.serialize_argument(payload)
    return configuration.hash_signature({"config": serialized_payload})


class UUIDUpdateConfig(BaseModel):
    account_uid: uuid.UUID


def test_create_config_crops_hash_prefix_to_postgres_identifier_limit(monkeypatch):
    class_name = "VeryLongTimeIndexTableUpdaterClassNameThatWouldOverflowPostgresIdentifierLimit"

    config = configuration.create_config(
        updater_class_name=class_name,
        kwargs={"identifier": "prices"},
    )

    expected_prefix = class_name.lower()[:30].rstrip("_")
    assert len(config.storage_hash) == 63
    assert len(config.update_hash) == 63
    assert config.storage_hash.startswith(f"{expected_prefix}_")
    assert config.update_hash.startswith(f"{expected_prefix}_")
    assert len(config.storage_hash.rsplit("_", 1)[1]) == 32
    assert len(config.update_hash.rsplit("_", 1)[1]) == 32


def test_nested_pydantic_hash_excluded_fields_inside_lists_do_not_affect_hashes(monkeypatch):
    class ColumnContract(BaseModel):
        column_name: str
        dtype: str
        label: str | None = Field(default=None, json_schema_extra={"hash_excluded": True})
        description: str | None = Field(default=None, json_schema_extra={"hash_excluded": True})

    class UpdateConfig(BaseModel):
        records: list[ColumnContract]

    hashes_a = _hashes(
        UpdateConfig(
            records=[
                ColumnContract(
                    column_name="close",
                    dtype="float64",
                    label="Close",
                    description="Close price",
                )
            ]
        )
    )
    hashes_b = _hashes(
        UpdateConfig(
            records=[
                ColumnContract(
                    column_name="close",
                    dtype="float64",
                    label="Last",
                    description="Last traded close",
                )
            ]
        )
    )

    assert hashes_a == hashes_b


def test_normal_config_value_changes_update_hash(monkeypatch):
    class UpdateConfig(BaseModel):
        shard_id: str
        identifier: str

    update_hash_a, storage_hash_a = _hashes(UpdateConfig(shard_id="A", identifier="prices"))
    update_hash_b, storage_hash_b = _hashes(UpdateConfig(shard_id="B", identifier="prices"))

    assert update_hash_a != update_hash_b
    assert storage_hash_a != storage_hash_b


def test_hash_excluded_metadata_does_not_affect_hashes(monkeypatch):
    class UpdateConfig(BaseModel):
        identifier: str
        label: str = Field(..., json_schema_extra={"hash_excluded": True})

    hashes_a = _hashes(UpdateConfig(identifier="prices", label="Close"))
    hashes_b = _hashes(UpdateConfig(identifier="prices", label="Last"))

    assert hashes_a == hashes_b


def test_hash_excluded_nested_metadata_does_not_affect_hashes(monkeypatch):
    class PublishedMetadata(BaseModel):
        identifier: str | None = Field(default=None, json_schema_extra={"hash_excluded": True})
        description: str | None = Field(default=None, json_schema_extra={"hash_excluded": True})
        data_frequency_id: str | None = Field(
            default=None,
            json_schema_extra={"hash_excluded": True},
        )

    class UpdateConfig(BaseModel):
        identifier: str
        published_metadata: PublishedMetadata | None = Field(
            default=None,
            json_schema_extra={"hash_excluded": True},
        )

    hashes_a = _hashes(UpdateConfig(identifier="prices"))
    hashes_b = _hashes(
        UpdateConfig(
            identifier="prices",
            published_metadata=PublishedMetadata(
                identifier="daily_prices",
                description="Published alias",
                data_frequency_id="one_d",
            ),
        )
    )

    assert hashes_a == hashes_b


def test_offset_start_changes_update_hash(monkeypatch):
    update_hash_a, storage_hash_a = _hashes(
        TimeIndexTableUpdateConfig(offset_start=datetime.datetime(2024, 1, 1, tzinfo=datetime.UTC))
    )
    update_hash_b, storage_hash_b = _hashes(
        TimeIndexTableUpdateConfig(offset_start=datetime.datetime(2025, 1, 1, tzinfo=datetime.UTC))
    )

    assert update_hash_a != update_hash_b
    assert storage_hash_a != storage_hash_b


def test_platform_time_index_meta_table_config_hashes_by_bound_metatable_uid(monkeypatch):
    class StorageA(PlatformTimeIndexMetaTable):
        pass

    class StorageB(PlatformTimeIndexMetaTable):
        pass

    class StorageC(PlatformTimeIndexMetaTable):
        pass

    StorageA._bind_meta_table(
        TimeIndexMetaTable.model_construct(uid="storage-uid-a", data_source_uid="data-source-uid")
    )
    StorageB._bind_meta_table(
        TimeIndexMetaTable.model_construct(uid="storage-uid-a", data_source_uid="data-source-uid")
    )
    StorageC._bind_meta_table(
        TimeIndexMetaTable.model_construct(uid="storage-uid-c", data_source_uid="data-source-uid")
    )

    class UpdateConfig(BaseModel):
        dependency_storage: type[PlatformTimeIndexMetaTable]

    hashes_a = _hashes(UpdateConfig(dependency_storage=StorageA))
    hashes_b = _hashes(UpdateConfig(dependency_storage=StorageB))
    hashes_c = _hashes(UpdateConfig(dependency_storage=StorageC))

    assert hashes_a == hashes_b
    assert hashes_a != hashes_c

    config = configuration.create_config(
        updater_class_name="StorageConfigUpdater",
        kwargs={"config": UpdateConfig(dependency_storage=StorageA)},
    )
    assert (
        config.local_initial_configuration["config"]["serialized_model"]["dependency_storage"][
            "uid"
        ]
        == "storage-uid-a"
    )


def test_platform_time_index_meta_table_config_requires_registered_before_hashing(monkeypatch):
    class AutoStorage(PlatformTimeIndexMetaTable):
        pass

    class UpdateConfig(BaseModel):
        dependency_storage: type[PlatformTimeIndexMetaTable]

    with pytest.raises(ValueError, match="migrations upgrade"):
        configuration.create_config(
            updater_class_name="AutoStorageConfigUpdater",
            kwargs={"config": UpdateConfig(dependency_storage=AutoStorage)},
        )


def test_uuid_config_values_serialize_hash_and_rebuild(monkeypatch):
    account_uid = uuid.UUID("00000000-0000-4000-8000-000000000001")
    other_account_uid = uuid.UUID("00000000-0000-4000-8000-000000000002")

    serialized = configuration.serialize_argument(UUIDUpdateConfig(account_uid=account_uid))
    assert serialized["serialized_model"]["account_uid"] == str(account_uid)

    hashes_a = _hashes(UUIDUpdateConfig(account_uid=account_uid))
    hashes_b = _hashes(UUIDUpdateConfig(account_uid=account_uid))
    hashes_c = _hashes(UUIDUpdateConfig(account_uid=other_account_uid))

    assert hashes_a == hashes_b
    assert hashes_a != hashes_c

    config = configuration.create_config(
        updater_class_name="UUIDConfigUpdater",
        kwargs={"config": UUIDUpdateConfig(account_uid=account_uid)},
    )
    rebuilt = configuration.DeserializerManager().rebuild_serialized_config(
        config.local_initial_configuration,
        updater_class_name="UUIDConfigUpdater",
    )

    assert rebuilt["config"].account_uid == account_uid
    assert isinstance(rebuilt["config"].account_uid, uuid.UUID)


def test_plain_dict_with_pydantic_model_import_path_key_is_not_treated_as_wrapper(monkeypatch):
    payload_a = {
        "config": {
            "pydantic_model_import_path": {"module": "alpha", "qualname": "Beta"},
            "other": 1,
        }
    }
    payload_b = {
        "config": {
            "pydantic_model_import_path": {"module": "alpha", "qualname": "Beta"},
            "other": 2,
        }
    }

    assert configuration._is_serialized_pydantic_model(payload_a["config"]) is False
    assert configuration.hash_signature(payload_a) != configuration.hash_signature(payload_b)


def test_time_index_table_configuration_overrides_offset_start():
    class UpdateConfig(TimeIndexTableUpdateConfig):
        pass

    class ConfigurableTimeIndexTableUpdater(TimeIndexTableUpdater):
        OFFSET_START = datetime.datetime(2018, 1, 1, tzinfo=datetime.UTC)

        def __init__(self, update_config: UpdateConfig, *args, **kwargs):
            self.update_config = update_config
            super().__init__(update_config, *args, **kwargs)

        def dependencies(self):
            return {}

        def update(self):
            return None

    updater = object.__new__(ConfigurableTimeIndexTableUpdater)
    updater.config = UpdateConfig(
        offset_start=datetime.datetime(2024, 1, 1, tzinfo=datetime.UTC),
    )

    assert updater.get_offset_start() == datetime.datetime(2024, 1, 1, tzinfo=datetime.UTC)

    updater._hash_namespace = "test"
    assert updater.get_offset_start() == datetime.datetime(2024, 1, 1, tzinfo=datetime.UTC)


def test_subclass_missing_super_config_is_rejected():
    class UpdateConfig(TimeIndexTableUpdateConfig):
        scope: str

    class InvalidWrappedConfigUpdater(TimeIndexTableUpdater):
        def __init__(self, update_config: UpdateConfig, *args, **kwargs):
            self.update_config = update_config
            super().__init__(*args, **kwargs)

        def dependencies(self):
            return {}

        def update(self):
            return None

    with pytest.raises(TypeError, match="config"):
        InvalidWrappedConfigUpdater(update_config=UpdateConfig(scope="desk_a"))
