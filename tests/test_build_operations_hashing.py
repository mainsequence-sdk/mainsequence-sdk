from __future__ import annotations

import datetime
import os
import uuid

import pytest
from pydantic import BaseModel, Field

os.environ.setdefault("MAINSEQUENCE_ACCESS_TOKEN", "test-access-token")
os.environ.setdefault("MAINSEQUENCE_REFRESH_TOKEN", "test-refresh-token")

import mainsequence.meta_tables.data_nodes.build_operations as build_operations
from mainsequence.client.metatables import TimeIndexMetaTable
from mainsequence.meta_tables import (
    DataNode,
    DataNodeConfiguration,
    PlatformTimeIndexMetaTable,
)


def _hashes(payload):
    serialized_payload = build_operations.serialize_argument(payload)
    return build_operations.hash_signature({"config": serialized_payload})


class UUIDNodeConfig(BaseModel):
    account_uid: uuid.UUID


def test_create_config_crops_hash_prefix_to_postgres_identifier_limit(monkeypatch):
    monkeypatch.setattr(build_operations, "POD_PROJECT", None, raising=False)

    class_name = "VeryLongDataNodeClassNameThatWouldOverflowPostgresIdentifierLimit"

    config = build_operations.create_config(
        ts_class_name=class_name,
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
    monkeypatch.setattr(build_operations, "POD_PROJECT", None, raising=False)

    class ColumnContract(BaseModel):
        column_name: str
        dtype: str
        label: str | None = Field(default=None, json_schema_extra={"hash_excluded": True})
        description: str | None = Field(default=None, json_schema_extra={"hash_excluded": True})

    class NodeConfig(BaseModel):
        records: list[ColumnContract]

    hashes_a = _hashes(
        NodeConfig(
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
        NodeConfig(
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
    monkeypatch.setattr(build_operations, "POD_PROJECT", None, raising=False)

    class NodeConfig(BaseModel):
        shard_id: str
        identifier: str

    update_hash_a, storage_hash_a = _hashes(NodeConfig(shard_id="A", identifier="prices"))
    update_hash_b, storage_hash_b = _hashes(NodeConfig(shard_id="B", identifier="prices"))

    assert update_hash_a != update_hash_b
    assert storage_hash_a != storage_hash_b


def test_hash_excluded_metadata_does_not_affect_hashes(monkeypatch):
    monkeypatch.setattr(build_operations, "POD_PROJECT", None, raising=False)

    class NodeConfig(BaseModel):
        identifier: str
        label: str = Field(..., json_schema_extra={"hash_excluded": True})

    hashes_a = _hashes(NodeConfig(identifier="prices", label="Close"))
    hashes_b = _hashes(NodeConfig(identifier="prices", label="Last"))

    assert hashes_a == hashes_b


def test_hash_excluded_nested_metadata_does_not_affect_hashes(monkeypatch):
    monkeypatch.setattr(build_operations, "POD_PROJECT", None, raising=False)

    class PublishedMetadata(BaseModel):
        identifier: str | None = Field(default=None, json_schema_extra={"hash_excluded": True})
        description: str | None = Field(default=None, json_schema_extra={"hash_excluded": True})
        data_frequency_id: str | None = Field(
            default=None,
            json_schema_extra={"hash_excluded": True},
        )

    class NodeConfig(BaseModel):
        identifier: str
        published_metadata: PublishedMetadata | None = Field(
            default=None,
            json_schema_extra={"hash_excluded": True},
        )

    hashes_a = _hashes(NodeConfig(identifier="prices"))
    hashes_b = _hashes(
        NodeConfig(
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
    monkeypatch.setattr(build_operations, "POD_PROJECT", None, raising=False)

    update_hash_a, storage_hash_a = _hashes(
        DataNodeConfiguration(offset_start=datetime.datetime(2024, 1, 1, tzinfo=datetime.UTC))
    )
    update_hash_b, storage_hash_b = _hashes(
        DataNodeConfiguration(offset_start=datetime.datetime(2025, 1, 1, tzinfo=datetime.UTC))
    )

    assert update_hash_a != update_hash_b
    assert storage_hash_a != storage_hash_b


def test_platform_time_index_metadata_config_hashes_by_bound_metadata_uid(monkeypatch):
    monkeypatch.setattr(build_operations, "POD_PROJECT", None, raising=False)

    class StorageA(PlatformTimeIndexMetaTable):
        pass

    class StorageB(PlatformTimeIndexMetaTable):
        pass

    class StorageC(PlatformTimeIndexMetaTable):
        pass

    StorageA._bind_meta_table(TimeIndexMetaTable.model_construct(uid="storage-uid-a"))
    StorageB._bind_meta_table(TimeIndexMetaTable.model_construct(uid="storage-uid-a"))
    StorageC._bind_meta_table(TimeIndexMetaTable.model_construct(uid="storage-uid-c"))

    class NodeConfig(BaseModel):
        dependency_storage: type[PlatformTimeIndexMetaTable]

    hashes_a = _hashes(NodeConfig(dependency_storage=StorageA))
    hashes_b = _hashes(NodeConfig(dependency_storage=StorageB))
    hashes_c = _hashes(NodeConfig(dependency_storage=StorageC))

    assert hashes_a == hashes_b
    assert hashes_a != hashes_c

    config = build_operations.create_config(
        ts_class_name="StorageConfigNode",
        kwargs={"config": NodeConfig(dependency_storage=StorageA)},
    )
    assert (
        config.local_initial_configuration["config"]["serialized_model"]["dependency_storage"][
            "uid"
        ]
        == "storage-uid-a"
    )


def test_platform_time_index_metadata_config_requires_registered_before_hashing(monkeypatch):
    monkeypatch.setattr(build_operations, "POD_PROJECT", None, raising=False)

    class AutoStorage(PlatformTimeIndexMetaTable):
        pass

    class NodeConfig(BaseModel):
        dependency_storage: type[PlatformTimeIndexMetaTable]

    with pytest.raises(ValueError, match="migrations upgrade"):
        build_operations.create_config(
            ts_class_name="AutoStorageConfigNode",
            kwargs={"config": NodeConfig(dependency_storage=AutoStorage)},
        )


def test_uuid_config_values_serialize_hash_and_rebuild(monkeypatch):
    monkeypatch.setattr(build_operations, "POD_PROJECT", None, raising=False)

    account_uid = uuid.UUID("00000000-0000-4000-8000-000000000001")
    other_account_uid = uuid.UUID("00000000-0000-4000-8000-000000000002")

    serialized = build_operations.serialize_argument(UUIDNodeConfig(account_uid=account_uid))
    assert serialized["serialized_model"]["account_uid"] == str(account_uid)

    hashes_a = _hashes(UUIDNodeConfig(account_uid=account_uid))
    hashes_b = _hashes(UUIDNodeConfig(account_uid=account_uid))
    hashes_c = _hashes(UUIDNodeConfig(account_uid=other_account_uid))

    assert hashes_a == hashes_b
    assert hashes_a != hashes_c

    config = build_operations.create_config(
        ts_class_name="UUIDConfigNode",
        kwargs={"config": UUIDNodeConfig(account_uid=account_uid)},
    )
    rebuilt = build_operations.DeserializerManager().rebuild_serialized_config(
        config.local_initial_configuration,
        time_serie_class_name="UUIDConfigNode",
    )

    assert rebuilt["config"].account_uid == account_uid
    assert isinstance(rebuilt["config"].account_uid, uuid.UUID)


def test_plain_dict_with_pydantic_model_import_path_key_is_not_treated_as_wrapper(monkeypatch):
    monkeypatch.setattr(build_operations, "POD_PROJECT", None, raising=False)

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

    assert build_operations._is_serialized_pydantic_model(payload_a["config"]) is False
    assert build_operations.hash_signature(payload_a) != build_operations.hash_signature(payload_b)


def test_data_node_configuration_overrides_offset_start():
    class NodeConfig(DataNodeConfiguration):
        pass

    class ConfigurableDataNode(DataNode):
        OFFSET_START = datetime.datetime(2018, 1, 1, tzinfo=datetime.UTC)

        def __init__(self, node_config: NodeConfig, *args, **kwargs):
            self.node_config = node_config
            super().__init__(node_config, *args, **kwargs)

        def dependencies(self):
            return {}

        def update(self):
            return None

    node = object.__new__(ConfigurableDataNode)
    node.config = NodeConfig(
        offset_start=datetime.datetime(2024, 1, 1, tzinfo=datetime.UTC),
    )

    assert node.get_offset_start() == datetime.datetime(2024, 1, 1, tzinfo=datetime.UTC)

    node._hash_namespace = "test"
    assert node.get_offset_start() == datetime.datetime(2024, 1, 1, tzinfo=datetime.UTC)


def test_subclass_missing_super_config_is_rejected():
    class NodeConfig(DataNodeConfiguration):
        scope: str

    class InvalidWrappedConfigNode(DataNode):
        def __init__(self, node_config: NodeConfig, *args, **kwargs):
            self.node_config = node_config
            super().__init__(*args, **kwargs)

        def dependencies(self):
            return {}

        def update(self):
            return None

    with pytest.raises(TypeError, match="config"):
        InvalidWrappedConfigNode(node_config=NodeConfig(scope="desk_a"))
