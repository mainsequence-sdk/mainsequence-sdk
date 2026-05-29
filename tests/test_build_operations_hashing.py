from __future__ import annotations

import datetime
import os

import pytest
from pydantic import BaseModel, Field

os.environ.setdefault("MAINSEQUENCE_ACCESS_TOKEN", "test-access-token")
os.environ.setdefault("MAINSEQUENCE_REFRESH_TOKEN", "test-refresh-token")

import mainsequence.meta_tables.data_nodes.build_operations as build_operations
from mainsequence.client.models_metatables import MetaTable
from mainsequence.meta_tables import (
    DataNode,
    DataNodeConfiguration,
    RecordDefinition,
    SourceTableForeignKey,
)


def _hashes(payload):
    serialized_payload = build_operations.serialize_argument(payload)
    return build_operations.hash_signature({"config": serialized_payload})


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

    class RecordDefinition(BaseModel):
        column_name: str
        dtype: str
        label: str | None = Field(default=None, json_schema_extra={"hash_excluded": True})
        description: str | None = Field(default=None, json_schema_extra={"hash_excluded": True})

    class NodeConfig(BaseModel):
        records: list[RecordDefinition]

    hashes_a = _hashes(
        NodeConfig(
            records=[
                RecordDefinition(
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
                RecordDefinition(
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


def test_update_only_metadata_is_rejected():
    class NodeConfig(BaseModel):
        shard_id: str = Field(..., json_schema_extra={"update_only": True})

    with pytest.raises(ValueError, match="update_only"):
        build_operations.serialize_argument(NodeConfig(shard_id="desk_a"))


def test_runtime_only_metadata_is_rejected():
    class NodeConfig(BaseModel):
        label: str = Field(..., json_schema_extra={"runtime_only": True})

    with pytest.raises(ValueError, match="runtime_only"):
        build_operations.serialize_argument(NodeConfig(label="Close"))


def test_hash_excluded_metadata_does_not_affect_hashes(monkeypatch):
    monkeypatch.setattr(build_operations, "POD_PROJECT", None, raising=False)

    class NodeConfig(BaseModel):
        identifier: str
        label: str = Field(..., json_schema_extra={"hash_excluded": True})

    hashes_a = _hashes(NodeConfig(identifier="prices", label="Close"))
    hashes_b = _hashes(NodeConfig(identifier="prices", label="Last"))

    assert hashes_a == hashes_b


def test_hash_excluded_node_metadata_does_not_affect_hashes(monkeypatch):
    monkeypatch.setattr(build_operations, "POD_PROJECT", None, raising=False)

    class DataNodeMetaData(BaseModel):
        identifier: str | None = Field(default=None, json_schema_extra={"hash_excluded": True})
        description: str | None = Field(default=None, json_schema_extra={"hash_excluded": True})
        data_frequency_id: str | None = Field(
            default=None,
            json_schema_extra={"hash_excluded": True},
        )

    class NodeConfig(BaseModel):
        identifier: str
        node_metadata: DataNodeMetaData | None = Field(
            default=None,
            json_schema_extra={"hash_excluded": True},
        )

    hashes_a = _hashes(NodeConfig(identifier="prices"))
    hashes_b = _hashes(
        NodeConfig(
            identifier="prices",
            node_metadata=DataNodeMetaData(
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


def test_source_table_foreign_key_hash_uses_target_meta_table_uid(monkeypatch):
    monkeypatch.setattr(build_operations, "POD_PROJECT", None, raising=False)

    class ColumnRef:
        def __init__(self, name: str):
            self.name = name

    asset_uid = RecordDefinition(column_name="asset_uid", dtype="uuid")
    value = RecordDefinition(column_name="value", dtype="float64")

    target_a = MetaTable(
        uid="asset-meta-table-uid",
        data_source_uid="data-source-1",
        storage_hash="asset_storage_v1",
        management_mode="platform_managed",
        physical_table_name="asset_storage_v1",
    )
    target_b = MetaTable(
        uid="asset-meta-table-uid",
        data_source_uid="data-source-1",
        storage_hash="asset_storage_v2",
        management_mode="platform_managed",
        physical_table_name="asset_storage_v2",
    )
    target_c = MetaTable(
        uid="other-asset-meta-table-uid",
        data_source_uid="data-source-1",
        storage_hash="asset_storage_v1",
        management_mode="platform_managed",
        physical_table_name="asset_storage_v1",
    )

    config_a = DataNodeConfiguration(
        records=[asset_uid, value],
        foreign_keys=[
            SourceTableForeignKey(
                target=target_a,
                source_columns=[asset_uid],
                target_columns=[ColumnRef("uid")],
            )
        ],
    )
    config_b = DataNodeConfiguration(
        records=[asset_uid, value],
        foreign_keys=[
            SourceTableForeignKey(
                target=target_b,
                source_columns=[asset_uid],
                target_columns=[ColumnRef("uid")],
            )
        ],
    )
    config_c = DataNodeConfiguration(
        records=[asset_uid, value],
        foreign_keys=[
            SourceTableForeignKey(
                target=target_c,
                source_columns=[asset_uid],
                target_columns=[ColumnRef("uid")],
            )
        ],
    )

    assert _hashes(config_a) == _hashes(config_b)
    assert _hashes(config_a) != _hashes(config_c)


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


def test_legacy_ignore_from_storage_hash_metadata_is_rejected():
    class LegacyConfig(BaseModel):
        shard_id: str = Field(..., json_schema_extra={"ignore_from_storage_hash": True})

    with pytest.raises(ValueError, match="ignore_from_storage_hash"):
        build_operations.serialize_argument(LegacyConfig(shard_id="desk_a"))


def test_legacy_args_ignore_in_storage_hash_class_attribute_is_rejected():
    with pytest.raises(TypeError, match="_ARGS_IGNORE_IN_STORAGE_HASH"):

        class LegacyDataNode(DataNode):
            _ARGS_IGNORE_IN_STORAGE_HASH = ["asset_list"]

            def __init__(self, *args, **kwargs):
                super().__init__(*args, **kwargs)

            def dependencies(self):
                return {}

            def update(self):
                return None


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
