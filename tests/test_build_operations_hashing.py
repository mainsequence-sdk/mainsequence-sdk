from __future__ import annotations

import datetime
import os

import pytest
from pydantic import BaseModel, Field

os.environ.setdefault("MAINSEQUENCE_ACCESS_TOKEN", "test-access-token")
os.environ.setdefault("MAINSEQUENCE_REFRESH_TOKEN", "test-refresh-token")

import mainsequence.tdag.data_nodes.build_operations as build_operations
from mainsequence.tdag import DataNode, DataNodeConfiguration


def _hashes(payload):
    serialized_payload = build_operations.serialize_argument(payload, pickle_ts=False)
    return build_operations.hash_signature({"config": serialized_payload})


def test_nested_pydantic_runtime_only_fields_inside_lists_do_not_affect_hashes(monkeypatch):
    monkeypatch.setattr(build_operations, "POD_PROJECT", None)

    class RecordDefinition(BaseModel):
        column_name: str
        dtype: str
        label: str | None = Field(default=None, json_schema_extra={"runtime_only": True})
        description: str | None = Field(default=None, json_schema_extra={"runtime_only": True})

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


def test_update_only_value_changes_update_hash_but_not_storage_hash(monkeypatch):
    monkeypatch.setattr(build_operations, "POD_PROJECT", None)

    class NodeConfig(BaseModel):
        shard_id: str = Field(..., json_schema_extra={"update_only": True})
        identifier: str

    update_hash_a, storage_hash_a = _hashes(NodeConfig(shard_id="A", identifier="prices"))
    update_hash_b, storage_hash_b = _hashes(NodeConfig(shard_id="B", identifier="prices"))

    assert update_hash_a != update_hash_b
    assert storage_hash_a == storage_hash_b


def test_update_only_metadata_changes_storage_hash_only(monkeypatch):
    monkeypatch.setattr(build_operations, "POD_PROJECT", None)

    class NodeConfig(BaseModel):
        scope: str
        identifier: str

    config = NodeConfig(scope="desk_a", identifier="prices")
    field_info = NodeConfig.model_fields["scope"]
    original_extra = field_info.json_schema_extra

    try:
        update_hash_normal, storage_hash_normal = _hashes(config)
        field_info.json_schema_extra = {"update_only": True}
        update_hash_update_only, storage_hash_update_only = _hashes(config)
    finally:
        field_info.json_schema_extra = original_extra

    assert update_hash_normal == update_hash_update_only
    assert storage_hash_normal != storage_hash_update_only


def test_runtime_only_value_changes_neither_hash(monkeypatch):
    monkeypatch.setattr(build_operations, "POD_PROJECT", None)

    class NodeConfig(BaseModel):
        identifier: str
        label: str = Field(..., json_schema_extra={"runtime_only": True})

    hashes_a = _hashes(NodeConfig(identifier="prices", label="Close"))
    hashes_b = _hashes(NodeConfig(identifier="prices", label="Last"))

    assert hashes_a == hashes_b


def test_runtime_only_node_metadata_does_not_affect_hashes(monkeypatch):
    monkeypatch.setattr(build_operations, "POD_PROJECT", None)

    class DataNodeMetaData(BaseModel):
        identifier: str | None = Field(default=None, json_schema_extra={"runtime_only": True})
        description: str | None = Field(default=None, json_schema_extra={"runtime_only": True})
        data_frequency_id: str | None = Field(default=None, json_schema_extra={"runtime_only": True})

    class NodeConfig(BaseModel):
        identifier: str
        node_metadata: DataNodeMetaData | None = Field(
            default=None,
            json_schema_extra={"runtime_only": True},
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


def test_offset_start_changes_update_hash_but_not_storage_hash(monkeypatch):
    monkeypatch.setattr(build_operations, "POD_PROJECT", None)

    update_hash_a, storage_hash_a = _hashes(
        DataNodeConfiguration(offset_start=datetime.datetime(2024, 1, 1, tzinfo=datetime.UTC))
    )
    update_hash_b, storage_hash_b = _hashes(
        DataNodeConfiguration(offset_start=datetime.datetime(2025, 1, 1, tzinfo=datetime.UTC))
    )

    assert update_hash_a != update_hash_b
    assert storage_hash_a == storage_hash_b


def test_open_to_public_changes_neither_hash(monkeypatch):
    monkeypatch.setattr(build_operations, "POD_PROJECT", None)

    hashes_a = _hashes(DataNodeConfiguration(open_to_public=False))
    hashes_b = _hashes(DataNodeConfiguration(open_to_public=True))

    assert hashes_a == hashes_b


def test_plain_dict_with_pydantic_model_import_path_key_is_not_treated_as_wrapper(monkeypatch):
    monkeypatch.setattr(build_operations, "POD_PROJECT", None)

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
        build_operations.serialize_argument(LegacyConfig(shard_id="desk_a"), pickle_ts=False)


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


def test_data_node_configuration_overrides_offset_start_and_open_to_public():
    class NodeConfig(DataNodeConfiguration):
        pass

    class ConfigurableDataNode(DataNode):
        OFFSET_START = datetime.datetime(2018, 1, 1, tzinfo=datetime.UTC)
        OPEN_TO_PUBLIC = True
        TEST_OFFSET_START = datetime.datetime(2030, 1, 1, tzinfo=datetime.UTC)

        def __init__(self, node_config: NodeConfig, *args, **kwargs):
            self.node_config = node_config
            super().__init__(config=node_config, *args, **kwargs)

        def dependencies(self):
            return {}

        def update(self):
            return None

    node = object.__new__(ConfigurableDataNode)
    node.config = NodeConfig(
        offset_start=datetime.datetime(2024, 1, 1, tzinfo=datetime.UTC),
        open_to_public=True,
    )

    assert node.get_offset_start() == datetime.datetime(2024, 1, 1, tzinfo=datetime.UTC)
    assert node.get_open_to_public() is True

    node.config = NodeConfig()
    assert node.get_open_to_public() is True

    node._hash_namespace = "test"
    assert node.get_offset_start() == ConfigurableDataNode.TEST_OFFSET_START


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
