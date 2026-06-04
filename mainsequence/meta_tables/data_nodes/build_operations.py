from __future__ import annotations

import collections
import copy
import datetime
import hashlib
import importlib
import json
from abc import ABC, abstractmethod
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from enum import Enum
from functools import singledispatch
from types import SimpleNamespace
from typing import TYPE_CHECKING, Any
from uuid import UUID

from pydantic import BaseModel

from mainsequence.client import BaseObjectOrm
from mainsequence.client.metatables.core import _resolve_local_pod_project
from mainsequence.client.models_helpers import get_model_class
from mainsequence.meta_tables.pydantic_metadata import (
    is_serialized_pydantic_model,
    serialize_pydantic_model,
    strip_pydantic_hash_exclusions,
)

if TYPE_CHECKING:
    from .data_nodes import APIDataNode, DataNode


def build_model(model_data):
    return get_model_class(model_data["orm_class"])(**model_data)


POSTGRES_IDENTIFIER_MAX_LENGTH = 63
_HASH_SUFFIX_LENGTH = 33


# 1. Create a "registry" function using the decorator
@singledispatch
def serialize_argument(value: Any) -> Any:
    """
    Default implementation for any type not specifically registered.
    It can either return the value as is or raise a TypeError.
    """
    # For types we don't explicitly handle, we can check if they are serializable
    # or just return them. For simplicity, we return as is.
    return value


def _serialize_timeserie(value: DataNode) -> dict[str, Any]:
    """Serialization logic for DataNode objects."""
    return {
        "is_time_serie_instance": True,
        "update_hash": value.update_hash,
        "data_source_uid": str(value.data_source_uid),
    }


def _serialize_api_timeserie(value: APIDataNode) -> dict[str, Any]:
    return {
        "is_api_time_serie_instance": True,
        "update_hash": value.update_hash,
        "data_source_uid": str(value.data_source_uid),
    }


def _import_qualified_name(module_name: str, qualname: str) -> Any:
    module = importlib.import_module(module_name)
    value: Any = module
    for part in qualname.split("."):
        value = getattr(value, part)
    return value


def _is_platform_time_index_meta_table_class(value: Any) -> bool:
    try:
        from mainsequence.meta_tables.sqlalchemy_contracts import PlatformTimeIndexMetaTable
    except ImportError:
        return False

    try:
        return isinstance(value, type) and issubclass(value, PlatformTimeIndexMetaTable)
    except TypeError:
        return False


@serialize_argument.register(type)
def _(value: type[Any]) -> Any:
    if not _is_platform_time_index_meta_table_class(value):
        return value

    time_index_meta_table = value.get_time_index_meta_table()
    uid = getattr(time_index_meta_table, "uid", None)
    if uid in (None, ""):
        raise ValueError(
            "PlatformTimeIndexMetaTable config value is not registered. Run "
            "`mainsequence migrations upgrade --provider <provider> head` "
            "before using it in DataNode configuration."
        )

    if uid in (None, ""):
        raise ValueError(
            "PlatformTimeIndexMetaTable config value is missing TimeIndexMetaTable metadata."
        )
    return {
        "__type__": "platform_time_index_meta_table",
        "uid": str(uid),
        "module": value.__module__,
        "qualname": value.__qualname__,
    }


@serialize_argument.register(datetime.datetime)
def _(value: datetime.datetime) -> str:
    return value.isoformat()


@serialize_argument.register(UUID)
def _(value: UUID) -> str:
    return str(value)


@serialize_argument.register(BaseModel)
def _(value: BaseModel) -> dict[str, Any]:
    """Serialization logic for any Pydantic BaseModel."""
    return serialize_pydantic_model(
        value,
        serialize_field=serialize_argument,
    )


def _is_serialized_pydantic_model(value: Any) -> bool:
    return is_serialized_pydantic_model(value)


def _strip_pydantic_hash_exclusions(value: Any, *, for_storage_hash: bool) -> Any:
    return strip_pydantic_hash_exclusions(value, for_storage_hash=for_storage_hash)


@serialize_argument.register(BaseObjectOrm)
def _(value):
    new_dict = json.loads(value.model_dump_json())
    if hasattr(value, "unique_identifier"):
        # Generic SDK object identity.
        new_dict["unique_identifier"] = value.unique_identifier
    return new_dict


@serialize_argument.register(list)
def _(value: list):
    if not value:
        return []

    # 1. DETECT if it's a list of ORM models
    if isinstance(value[0], BaseObjectOrm):
        # 2. SORT the list to ensure a stable hash
        # BaseObjectOrm resources expose unique_identifier as generic identity.
        sorted_value = sorted(value, key=lambda x: x.unique_identifier)

        # 3. SERIALIZE each item in the now-sorted list
        serialized_items = [serialize_argument(item) for item in sorted_value]

        # 4. WRAP the result in an identifiable structure for deserialization
        return {"__type__": "orm_model_list", "items": serialized_items}

    # Fallback for all other list types
    return [serialize_argument(item) for item in value]


@serialize_argument.register(tuple)
def _(value):
    items = [serialize_argument(item) for item in value]
    return {"__type__": "tuple", "items": items}


@serialize_argument.register(dict)
def _(value: dict):
    # Check for the special marker key.
    if value.get("is_time_series_config") is True:
        # If it's a special config dict, preserve its unique structure.
        # Serialize its contents recursively.
        config_data = {k: serialize_argument(v) for k, v in value.items()}

        return {"is_time_series_config": True, "config_data": config_data}

    # Otherwise, handle it as a regular dictionary.
    return {k: serialize_argument(v) for k, v in value.items()}


@serialize_argument.register(SimpleNamespace)
def _(value):
    return serialize_argument.dispatch(dict)(vars(value))


@serialize_argument.register(Enum)
def _(value):
    return value.value


def parse_dictionary_before_hashing(dictionary: dict[str, Any]) -> dict[str, Any]:
    """
    Parses a dictionary before hashing, handling nested structures and special types.

    Args:
        dictionary: The dictionary to parse.

    Returns:
        A new dictionary ready for hashing.
    """
    local_ts_dict_to_hash = {}
    for key, value in dictionary.items():
        local_ts_dict_to_hash[key] = value
        if isinstance(value, dict):
            if "orm_class" in value.keys():
                local_ts_dict_to_hash[key] = value["unique_identifier"]

            elif "is_time_series_config" in value.keys():
                tmp_local_ts, remote_ts = hash_signature(value["config_data"])
                local_ts_dict_to_hash[key] = {
                    "is_time_series_config": value["is_time_series_config"],
                    "config_data": tmp_local_ts,
                }

            elif isinstance(value, dict) and value.get("__type__") == "orm_model_list":
                # The value["items"] are already serialized dicts

                local_ts_dict_to_hash[key] = [v["unique_identifier"] for v in value["items"]]
            elif value.get("__type__") == "platform_time_index_meta_table":
                local_ts_dict_to_hash[key] = value["uid"]
            else:
                # recursively apply hash signature
                local_ts_dict_to_hash[key] = parse_dictionary_before_hashing(value)

    return local_ts_dict_to_hash


def hash_signature(dictionary: dict[str, Any]) -> tuple[str, str]:
    """
    Computes MD5 hashes for local and remote configurations from a single dictionary.
    """
    dhash_local = hashlib.md5()
    dhash_remote = hashlib.md5()

    # The function expects to receive the full dictionary, including meta-args
    parsed_dictionary = parse_dictionary_before_hashing(dictionary)
    local_ts_dict_to_hash = _strip_pydantic_hash_exclusions(
        parsed_dictionary, for_storage_hash=False
    )
    remote_ts_in_db_hash = _strip_pydantic_hash_exclusions(parsed_dictionary, for_storage_hash=True)

    # Add project_uid for local hash so local hashing follows the public project contract.
    resolution = _resolve_local_pod_project()
    if resolution.project is not None and getattr(resolution.project, "uid", None):
        local_ts_dict_to_hash["project_uid"] = resolution.project.uid
    # Encode and hash both versions
    encoded_local = json.dumps(local_ts_dict_to_hash, sort_keys=True).encode()
    encoded_remote = json.dumps(remote_ts_in_db_hash, sort_keys=True).encode()

    dhash_local.update(encoded_local)
    dhash_remote.update(encoded_remote)

    return dhash_local.hexdigest(), dhash_remote.hexdigest()


def rebuild_with_type(value: dict[str, Any], rebuild_function: Callable) -> tuple | Any:
    """
    Rebuilds a tuple from a serialized dictionary representation.

    Args:
        value: A dictionary with a '__type__' key.
        rebuild_function: A function to apply to each item in the tuple.

    Returns:
        A rebuilt tuple.

    Raises:
        NotImplementedError: If the type is not 'tuple'.
    """
    type_marker = value.get("__type__")

    if type_marker == "tuple":
        return tuple([rebuild_function(c) for c in value["items"]])
        # Add this block to handle the ORM model list
    elif type_marker == "orm_model_list":
        return [rebuild_function(c) for c in value["items"]]
    else:
        raise NotImplementedError


class Serializer:
    """Encapsulates the logic for converting a configuration dict into a serializable format."""

    def serialize_init_kwargs(self, kwargs: dict[str, Any]) -> dict[str, Any]:
        """
        Serializes __init__ keyword arguments for a DataNode.
        This maps to your original `serialize_init_kwargs`.
        """
        return self._serialize_dict(kwargs=kwargs)

    def _serialize_dict(self, kwargs: dict[str, Any]) -> dict[str, Any]:
        """
        Internal worker that serializes a dictionary by calling the dispatcher.
        This maps to your original `_serialize_configuration_dict`.
        """
        new_kwargs = {key: serialize_argument(value) for key, value in kwargs.items()}
        return collections.OrderedDict(sorted(new_kwargs.items()))


class BaseRebuilder(ABC):
    """
    Abstract base class for deserialization specialists.
    Defines a common structure with a registry and a dispatch method.
    """

    @property
    @abstractmethod
    def registry(self) -> dict[str, callable]:
        """The registry mapping keys to handler methods."""
        pass

    def rebuild(self, value: Any, **kwargs) -> Any:
        """
        Main dispatch method. Recursively rebuilds a value using the registry.
        """
        # Base cases for recursion
        if not isinstance(value, (dict, list, tuple)):
            return value
        if isinstance(value, list):
            return [self.rebuild(item, **kwargs) for item in value]
        if isinstance(value, tuple):
            return tuple(self.rebuild(item, **kwargs) for item in value)

        # For dictionaries, use the specialized registry
        if isinstance(value, dict):
            pydantic_handler = self.registry.get("pydantic_model_import_path")
            if pydantic_handler is not None and _is_serialized_pydantic_model(value):
                return pydantic_handler(value, **kwargs)

            # Find a handler in the registry and use it
            for key, handler in self.registry.items():
                if key == "pydantic_model_import_path":
                    continue
                if key in value:
                    return handler(value, **kwargs)

            # If no handler, it's a generic dict; rebuild its contents
            return {k: self.rebuild(v, **kwargs) for k, v in value.items()}

        return value  # Fallback


class ConfigRebuilder(BaseRebuilder):
    @property
    def registry(self) -> dict[str, Callable]:
        return {
            "pydantic_model_import_path": self._handle_pydantic_model,
            "is_time_series_config": self._handle_timeseries_config,
            "orm_class": self._handle_orm_model,
            "__type__": self._handle_complex_type,
        }

    def _handle_pydantic_model(self, value: dict, **kwargs) -> Any:
        path_info = value["pydantic_model_import_path"]
        module = importlib.import_module(path_info["module"])
        PydanticClass = getattr(module, path_info["qualname"])

        rebuilt_value = self.rebuild(value["serialized_model"], **kwargs)
        return PydanticClass(**rebuilt_value)

    def _handle_timeseries_config(self, value: dict, **kwargs) -> dict:
        return self.rebuild(value["config_data"], **kwargs)

    def _handle_orm_model(self, value: dict, **kwargs) -> Any:
        return build_model(value)

    def _handle_complex_type(self, value: dict, **kwargs) -> Any:
        if value.get("__type__") == "platform_time_index_meta_table":
            return _import_qualified_name(value["module"], value["qualname"])
        # Special case for ORM lists within the generic complex type handler
        if value.get("__type__") == "orm_model_list":
            return [build_model(item) for item in value["items"]]
        # Fallback to the generic rebuild_with_type for other types (like tuples)
        return rebuild_with_type(value, rebuild_function=lambda x: self.rebuild(x, **kwargs))


class DeserializerManager:
    """Handles serialization and deserialization of configurations."""

    def __init__(self):
        self.config_rebuilder = ConfigRebuilder()

    def rebuild_config(self, config: dict[str, Any], **kwargs) -> dict[str, Any]:
        """Rebuilds an entire configuration dictionary."""
        return self.config_rebuilder.rebuild(config, **kwargs)

    def rebuild_serialized_config(
        self, config: dict[str, Any], time_serie_class_name: str
    ) -> dict[str, Any]:
        """
        Rebuilds a configuration dictionary from a serialized config.

        Args:
            config: The configuration dictionary.
            time_serie_class_name: The name of the DataNode class.

        Returns:
            The rebuilt configuration dictionary.
        """
        config = self.rebuild_config(config=config)

        return config


@dataclass
class TimeSerieConfig:
    """A container for all computed configuration attributes."""

    update_hash: str
    storage_hash: str
    local_initial_configuration: dict[str, Any]
    remote_initial_configuration: dict[str, Any]
    build_configuration_json_schema: dict[str, Any]


def extract_pydantic_fields_from_dict(d: Mapping[str, Any]) -> dict[str, dict[str, dict[str, Any]]]:
    """
    Returns: {key: {field_name: <metadata>}} for every value in `d` that is a Pydantic model.
    """
    result: dict[str, dict[str, dict[str, Any]]] = {}
    for k, v in d.items():
        if isinstance(v, BaseModel):
            try:
                result[k] = v.model_json_schema()
            except Exception as e:
                raise e
    return result


def _crop_hash_prefix(prefix: str, *, max_length: int = POSTGRES_IDENTIFIER_MAX_LENGTH) -> str:
    max_prefix_length = max_length - _HASH_SUFFIX_LENGTH
    if max_prefix_length <= 0:
        raise ValueError("max_length must leave room for '_' plus the hash suffix.")

    cropped_prefix = prefix[:max_prefix_length].rstrip("_")
    return cropped_prefix or "hash"


def create_config(
    ts_class_name: str,
    kwargs: dict[str, Any],
    *,
    update_hash_prefix: str | None = None,
    storage_hash_prefix: str | None = None,
):
    """
    Creates the configuration and hashes using the original hash_signature logic.
    """
    try:
        build_configuration_json_schema = extract_pydantic_fields_from_dict(kwargs)
    except Exception as e:
        raise e

    # 1. Serialize the core arguments
    serialized_core_kwargs = Serializer().serialize_init_kwargs(kwargs)

    # 2. Prepare the dictionary for hashing
    dict_to_hash = copy.deepcopy(serialized_core_kwargs)

    # 3. Generate the hashes
    update_hash, storage_hash = hash_signature(dict_to_hash)

    # 4. Create the remote configuration by removing ignored keys
    remote_config = copy.deepcopy(dict_to_hash)

    update_prefix = _crop_hash_prefix((update_hash_prefix or ts_class_name).lower())
    storage_prefix = _crop_hash_prefix((storage_hash_prefix or ts_class_name).lower())

    # 5. Return all computed values in the structured dataclass
    return TimeSerieConfig(
        update_hash=f"{update_prefix}_{update_hash}".lower(),
        storage_hash=f"{storage_prefix}_{storage_hash}".lower(),
        local_initial_configuration=dict_to_hash,
        remote_initial_configuration=remote_config,
        build_configuration_json_schema=build_configuration_json_schema,
    )
