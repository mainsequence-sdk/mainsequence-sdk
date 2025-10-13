import collections
import copy
import hashlib
import importlib
import json
import os
from abc import ABC, abstractmethod
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from enum import Enum
from functools import singledispatch
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import cloudpickle
from pydantic import BaseModel

import mainsequence.client as ms_client
from mainsequence.client import POD_PROJECT, BaseObjectOrm
from mainsequence.client.models_helpers import get_model_class
from mainsequence.instrumentation import tracer, tracer_instrumentator
from mainsequence.tdag.config import API_TS_PICKLE_PREFIFX, bcolors, ogm

from .persist_managers import PersistManager, get_data_node_source_code_git_hash

build_model = lambda model_data: get_model_class(model_data["orm_class"])(**model_data)


# 1. Create a "registry" function using the decorator
@singledispatch
def serialize_argument(value: Any, pickle_ts: bool) -> Any:
    """
    Default implementation for any type not specifically registered.
    It can either return the value as is or raise a TypeError.
    """
    # For types we don't explicitly handle, we can check if they are serializable
    # or just return them. For simplicity, we return as is.
    return value


def _serialize_timeserie(value: "DataNode", pickle_ts: bool = False) -> dict[str, Any]:
    """Serialization logic for DataNode objects."""
    print(f"Serializing DataNode: {value.update_hash}")
    if pickle_ts:
        return {
            "is_time_serie_pickled": True,
            "update_hash": value.update_hash,
            "data_source_id": value.data_source_id,
        }
    return {"is_time_serie_instance": True, "update_hash": value.update_hash}


def _serialize_api_timeserie(value, pickle_ts: bool):
    if pickle_ts:
        new_value = {"is_api_time_serie_pickled": True}
        value.persist_to_pickle()
        new_value["update_hash"] = value.update_hash
        new_value["data_source_id"] = value.data_source_id
        return new_value
    return value


@serialize_argument.register(BaseModel)
def _(value: BaseModel, pickle_ts: bool = False) -> dict[str, Any]:
    """Serialization logic for any Pydantic BaseModel."""
    import_path = {"module": value.__class__.__module__, "qualname": value.__class__.__qualname__}
    # Recursively call serialize_argument on each value in the model's dictionary.
    serialized_model = {
        k: serialize_argument(v, pickle_ts) for k, v in json.loads(value.model_dump_json()).items()
    }

    ignore_from_storage_hash = [
        k
        for k, v in value.model_fields.items()
        if v.json_schema_extra
        and v.json_schema_extra.get("ignore_from_storage_hash", False) == True
    ]

    return {
        "pydantic_model_import_path": import_path,
        "serialized_model": serialized_model,
        "ignore_from_storage_hash": ignore_from_storage_hash,
    }


@serialize_argument.register(BaseObjectOrm)
def _(value, pickle_ts: bool):
    new_dict = json.loads(value.model_dump_json())
    if hasattr(value, "unique_identifier"):
        new_dict["unique_identifier"] = value.unique_identifier
    return new_dict


@serialize_argument.register(list)
def _(value: list, pickle_ts: bool):
    if not value:
        return []

    # 1. DETECT if it's a list of ORM models
    if isinstance(value[0], BaseObjectOrm):
        # 2. SORT the list to ensure a stable hash
        sorted_value = sorted(value, key=lambda x: x.unique_identifier)

        # 3. SERIALIZE each item in the now-sorted list
        serialized_items = [serialize_argument(item, pickle_ts) for item in sorted_value]

        # 4. WRAP the result in an identifiable structure for deserialization
        return {"__type__": "orm_model_list", "items": serialized_items}

    # Fallback for all other list types
    return [serialize_argument(item, pickle_ts) for item in value]


@serialize_argument.register(tuple)
def _(value, pickle_ts: bool):
    items = [serialize_argument(item, pickle_ts) for item in value]
    return {"__type__": "tuple", "items": items}


@serialize_argument.register(dict)
def _(value: dict, pickle_ts: bool):
    # Check for the special marker key.
    if value.get("is_time_series_config") is True:
        # If it's a special config dict, preserve its unique structure.
        # Serialize its contents recursively.
        config_data = {k: serialize_argument(v, pickle_ts) for k, v in value.items()}

        return {"is_time_series_config": True, "config_data": config_data}

    # Otherwise, handle it as a regular dictionary.
    return {k: serialize_argument(v, pickle_ts) for k, v in value.items()}


@serialize_argument.register(SimpleNamespace)
def _(value, pickle_ts: bool):
    return serialize_argument.dispatch(dict)(vars(value), pickle_ts)


@serialize_argument.register(Enum)
def _(value, pickle_ts: bool):
    return value.value


class TimeSerieInitMeta(BaseModel): ...


def data_source_dir_path(data_source_id: int) -> str:
    path = ogm.pickle_storage_path
    return f"{path}/{data_source_id}"


def data_source_pickle_path(data_source_id: int) -> str:
    return f"{data_source_dir_path(data_source_id)}/data_source.pickle"


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
        if key != "build_meta_data":
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
                else:
                    # recursively apply hash signature
                    local_ts_dict_to_hash[key] = parse_dictionary_before_hashing(value)

    return local_ts_dict_to_hash


def prepare_config_kwargs(kwargs: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
    """
    Separates all meta-arguments from the core configuration arguments and applies defaults.
    This replaces _separate_meta_kwargs and sanitize_default_build_metadata.

    Returns:
        A tuple of (core_kwargs, meta_kwargs).
    """
    meta_keys = [
        "init_meta",
        "build_meta_data",
    ]
    meta_kwargs = {}

    for key in meta_keys:
        if key in kwargs:
            # Move the argument from the main dict to the meta dict
            meta_kwargs[key] = kwargs.pop(key)

    # --- Apply Defaults (replaces sanitize_default_build_metadata) ---
    if meta_kwargs.get("init_meta") is None:
        meta_kwargs["init_meta"] = TimeSerieInitMeta()

    if meta_kwargs.get("build_meta_data") is None:
        meta_kwargs["build_meta_data"] = {"initialize_with_default_partitions": True}

    return kwargs, meta_kwargs  # Returns (core_kwargs, meta_kwargs)


def verify_backend_git_hash_with_pickle(
    local_persist_manager: PersistManager, time_serie_class: "DataNode"
) -> None:
    """Verifies if the git hash in the backend matches the one from the pickled object."""
    if local_persist_manager.data_node_storage is not None:
        load_git_hash = get_data_node_source_code_git_hash(time_serie_class)

        persisted_pickle_hash = (
            local_persist_manager.data_node_storage.time_serie_source_code_git_hash
        )
        if load_git_hash != persisted_pickle_hash:
            local_persist_manager.logger.warning(
                f"{bcolors.WARNING}Source code does not match with pickle rebuilding{bcolors.ENDC}"
            )
            pickle_path = get_pickle_path(
                update_hash=local_persist_manager.update_hash,
                data_source_id=local_persist_manager.data_source.id,
            )
            flush_pickle(pickle_path)

            rebuild_time_serie = rebuild_from_configuration(
                update_hash=local_persist_manager.update_hash,
                data_source=local_persist_manager.data_source,
            )
            rebuild_time_serie.persist_to_pickle()
        else:
            # if no need to rebuild, just sync the metadata
            local_persist_manager.synchronize_data_node_storage(data_node_update=None)


def hash_signature(dictionary: dict[str, Any]) -> tuple[str, str]:
    """
    Computes MD5 hashes for local and remote configurations from a single dictionary.
    """
    dhash_local = hashlib.md5()
    dhash_remote = hashlib.md5()

    # The function expects to receive the full dictionary, including meta-args
    local_ts_dict_to_hash = parse_dictionary_before_hashing(dictionary)
    remote_ts_in_db_hash = copy.deepcopy(local_ts_dict_to_hash)

    # Add project_id for local hash
    local_ts_dict_to_hash["project_id"] = POD_PROJECT.id

    # Handle remote hash filtering internally
    if "arguments_to_ignore_from_storage_hash" in local_ts_dict_to_hash:
        keys_to_ignore = sorted(local_ts_dict_to_hash["arguments_to_ignore_from_storage_hash"])
        for k in keys_to_ignore:
            remote_ts_in_db_hash.pop(k, None)
        remote_ts_in_db_hash.pop("arguments_to_ignore_from_storage_hash", None)

    # remove keys from pydantic objects
    for k, val in local_ts_dict_to_hash.items():
        if isinstance(val, dict) == False:
            continue
        if "pydantic_model_import_path" in val:
            if "ignore_from_storage_hash" in val:
                for arg in val["ignore_from_storage_hash"]:
                    remote_ts_in_db_hash[k]["serialized_model"].pop(arg, None)
                if (
                    k in remote_ts_in_db_hash
                    and "ignore_from_storage_hash" in remote_ts_in_db_hash[k]
                ):
                    remote_ts_in_db_hash[k].pop("ignore_from_storage_hash")
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
        return self._serialize_dict(kwargs=kwargs, pickle_ts=False)

    def serialize_for_pickle(self, properties: dict[str, Any]) -> dict[str, Any]:
        """
        Serializes properties to a pickle-friendly dictionary.
        """
        return self._serialize_dict(kwargs=properties, pickle_ts=True)

    def _serialize_dict(self, kwargs: dict[str, Any], pickle_ts: bool) -> dict[str, Any]:
        """
        Internal worker that serializes a dictionary by calling the dispatcher.
        This maps to your original `_serialize_configuration_dict`.
        """
        new_kwargs = {key: serialize_argument(value, pickle_ts) for key, value in kwargs.items()}
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
            # Find a handler in the registry and use it
            for key, handler in self.registry.items():
                if key in value:
                    return handler(value, **kwargs)

            # If no handler, it's a generic dict; rebuild its contents
            return {k: self.rebuild(v, **kwargs) for k, v in value.items()}

        return value  # Fallback


class PickleRebuilder(BaseRebuilder):
    """Specialist for deserializing objects from a pickled state."""

    @property
    def registry(self) -> dict[str, Callable]:
        return {
            "is_time_serie_pickled": self._handle_pickled_timeserie,
            "is_api_time_serie_pickled": self._handle_api_timeserie,
            "pydantic_model_import_path": self._handle_pydantic_model,
            "is_time_series_config": self._handle_timeseries_config,
            "orm_class": self._handle_orm_model,
            "__type__": self._handle_complex_type,
        }

    def _handle_pickled_timeserie(self, value: dict, **state_kwargs) -> "DataNode":
        """Handles 'is_time_serie_pickled' markers."""
        import cloudpickle

        # Note: You need to make DataNode available here
        full_path = get_pickle_path(
            update_hash=value["update_hash"], data_source_id=value["data_source_id"]
        )
        with open(full_path, "rb") as handle:
            ts = cloudpickle.load(handle)

        ds_pickle_path = data_source_pickle_path(value["data_source_id"])
        data_source = load_data_source_from_pickle(ds_pickle_path)
        ts.set_data_source(data_source=data_source)

        if state_kwargs.get("graph_depth", 0) - 1 <= state_kwargs.get("graph_depth_limit", 0):
            ts._set_state_with_sessions(**state_kwargs)
        return ts

    def _handle_pydantic_model(self, value: dict, **state_kwargs) -> Any:
        path_info = value["pydantic_model_import_path"]
        module = importlib.import_module(path_info["module"])
        PydanticClass = getattr(module, path_info["qualname"])

        rebuilt_value = self.rebuild(value["serialized_model"], **state_kwargs)
        return PydanticClass(**rebuilt_value)

    def _handle_api_timeserie(self, value: dict, **state_kwargs) -> "APIDataNode":
        """Handles 'is_api_time_serie_pickled' markers."""
        import cloudpickle

        # Note: You need to make APIDataNode available here
        full_path = get_pickle_path(
            update_hash=value["update_hash"],
            data_source_id=value["data_source_id"],
            is_api=True,
        )
        with open(full_path, "rb") as handle:
            ts = cloudpickle.load(handle)
        return ts

    def _handle_timeseries_config(self, value: dict, **state_kwargs) -> dict:
        """Handles 'is_time_series_config' markers."""
        return self.rebuild(value["config_data"], **state_kwargs)

    def _handle_orm_model(self, value: dict, **state_kwargs) -> Any:
        """Handles 'orm_class' markers for single models."""
        return build_model(value)

    def _handle_complex_type(self, value: dict, **state_kwargs) -> Any:
        """Handles generic '__type__' markers (like tuples)."""
        rebuild_function = lambda x: self.rebuild(x, **state_kwargs)
        # Assumes rebuild_with_type handles different __type__ values
        return rebuild_with_type(value, rebuild_function=rebuild_function)


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
        # Special case for ORM lists within the generic complex type handler
        if value.get("__type__") == "orm_model_list":
            return [build_model(item) for item in value["items"]]
        # Fallback to the generic rebuild_with_type for other types (like tuples)
        return rebuild_with_type(value, rebuild_function=lambda x: self.rebuild(x, **kwargs))


class DeserializerManager:
    """Handles serialization and deserialization of configurations."""

    def __init__(self):
        self.pickle_rebuilder = PickleRebuilder()
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

    def deserialize_pickle_state(self, state: Any, **kwargs) -> Any:
        """Deserializes an entire pickled state object."""
        return self.pickle_rebuilder.rebuild(state, **kwargs)


@dataclass
class TimeSerieConfig:
    """A container for all computed configuration attributes."""

    init_meta: Any
    remote_build_metadata: Any
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
            result[k] = v.model_json_schema()
    return result


def create_config(
    ts_class_name: str, arguments_to_ignore_from_storage_hash: list[str], kwargs: dict[str, Any]
):
    """
    Creates the configuration and hashes using the original hash_signature logic.
    """
    global logger

    build_configuration_json_schema = extract_pydantic_fields_from_dict(kwargs)

    # 1. Use the helper to separate meta args from core args.
    core_kwargs, meta_kwargs = prepare_config_kwargs(kwargs)

    # 2. Serialize the core arguments
    serialized_core_kwargs = Serializer().serialize_init_kwargs(core_kwargs)

    # 3. Prepare the dictionary for hashing
    dict_to_hash = copy.deepcopy(serialized_core_kwargs)

    dict_to_hash["arguments_to_ignore_from_storage_hash"] = arguments_to_ignore_from_storage_hash

    # 4. Generate the hashes
    update_hash, storage_hash = hash_signature(dict_to_hash)

    # 5. Create the remote configuration by removing ignored keys
    remote_config = copy.deepcopy(dict_to_hash)

    # 6. Return all computed values in the structured dataclass
    return TimeSerieConfig(
        init_meta=meta_kwargs["init_meta"],
        remote_build_metadata=meta_kwargs["build_meta_data"],
        update_hash=f"{ts_class_name}_{update_hash}".lower(),
        storage_hash=f"{ts_class_name}_{storage_hash}".lower(),
        local_initial_configuration=dict_to_hash,
        remote_initial_configuration=remote_config,
        build_configuration_json_schema=build_configuration_json_schema,
    )


def flush_pickle(pickle_path) -> None:
    """Deletes the pickle file for this time series."""
    if os.path.isfile(pickle_path):
        os.remove(pickle_path)


# In class BuildManager:


@tracer.start_as_current_span("TS: load_from_pickle")
def load_from_pickle(pickle_path: str) -> "DataNode":
    """
    Loads a DataNode object from a pickle file, handling both standard and API types.

    Args:
        pickle_path: The path to the pickle file.

    Returns:
        The loaded DataNode object.
    """

    import cloudpickle

    directory = os.path.dirname(pickle_path)
    filename = os.path.basename(pickle_path)
    prefixed_path = os.path.join(directory, f"{API_TS_PICKLE_PREFIFX}{filename}")
    if os.path.isfile(prefixed_path) and os.path.isfile(pickle_path):
        raise FileExistsError(
            "Both default and API timeseries pickle exist - cannot decide which to load"
        )

    if os.path.isfile(prefixed_path):
        pickle_path = prefixed_path

    try:
        with open(pickle_path, "rb") as handle:
            time_serie = cloudpickle.load(handle)
    except Exception as e:
        raise e

    if time_serie.is_api:
        return time_serie

    data_source = load_data_source_from_pickle(pickle_path=pickle_path)

    # set objects that are not pickleable
    time_serie.set_data_source(data_source=data_source)
    time_serie._local_persist_manager = None
    # verify pickle
    verify_backend_git_hash_with_pickle(
        local_persist_manager=time_serie.local_persist_manager,
        time_serie_class=time_serie.__class__,
    )
    return time_serie


def get_pickle_path(update_hash: str, data_source_id: int, is_api=False) -> str:
    if is_api:
        return os.path.join(
            ogm.pickle_storage_path,
            str(data_source_id),
            f"{API_TS_PICKLE_PREFIFX}{update_hash}.pickle",
        )
    return os.path.join(ogm.pickle_storage_path, str(data_source_id), f"{update_hash}.pickle")


def load_data_source_from_pickle(pickle_path: str) -> Any:
    data_path = Path(pickle_path).parent / "data_source.pickle"
    with open(data_path, "rb") as handle:
        data_source = cloudpickle.load(handle)
    return data_source


def rebuild_and_set_from_update_hash(
    update_hash: int,
    data_source_id: int,
    set_dependencies_df: bool = False,
    graph_depth_limit: int = 1,
) -> tuple["DataNode", str]:
    """
    Rebuilds a DataNode from its local hash ID and pickles it if it doesn't exist.

    Args:
        update_hash: The local hash ID of the DataNode.
        data_source_id: The data source ID.
        set_dependencies_df: Whether to set the dependencies DataFrame.
        graph_depth_limit: The depth limit for graph traversal.

    Returns:
        A tuple containing the DataNode object and the path to its pickle file.
    """
    pickle_path = get_pickle_path(
        update_hash=update_hash,
        data_source_id=data_source_id,
    )
    if os.path.isfile(pickle_path) == False or os.stat(pickle_path).st_size == 0:
        # rebuild time serie and pickle
        ts = rebuild_from_configuration(
            update_hash=update_hash,
            data_source=data_source_id,
        )
        if set_dependencies_df == True:
            ts.set_relation_tree()

        ts.persist_to_pickle()
        ts.logger.info(f"ts {update_hash} pickled ")

    ts = load_and_set_from_pickle(
        pickle_path=pickle_path,
        graph_depth_limit=graph_depth_limit,
    )
    ts.logger.debug(f"ts {update_hash} loaded from pickle ")
    return ts, pickle_path


def load_and_set_from_pickle(pickle_path: str, graph_depth_limit: int = 1) -> "DataNode":
    """
    Loads a DataNode from a pickle file and sets its state.

    Args:
        pickle_path: The path to the pickle file.
        graph_depth_limit: The depth limit for setting the state.

    Returns:
        The loaded and configured DataNode object.
    """
    ts = load_from_pickle(pickle_path)
    ts._set_state_with_sessions(
        graph_depth=0, graph_depth_limit=graph_depth_limit, include_vam_client_objects=False
    )
    return ts


@tracer.start_as_current_span("TS: Rebuild From Configuration")
def rebuild_from_configuration(update_hash: str, data_source: int | object) -> "DataNode":
    """
    Rebuilds a DataNode instance from its configuration.

    Args:
        update_hash: The local hash ID of the DataNode.
        data_source: The data source ID or object.

    Returns:
        The rebuilt DataNode instance.
    """
    import importlib

    tracer_instrumentator.append_attribute_to_current_span("update_hash", update_hash)

    if isinstance(data_source, int):
        pickle_path = get_pickle_path(data_source_id=data_source, update_hash=update_hash)
        if os.path.isfile(pickle_path) == False:
            data_source = ms_client.DynamicTableDataSource.get(pk=data_source)
            data_source.persist_to_pickle(data_source_pickle_path(data_source.id))

        data_source = load_data_source_from_pickle(pickle_path=pickle_path)

    persist_manager = PersistManager.get_from_data_type(
        update_hash=update_hash,
        data_source=data_source,
    )
    try:
        time_serie_config = persist_manager.local_build_configuration
    except Exception as e:
        raise e

    try:
        mod = importlib.import_module(time_serie_config["time_series_class_import_path"]["module"])
        TimeSerieClass = getattr(
            mod, time_serie_config["time_series_class_import_path"]["qualname"]
        )
    except Exception as e:
        raise e

    time_serie_class_name = time_serie_config["time_series_class_import_path"]["qualname"]

    time_serie_config.pop("time_series_class_import_path")
    time_serie_config = DeserializerManager().rebuild_serialized_config(
        time_serie_config, time_serie_class_name=time_serie_class_name
    )
    time_serie_config["init_meta"] = {}

    re_build_ts = TimeSerieClass(**time_serie_config)

    return re_build_ts


def load_and_set_from_hash_id(update_hash: int, data_source_id: int) -> "DataNode":
    path = get_pickle_path(update_hash=update_hash, data_source_id=data_source_id)
    ts = load_and_set_from_pickle(pickle_path=path)
    return ts
