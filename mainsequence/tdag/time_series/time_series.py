import datetime
import os
from venv import logger

import numpy as np
import pandas as pd
from typing import Dict, Any, List, Optional, Tuple, Callable
import json
import time
import traceback
import pytz
import inspect
import logging
import copy
import hashlib
import importlib
import cloudpickle
from pathlib import Path

from mainsequence.client import MARKETS_CONSTANTS
from mainsequence.instrumentation import tracer, tracer_instrumentator
from mainsequence.tdag.config import (
    ogm
)
import structlog.contextvars as cvars

from mainsequence.tdag.time_series.persist_managers import PersistManager, APIPersistManager
from mainsequence.client.models_tdag import (DataSource, LocalTimeSeriesHistoricalUpdate,
                                             DataUpdates, UniqueIdentifierRangeMap, ColumnMetaData, POD_PROJECT
                                             )
from pandas.api.types import is_datetime64_any_dtype

from pydantic import BaseModel

from abc import ABC

from typing import Union
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

from mainsequence.client import LocalTimeSerie, LocalTimeSerieUpdateDetails, CONSTANTS, \
    DynamicTableDataSource, AssetTranslationTable
from enum import Enum
from functools import wraps
from mainsequence.tdag.config import bcolors
from mainsequence.logconf import logger


def serialize_model_list(value: Optional[List[Any]]) -> Optional[Dict[str, Any]]:
    """
    Serializes a list of model objects into a dictionary.

    Args:
        value: A list of model objects or None.

    Returns:
        A dictionary representation of the model list, or None if the input is None.
    """
    if value is None:
        return None
    value.sort(key=lambda x: x.unique_identifier, reverse=False)
    new_value = {"is_model_list": True}
    new_value['model_list'] = [v.to_serialized_dict() for v in value]
    value = new_value
    return value


def rebuild_with_type(value: Dict[str, Any], rebuild_function: Callable) -> Union[tuple, Any]:
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
    if value["__type__"] == "tuple":
        return tuple([rebuild_function(c) for c in value["items"]])
    else:
        raise NotImplementedError


from mainsequence.client.models_helpers import get_model_class, MarketsTimeSeriesDetails

build_model = lambda model_data: get_model_class(model_data["orm_class"])(**model_data)


def parse_dictionary_before_hashing(dictionary: Dict[str, Any]) -> Dict[str, Any]:
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

                    local_ts_dict_to_hash[key] = value['unique_identifier']

                elif "is_time_series_config" in value.keys():
                    tmp_local_ts, remote_ts = hash_signature(value["config_data"])
                    local_ts_dict_to_hash[key] = {"is_time_series_config": value["is_time_series_config"],
                                                  "config_data": tmp_local_ts}

                elif "is_model_list" in value.keys():  # do not inclue
                    local_ts_dict_to_hash[key] = {"is_model_list": value["is_model_list"],
                                                  "config_data": [v["unique_identifier"] for v in value['model_list']]}
                else:
                    # recursively apply hash signature
                    local_ts_dict_to_hash[key] = parse_dictionary_before_hashing(value)

            if isinstance(value, list):
                if len(value) == 0:
                    local_ts_dict_to_hash[key] = value
                else:
                    if isinstance(value[0], dict):
                        if "orm_class" in value[0].keys():
                            try:
                                new_list = [v["unique_identifier"] for v in value]
                            except Exception as e:
                                raise e
                            local_ts_dict_to_hash[key] = new_list
                            raise Exception("Use ModelList")

    return local_ts_dict_to_hash


def hash_signature(dictionary: Dict[str, Any]) -> Tuple[str, str]:
    """
    Computes MD5 hashes for local and remote configurations of a dictionary.

    Args:
        dictionary: The dictionary to hash.

    Returns:
        A tuple containing the local hash and the remote hash.
    """
    import hashlib
    dhash_local = hashlib.md5()
    dhash_remote = hashlib.md5()

    local_ts_dict_to_hash = parse_dictionary_before_hashing(dictionary)
    remote_ts_in_db_hash = copy.deepcopy(local_ts_dict_to_hash)

    # local_hash are bound to project
    if "project_id" in local_ts_dict_to_hash:
        raise ValueError(f"project_id is set automatically for the local_hash to the current project.")
    local_ts_dict_to_hash["project_id"] = POD_PROJECT.id

    if "local_kwargs_to_ignore" in local_ts_dict_to_hash.keys():
        keys_to_ignore = local_ts_dict_to_hash['local_kwargs_to_ignore']
        keys_to_ignore.sort()
        for k in keys_to_ignore:
            remote_ts_in_db_hash.pop(k, None)
        remote_ts_in_db_hash.pop("local_kwargs_to_ignore", None)

    try:
        encoded_local = json.dumps(local_ts_dict_to_hash, sort_keys=True).encode()
        encoded_remote = json.dumps(remote_ts_in_db_hash, sort_keys=True).encode()
    except Exception as e:
        logger.error(dictionary)
        logger.error(traceback.format_exc())
        raise e
    dhash_local.update(encoded_local)
    dhash_remote.update(encoded_remote)
    return dhash_local.hexdigest(), dhash_remote.hexdigest()


class ConfigSerializer:
    """Handles serialization and deserialization of configurations."""

    @staticmethod
    def _serialize_model(model: Any) -> Dict[str, Any]:
        columns = {"model": model.__class__.__name__, "id": model.unique_identifier}
        return columns

    @classmethod
    def rebuild_serialized_wrapper_dict(cls, time_series_dict_config: dict) -> Dict[str, Any]:
        """
        Rebuilds a dictionary of TimeSerie objects from a serialized wrapper configuration.

        Args:
            time_series_dict_config: The serialized wrapper dictionary.

        Returns:
            A dictionary of TimeSerie objects.
        """
        time_series_dict = {}
        for key, value in time_series_dict_config.items():
            new_ts = cls.rebuild_from_configuration(hash_id=value)
            time_series_dict[key] = new_ts

        return time_series_dict

    @classmethod
    def rebuild_pydantic_model(cls, details: Dict[str, Any], state_kwargs: Optional[dict] = None) -> Any:
        """
        Rebuilds a Pydantic model from its serialized representation.

        Args:
            details: The serialized model details.
            state_kwargs: Optional state arguments for deserialization.

        Returns:
            An instance of the rebuilt Pydantic model.
        """
        rebuild_function = lambda x, state_kwargs: cls._rebuild_configuration_argument(x, ignore_pydantic=False)
        if state_kwargs is not None:
            rebuild_function = lambda x, state_kwargs: cls.deserialize_pickle_value(x, **state_kwargs)

        module = importlib.import_module(details["pydantic_model_import_path"]["module"])
        PydanticClass = getattr(module, details["pydantic_model_import_path"]['qualname'])
        new_details = {}
        for arg, arg_value in details["serialized_model"].items():
            if isinstance(arg_value, dict):
                if "pydantic_model_import_path" in arg_value.keys():
                    arg_value = cls.rebuild_pydantic_model(arg_value,
                                                           state_kwargs=state_kwargs)
                elif "__type__" in arg_value.keys():
                    arg_value = rebuild_with_type(arg_value)
                else:
                    arg_value = {k: rebuild_function(v, state_kwargs=state_kwargs) for k, v in arg_value.items()}
            elif isinstance(arg_value, list):
                new_list = []
                for a in arg_value:

                    if isinstance(a, dict):
                        if "pydantic_model_import_path" in a.keys():
                            new_item = cls.rebuild_pydantic_model(a,
                                                                  state_kwargs=state_kwargs)
                        else:
                            new_item = rebuild_function(a, state_kwargs=state_kwargs)
                    else:
                        new_item = rebuild_function(a, state_kwargs=state_kwargs)
                    new_list.append(new_item)

                arg_value = new_list
            else:

                arg_value = rebuild_function(arg_value, state_kwargs=state_kwargs)

            new_details[arg] = arg_value
        try:

            value = PydanticClass(**new_details)
        except Exception as e:
            raise e

        return value

    @classmethod
    def rebuild_serialized_config(cls, config: Dict[str, Any], time_serie_class_name: str) -> Dict[str, Any]:
        """
        Rebuilds a configuration dictionary from a serialized config.

        Args:
            config: The configuration dictionary.
            time_serie_class_name: The name of the TimeSerie class.

        Returns:
            The rebuilt configuration dictionary.
        """
        config = cls.rebuild_config(config=config)
        if time_serie_class_name == "WrapperTimeSerie":
            config["time_series_dict"] = cls.rebuild_serialized_wrapper_dict(
                time_series_dict_config=config["time_series_dict"],
            )

        return config

    @classmethod
    def _rebuild_configuration_argument(cls, value: Any, ignore_pydantic: bool) -> Any:
        """
        Recursively rebuilds a configuration argument from its serialized form.

        Args:
            value: The configuration value to rebuild.
            ignore_pydantic: Whether to ignore Pydantic model rebuilding.

        Returns:
            The rebuilt configuration value.
        """
        if isinstance(value, dict):

            if "is_time_series_config" in value.keys():
                config_data = value["config_data"]
                new_config = cls.rebuild_config(config=config_data, )
                value = TimeSerieConfigKwargs(new_config)
            elif "is_model_list" in value.keys():
                value = ModelList([build_model(v) for v in value["model_list"]])
            elif "orm_class" in value.keys():
                value = build_model(value)
            elif "pydantic_model_import_path" in value.keys():
                value = cls.rebuild_config(value, ignore_pydantic=True)
                if ignore_pydantic == False:
                    value = cls.rebuild_pydantic_model(value)
            elif "__type__" in value.keys():
                value = rebuild_with_type(value, rebuild_function=lambda x: cls._rebuild_configuration_argument(x,
                                                                                                                ignore_pydantic=ignore_pydantic))
            else:
                value = cls.rebuild_config(config=value)


        elif isinstance(value, list):
            if len(value) == 0:
                return value
            value = [cls._rebuild_configuration_argument(v, ignore_pydantic=ignore_pydantic) for v in value]

        return value

    @classmethod
    def rebuild_config(cls, config: Dict[str, Any], ignore_pydantic: bool = False) -> Dict[str, Any]:
        """
        Rebuilds a configuration dictionary.

        Args:
            config: The configuration dictionary to rebuild.
            ignore_pydantic: If True, Pydantic models will not be rebuilt.

        Returns:
            The rebuilt configuration dictionary.
        """
        for key, value in config.items():
            config[key] = cls._rebuild_configuration_argument(value, ignore_pydantic)

        return config

    def _serialize_signature_argument(self, value: Any, pickle_ts: bool) -> Any:
        """
        Serializes a single argument from a function signature.

        Args:
            value: The argument value to serialize.
            pickle_ts: Whether to pickle TimeSerie objects.

        Returns:
            The serialized argument.
        """
        from types import SimpleNamespace
        from mainsequence.client import BaseObjectOrm

        if issubclass(value.__class__, TimeSerie):
            if pickle_ts == True:
                new_value = {"is_time_serie_pickled": True}
                try:
                    full_path, relative_path = value.persist_to_pickle()
                    new_value["local_hash_id"] = value.local_hash_id
                    new_value['data_source_id'] = value.data_source_id
                except Exception as e:
                    raise e
                value = new_value
        elif issubclass(value.__class__, APITimeSerie):
            if pickle_ts == True:
                new_value = {"is_api_time_serie_pickled": True}
                try:
                    full_path, relative_path = value.persist_to_pickle()
                    new_value["local_hash_id"] = value.local_hash_id
                    new_value['data_source_id'] = value.data_source_id
                except Exception as e:
                    raise e
                value = new_value
        elif isinstance(value, SimpleNamespace):
            value = vars(value)
        elif isinstance(value, Enum):
            value = value.value
        elif isinstance(value, BaseModel) == True and issubclass(value.__class__, BaseObjectOrm) == False:

            import_path = {"module": value.__class__.__module__,
                           "qualname": value.__class__.__qualname__}

            serialized_model = {}
            for key, model_tmp_value in value.model_dump().items():  # model_dict.items():
                serialized_model[key] = self._serialize_signature_argument(model_tmp_value, pickle_ts)
            try:

                json.dumps(serialized_model)
            except Exception as e:
                raise e
            value = {"pydantic_model_import_path": import_path, "serialized_model": serialized_model}
        elif issubclass(value.__class__, BaseObjectOrm):
            value = value.to_serialized_dict()
        elif isinstance(value, tuple):
            tuple_list = []
            for v in value:
                new_v = v
                if isinstance(v, BaseModel):
                    new_v = self._serialize_signature_argument(v, pickle_ts=pickle_ts)
                else:
                    try:
                        new_v = self._serialize_signature_argument(v, pickle_ts=pickle_ts)
                    except Exception as e:
                        raise e
                tuple_list.append(new_v)
            new_value = {"__type__": "tuple", "items": tuple_list}
            value = new_value
        elif isinstance(value, list):
            if len(value) == 0:
                return []
            if isinstance(value, ModelList):
                value = serialize_model_list(value)
            else:
                if all(isinstance(member, BaseObjectOrm) for member in value):
                    value = serialize_model_list(value)
                elif issubclass(value[0].__class__, BaseObjectOrm):

                    raise Exception("Use Mixed Object Model ")
                elif issubclass(value[0].__class__, dict):
                    value = [self._serialize_configuration_dict(v) for v in value]
                elif isinstance(value[0], BaseModel):
                    value = [self._serialize_signature_argument(v, pickle_ts=pickle_ts) for v in value]
                else:
                    new_value = []
                    sort = True
                    for v in value:
                        if issubclass(v.__class__, dict):
                            if "__type__" in v.keys():
                                new_value.append(v)
                            else:
                                new_value.append(self._serialize_configuration_dict(v))
                            sort = False
                        elif isinstance(value[0], BaseModel):
                            new_value.append(self._serialize_signature_argument(v, pickle_ts=pickle_ts))
                            sort = False
                        else:
                            new_value.append(v)
                    if sort == True:
                        new_value.sort()
                    value = new_value

        elif isinstance(value, dict):
            for value_key, value_value in value.items():
                if isinstance(value_value, dict):
                    value[value_key] = self._serialize_configuration_dict(value_value, pickle_ts=pickle_ts)
                else:
                    value[value_key] = self._serialize_signature_argument(value_value, pickle_ts=pickle_ts)
        elif isinstance(value, TimeSerieConfigKwargs):
            self.logger.warning("TimeSeriesConfigKwargs will be depreciated")
            new_value = {"is_time_series_config": True}
            if pickle_ts == False:
                value = self._serialize_configuration_dict(value, pickle_ts=pickle_ts)
            else:
                value = self._serialize_configuration_dict(value, pickle_ts=pickle_ts)
            new_value["config_data"] = value
            value = new_value
        return value

    def _serialize_configuration_dict(self, kwargs: Dict[str, Any], pickle_ts: bool = False, ordered_dict: bool = True) -> Dict[str, Any]:
        """
        Serializes a configuration dictionary.

        Args:
            kwargs: The dictionary to serialize.
            pickle_ts: Whether to pickle TimeSerie objects.
            ordered_dict: Whether to return an ordered dictionary.

        Returns:
            The serialized dictionary.
        """
        import collections
        new_kwargs = {}

        for key, value in kwargs.items():

            if key in ["model_list"]:
                ##already serialized arguments
                new_kwargs[key] = value
                continue
            if isinstance(value, dict):
                if "is_time_serie_pickled" in value.keys() or "is_api_time_serie_pickled" in value.keys():
                    new_kwargs[key] = value
                    continue

            value = self._serialize_signature_argument(value, pickle_ts=pickle_ts)

            new_kwargs[key] = value
        if ordered_dict == True:
            ordered_kwargs = collections.OrderedDict(sorted(new_kwargs.items()))
        else:
            ordered_kwargs = new_kwargs
        return ordered_kwargs

    def serialize_to_pickle(self, properties: Dict[str, Any]) -> Dict[str, Any]:
        """
        Serializes properties to a pickle-friendly dictionary.
        """
        serialized_properties = self._serialize_configuration_dict(kwargs=properties, pickle_ts=True)
        return serialized_properties

    def serialize_init_kwargs(self, kwargs: Dict[str, Any]) -> Dict[str, Any]:
        """
        Serializes __init__ keyword arguments for a TimeSerie.
        """
        ordered_kwargs = self._serialize_configuration_dict(kwargs=kwargs)
        return ordered_kwargs

    @classmethod
    def deserialize_pickle_value(cls,
                                 value: Any,
                                 include_vam_client_objects: bool,
                                 graph_depth_limit: int,
                                 graph_depth: int,
                                 ignore_pydantic: bool = False,
                                 data_source_id: Optional[int] = None) -> Any:
        """
        Deserializes a single value from a pickled state.

        Args:
            value: The value to deserialize.
            include_vam_client_objects: Whether to include VAM client objects.
            graph_depth_limit: The depth limit for graph traversal.
            graph_depth: The current depth in the graph.
            ignore_pydantic: Whether to ignore Pydantic model deserialization.
            data_source_id: The ID of the data source.

        Returns:
            The deserialized value.
        """
        import cloudpickle

        new_value = value
        state_kwargs = dict(
                            graph_depth_limit=graph_depth_limit,
                            data_source_id=data_source_id,
                            graph_depth=copy.deepcopy(graph_depth),
                            include_vam_client_objects=include_vam_client_objects)

        if isinstance(value, dict):
            if "is_time_series_config" in value.keys():
                new_value = TimeSerieConfigKwargs(**value["config_data"])
            elif "__type__" in value.keys():
                rebuild_function = lambda x: cls.deserialize_pickle_state(x, **state_kwargs)
                new_value = rebuild_with_type(value, rebuild_function=rebuild_function)
            elif "is_model_list" in value.keys():
                new_value = ModelList([build_model(v) for v in value['model_list']])
            elif "is_time_serie_pickled" in value.keys():
                try:
                    full_path = TimeSerie.get_pickle_path(local_hash_id=value['local_hash_id'],
                                                          data_source_id=value['data_source_id']
                                                          )
                except Exception as e:
                    raise e
                with open(full_path, 'rb') as handle:
                    ts = cloudpickle.load(handle)
                    ts.set_data_source_from_pickle_path(full_path)
                    if graph_depth - 1 <= graph_depth_limit:
                        ts.set_state_with_sessions(
                            graph_depth_limit=graph_depth_limit,
                            graph_depth=graph_depth,
                            include_vam_client_objects=include_vam_client_objects)
                    new_value = ts
            elif "is_api_time_serie_pickled" in value.keys():
                full_path = APITimeSerie.get_pickle_path(local_hash_id=value['local_hash_id'],
                                                         data_source_id=value['data_source_id']
                                                         )
                with open(full_path, 'rb') as handle:
                    ts = cloudpickle.load(handle)
                new_value = ts
            elif "orm_class" in value.keys():
                new_value = build_model(value)
            elif "pydantic_model_import_path" in value.keys():
                new_value = cls.deserialize_pickle_state(value,
                                                          **state_kwargs
                                                         )
                if ignore_pydantic == False:
                    new_value = cls.rebuild_pydantic_model(new_value,
                                                           state_kwargs=state_kwargs)
            else:
                new_value = cls.deserialize_pickle_state(value,
                                                         **state_kwargs)
        if isinstance(value, tuple):
            new_value = [
                cls.deserialize_pickle_value(v, ignore_pydantic=ignore_pydantic, **state_kwargs) if isinstance(v,
                                                                                                               dict) else v
                for v in value]
            new_value = tuple(new_value)

        if isinstance(value, list):
            if len(value) == 0:
                return new_value
            # if isinstance(value[0], dict):
            #     if "orm_class" in value[0].keys():
            #         new_value = [build_model(v) for v in value]
            #     elif "pydantic_model_import_path" in value[0].keys():
            #             new_value = [cls.rebuild_pydantic_model(v) for v in value]

            new_value = [
                cls.deserialize_pickle_value(v, ignore_pydantic=ignore_pydantic, **state_kwargs) if isinstance(v,
                                                                                                               dict) else v
                for v in value]

        return new_value

    @classmethod
    def deserialize_pickle_state(cls, state: Any, include_vam_client_objects: bool, data_source_id: int,
                                 graph_depth_limit: int,
                                 graph_depth: int) -> Any:
        """
        Deserializes the state of an object from a pickle.

        Args:
            state: The state to deserialize.
            include_vam_client_objects: Whether to include VAM client objects.
            data_source_id: The ID of the data source.
            graph_depth_limit: The depth limit for graph traversal.
            graph_depth: The current depth in the graph.

        Returns:
            The deserialized state.
        """
        if isinstance(state, dict):
            for key, value in state.items():
                state[key] = cls.deserialize_pickle_value(value, include_vam_client_objects=include_vam_client_objects,
                                                          graph_depth_limit=graph_depth_limit, graph_depth=graph_depth,
                                                          data_source_id=data_source_id,
                                                          )
        elif isinstance(state, tuple):
            state = tuple([cls.deserialize_pickle_value(v, include_vam_client_objects=include_vam_client_objects,
                                                        graph_depth_limit=graph_depth_limit, graph_depth=graph_depth,
                                                         data_source_id=data_source_id,
                                                        ) for v in state])
        elif isinstance(state, str) or isinstance(state, float) or isinstance(state, int) or isinstance(state, bool):
            pass
        else:
            raise NotImplementedError

        return state

class DependencyUpdateError(Exception):
    pass


class GraphNodeMethods(ABC):

    def get_mermaid_dependency_diagram(self) -> str:
        """
        Displays a Mermaid.js dependency diagram in a Jupyter environment.

        Returns:
            The Mermaid diagram string.
        """
        from IPython.display import display, HTML

        mermaid_diagram = self.local_persist_manager.display_mermaid_dependency_diagram()

        # Mermaid.js initialization script (only run once)
        if not hasattr(display, "_mermaid_initialized"):
            mermaid_initialize = """
                   <script src="https://cdn.jsdelivr.net/npm/mermaid/dist/mermaid.min.js"></script>
                   <script>
                       function initializeMermaid() {
                           if (typeof mermaid !== 'undefined') {
                               console.log('Initializing Mermaid.js...');
                               const mermaidDivs = document.querySelectorAll('.mermaid');
                               mermaidDivs.forEach(mermaidDiv => {
                                   mermaid.init(undefined, mermaidDiv);
                               });
                           } else {
                               console.error('Mermaid.js is not loaded.');
                           }
                       }
                   </script>
                   """
            display(HTML(mermaid_initialize))
            display._mermaid_initialized = True

        # HTML template for rendering the Mermaid diagram
        html_template = f"""
               <div class="mermaid">
               {mermaid_diagram}
               </div>
               <script>
                   initializeMermaid();
               </script>
               """

        # Display the Mermaid diagram in the notebook
        display(HTML(html_template))

        # Optionally return the raw diagram code for further use
        return mermaid_diagram

    def get_all_local_dependencies(self) -> pd.DataFrame:
        """
        Gets a DataFrame of all local dependencies in the graph.

        Returns:
            A pandas DataFrame with dependency information.
        """
        dependencies_df = self.local_persist_manager.get_all_local_dependencies()
        return dependencies_df

    @property
    def is_local_relation_tree_set(self) -> bool:
        return self.local_persist_manager.local_metadata.ogm_dependencies_linked

    def get_update_map(self, dependecy_map: Optional[Dict] = None) -> Dict[Tuple[str, int], Dict[str, Any]]:
        """
        Obtain all local time_series in the dependency graph by introspecting the code class members.
        Dicts are allowed to have timeseries.

        Args:
            dependecy_map: An optional dictionary to store the dependency map.

        Returns:
            A dictionary mapping (local_hash_id, data_source_id) to TimeSerie info.
        """
        members = self.__dict__
        dependecy_map = {} if dependecy_map is None else dependecy_map

        def process_ts_value(value, dependecy_map):
            if isinstance(value, TimeSerie):
                value.local_persist_manager  # before connection call local persist manager to garantee ts is created
                dependecy_map[(value.local_hash_id, value.data_source_id)] = {"is_pickle": False, "ts": value}
                value.get_update_map(dependecy_map)
            if isinstance(value, APITimeSerie):
                value.local_persist_manager  # before connection call local persist manager to garantee ts is created
                dependecy_map[(value.local_hash_id, value.data_source_id)] = {"is_pickle": False, "ts": value}

        for key, value in members.items():
            try:
                process_ts_value(value, dependecy_map)
                if isinstance(value, dict):
                    if "is_time_serie_pickled" in value.keys():
                        pickle_path = self.get_pickle_path(local_hash_id=value["local_hash_id"])
                        dependecy_map[(value.local_hash_id, value.data_source_id)] = {"is_pickle": True,
                                                                                      "ts": pickle_path}

                    if "is_api_time_serie_pickled" in value.keys():
                        is_api = isinstance(value, APITimeSerie)
                        dependecy_map[(value.local_hash_id, value.data_source_id)] = {"is_pickle": False, "ts": value}
                        # tm_ts.get_update_map(dependecy_map)

                    # add timeseries from values
                    nested_values = value.values()
                    for nested_value in nested_values:
                        process_ts_value(nested_value, dependecy_map)

            except Exception as e:
                raise e
        return dependecy_map

    def set_relation_tree(self):
        """Sets the node relationships in the backend."""
        def process_ts_value(value):
            if isinstance(value, TimeSerie):
                value.local_persist_manager
                value._verify_and_build_remote_objects()  # before connection call local persist manager to garantee ts is created
                self.local_persist_manager.depends_on_connect(value, is_api=False)
                value.set_relation_tree()
            if isinstance(value, APITimeSerie):
                value.local_persist_manager  # before connection call local persist manager to garantee ts is created
                self.local_persist_manager.depends_on_connect(value, is_api=True)

        members = self.__dict__
        if self.local_persist_manager.local_metadata is None:
            self._verify_and_build_remote_objects() #critical point to build the objects

        if self.is_local_relation_tree_set == False:
            # persiste manager needs to have full information
            self.local_persist_manager.set_local_metadata_lazy(include_relations_detail=True,force_registry=True)

            for key, value in members.items():
                try:
                    process_ts_value(value)
                    if isinstance(value, dict):
                        if "is_time_serie_pickled" in value.keys():
                            new_ts = self.load_and_set_from_hash_id(local_hash_id=value["local_hash_id"],
                                                                    data_source_id=value["data_source_id"])
                            new_ts.local_persist_manager  # before connection call local persist manager to garantee ts is created
                            self.local_persist_manager.depends_on_connect(new_ts)
                            new_ts.set_relation_tree()
                        if "is_api_time_serie_pickled" in value.keys():
                            new_ts = APITimeSerie.load_and_set_from_pickle(local_hash_id=value["local_hash_id"],
                                                                           data_source_id=value["data_source_id"]
                                                                           )
                            self.local_persist_manager.depends_on_connect(new_ts)
                            new_ts.set_relation_tree()

                        nested_values = value.values()
                        for nested_value in nested_values:
                            process_ts_value(nested_value)

                except Exception as e:
                    raise e
            self.local_persist_manager.set_ogm_dependencies_linked()


class TimeSerieRebuildMethods(ABC):

    def verify_backend_git_hash_with_pickle(self) -> None:
        """Verifies if the git hash in the backend matches the one from the pickled object."""
        if self.local_persist_manager.metadata is not None:
            load_git_hash = self.get_time_serie_source_code_git_hash(self.__class__)

            persisted_pickle_hash = self.local_persist_manager.metadata.time_serie_source_code_git_hash
            if load_git_hash != persisted_pickle_hash:
                self.logger.warning(
                    f"{bcolors.WARNING}Source code does not match with pickle rebuilding{bcolors.ENDC}")
                self.flush_pickle()

                rebuild_time_serie = TimeSerie.rebuild_from_configuration(local_hash_id=self.local_hash_id,
                                                                          data_source=self.data_source,
                                                                          )
                rebuild_time_serie.persist_to_pickle()
            else:
                # if no need to rebuild, just sync the metadata
                self.local_persist_manager.synchronize_metadata(local_metadata=None)

    @classmethod
    @tracer.start_as_current_span("TS: load_from_pickle")
    def load_from_pickle(cls, pickle_path: str) -> "TimeSerie":
        time_serie = load_from_pickle(pickle_path)
        return time_serie

    @classmethod
    def load_data_source_from_pickle(self, pickle_path: str) -> Any:
        data_path = Path(pickle_path).parent / "data_source.pickle"
        with open(data_path, 'rb') as handle:
            data_source = cloudpickle.load(handle)
        return data_source

    @classmethod
    def rebuild_and_set_from_local_hash_id(cls, local_hash_id: int, data_source_id: int, set_dependencies_df: bool = False,
                                           graph_depth_limit: int = 1) -> Tuple["TimeSerie", str]:
        """
        Rebuilds a TimeSerie from its local hash ID and pickles it if it doesn't exist.

        Args:
            local_hash_id: The local hash ID of the TimeSerie.
            data_source_id: The data source ID.
            set_dependencies_df: Whether to set the dependencies DataFrame.
            graph_depth_limit: The depth limit for graph traversal.

        Returns:
            A tuple containing the TimeSerie object and the path to its pickle file.
        """
        pickle_path = TimeSerie.get_pickle_path(local_hash_id=local_hash_id,
                                                data_source_id=data_source_id,
                                                )
        if os.path.isfile(pickle_path) == False or os.stat(pickle_path).st_size == 0:
            # rebuild time serie and pickle
            ts = TimeSerie.rebuild_from_configuration(
                local_hash_id=local_hash_id,
                data_source=data_source_id,
            )
            if set_dependencies_df == True:
                ts.set_relation_tree()

            ts.persist_to_pickle()
            ts.logger.info(f"ts {local_hash_id} pickled ")

        ts = TimeSerie.load_and_set_from_pickle(
            pickle_path=pickle_path,
            graph_depth_limit=graph_depth_limit,
        )
        ts.logger.debug(f"ts {local_hash_id} loaded from pickle ")
        return ts, pickle_path

    def set_data_source_from_pickle_path(self, pikle_path: str) -> None:
        """
        Sets the data source for the TimeSerie from a pickle path.

        Args:
            pikle_path: The path to the pickle file.
        """
        data_source = self.load_data_source_from_pickle(pikle_path)
        self.set_data_source(data_source=data_source)

    @classmethod
    def load_and_set_from_pickle(cls, pickle_path: str, graph_depth_limit: int = 1) -> "TimeSerie":
        """
        Loads a TimeSerie from a pickle file and sets its state.

        Args:
            pickle_path: The path to the pickle file.
            graph_depth_limit: The depth limit for setting the state.

        Returns:
            The loaded and configured TimeSerie object.
        """
        ts = cls.load_from_pickle(pickle_path)
        ts.set_state_with_sessions(
            graph_depth=0,
            graph_depth_limit=graph_depth_limit,
            include_vam_client_objects=False)
        return ts

    @classmethod
    @tracer.start_as_current_span("TS: Rebuild From Configuration")
    def rebuild_from_configuration(cls, local_hash_id: str,
                                   data_source: Union[int, object]) -> "TimeSerie":
        """
        Rebuilds a TimeSerie instance from its configuration.

        Args:
            local_hash_id: The local hash ID of the TimeSerie.
            data_source: The data source ID or object.

        Returns:
            The rebuilt TimeSerie instance.
        """
        import importlib
        from mainsequence.tdag.time_series.persist_managers import PersistManager

        tracer_instrumentator.append_attribute_to_current_span("local_hash_id", local_hash_id)

        if isinstance(data_source, int):
            pickle_path = cls.get_pickle_path(data_source_id=data_source,
                                              local_hash_id=local_hash_id)
            if os.path.isfile(pickle_path) == False:
                data_source = DynamicTableDataSource.get(pk=data_source)
                data_source.persist_to_pickle(data_source_pickle_path(data_source.id))

            data_source = cls.load_data_source_from_pickle(pickle_path=pickle_path)

        persist_manager = PersistManager.get_from_data_type(local_hash_id=local_hash_id,
                                                            data_source=data_source,
                                                            )
        try:
            time_serie_config = persist_manager.local_build_configuration
        except Exception as e:
            raise e

        try:
            mod = importlib.import_module(time_serie_config["time_series_class_import_path"]["module"])
            TimeSerieClass = getattr(mod, time_serie_config["time_series_class_import_path"]["qualname"])
        except Exception as e:
            raise e

        time_serie_class_name = time_serie_config["time_series_class_import_path"]["qualname"]

        time_serie_config.pop("time_series_class_import_path")
        time_serie_config = ConfigSerializer.rebuild_serialized_config(time_serie_config,
                                                                       time_serie_class_name=time_serie_class_name)
        time_serie_config["init_meta"] = {}

        re_build_ts = TimeSerieClass(**time_serie_config)

        return re_build_ts

    def __setstate__(self, state: Dict[str, Any]) -> None:
        # Restore instance attributes (i.e., filename and lineno).
        self.__dict__.update(state)

    def __getstate__(self) -> Dict[str, Any]:
        # Copy the object's state from self.__dict__ which contains
        # all our instance attributes. Always use the dict.copy()
        # method to avoid modifying the original state.
        state = self._prepare_state_for_pickle(state=self.__dict__)

        # Remove the unpicklable entries.
        return state

    @classmethod
    def get_pickle_path(cls, local_hash_id: str, data_source_id: int) -> str:
        return os.path.join(ogm.pickle_storage_path, str(data_source_id), f"{local_hash_id}.pickle")

    @classmethod
    def load_and_set_from_hash_id(cls, local_hash_id: int, data_source_id: int) -> "TimeSerie":
        path = cls.get_pickle_path(local_hash_id=local_hash_id,data_source_id=data_source_id)
        ts = cls.load_and_set_from_pickle(pickle_path=path)
        return ts

    @property
    def pickle_path(self) -> str:
        pp = data_source_dir_path(self.data_source.id)
        path = f"{pp}/{self.local_hash_id}.pickle"
        return path

    def _update_git_and_code_in_backend(self) -> None:
        """Updates the source code and git hash information in the backend."""
        self.local_persist_manager.update_source_informmation(
            git_hash_id=self.get_time_serie_source_code_git_hash(self.__class__),
            source_code=self.get_time_serie_source_code(self.__class__),
        )

    def persist_to_pickle(self, overwrite: bool = False) -> Tuple[str, str]:
        """
        Persists the TimeSerie object to a pickle file.

        Args:
            overwrite: If True, overwrites the existing pickle file.

        Returns:
            A tuple containing the full path and the relative path of the pickle file.
        """
        import cloudpickle
        path = self.pickle_path
        # after persisting pickle , build_hash and source code need to be patched
        self.logger.debug(f"Persisting pickle and patching source code and git hash for {self.hash_id}")
        self._update_git_and_code_in_backend()

        pp = data_source_pickle_path(self.data_source.id)
        if os.path.isfile(pp) == False or overwrite == True:
            self.data_source.persist_to_pickle(pp)

        if os.path.isfile(path) == False or overwrite == True:
            if overwrite == True:
                self.logger.warning("overwriting pickle")

            with open(path, 'wb') as handle:
                cloudpickle.dump(self, handle)

        return path, path.replace(ogm.pickle_storage_path + "/", "")

    @tracer.start_as_current_span("TS: set_state_with_sessions")
    def set_state_with_sessions(self, include_vam_client_objects: bool = True,
                                graph_depth_limit: int = 1000,
                                graph_depth: int = 0) -> None:
        """
        Sets the state of the TimeSerie after loading from pickle, including sessions.

        Args:
            include_vam_client_objects: Whether to include VAM client objects.
            graph_depth_limit: The depth limit for graph traversal.
            graph_depth: The current depth in the graph.
        """
        if graph_depth_limit == -1:
            graph_depth_limit = 1e6

        minimum_required_depth_for_update = self.get_minimum_required_depth_for_update()

        state = self.__dict__

        state, init_meta = self._sanitize_init_meta(kwargs=state)
        if graph_depth_limit < minimum_required_depth_for_update and graph_depth == 0:
            graph_depth_limit = minimum_required_depth_for_update
            self.logger.warning(f"Graph depht limit overrided to {minimum_required_depth_for_update}")

        # if the data source is not local then the de-serialization needs to happend after setting the local persist manager
        # to guranteed a proper patch in the back-end
        if graph_depth <= graph_depth_limit and self.data_source.related_resource_class_type:


            self._set_local_persist_manager(local_hash_id=self.local_hash_id,
                                            remote_table_hashed_name=self.remote_table_hashed_name,
                                            local_metadata=None, verify_local_run=False,
                                            )

        serializer = ConfigSerializer()
        state = serializer.deserialize_pickle_state(state=state, data_source_id=self.data_source.id,
                                                    include_vam_client_objects=include_vam_client_objects,
                                                    graph_depth_limit=graph_depth_limit,
                                                    graph_depth=graph_depth + 1)

        self.__dict__.update(state)

        self.local_persist_manager.synchronize_metadata(local_metadata=None)

    def get_minimum_required_depth_for_update(self) -> int:
        """
        Controls the minimum depth that needs to be rebuilt.
        """
        return 0

    def _prepare_state_for_pickle(self, state: Dict[str, Any]) -> Dict[str, Any]:
        """
        Prepares the object's state for pickling by serializing and removing unpicklable entries.

        Args:
            state: The object's __dict__.

        Returns:
            A pickle-safe dictionary representing the object's state.
        """
        import cloudpickle
        properties = state
        serializer = ConfigSerializer()
        properties = serializer.serialize_to_pickle(properties)
        names_to_remove = []
        for name, attr in properties.items():
            if name in [
                "local_persist_manager",
                "logger",
                "init_meta",
                "_local_metadata_future",
                "_local_metadata_lock",
                "_local_persist_manager",
                "update_tracker",
            ]:
                names_to_remove.append(name)
                continue

            try:
                cloudpickle.dumps(attr)
            except Exception as e:
                self.logger.exception(f"Cant Pickle property {name}")
                raise e

        for n in names_to_remove:
            properties.pop(n, None)

        return properties

    def run_in_debug_scheduler(self, break_after_one_update: bool = True, run_head_in_main_process: bool = True,
                               wait_for_update: bool = True, force_update: bool = True, debug: bool = True,
                               update_tree: bool = True,
                               raise_exception_on_error: bool = True) -> None:
        """
        Runs the TimeSerie update in a debug scheduler.

        Args:
            break_after_one_update: If True, stops after one update cycle.
            run_head_in_main_process: If True, runs the head node in the main process.
            wait_for_update: If True, waits for the update to complete.
            force_update: If True, forces an update.
            debug: If True, runs in debug mode.
            update_tree: If True, updates the entire dependency tree.
            raise_exception_on_error: If True, raises exceptions on errors.
        """
        from .update.scheduler import SchedulerUpdater
        SchedulerUpdater.debug_schedule_ts(
            time_serie_hash_id=self.local_hash_id,
            data_source_id=self.data_source.id,
            break_after_one_update=break_after_one_update,
            run_head_in_main_process=True,
            wait_for_update=False,
            force_update=True,
            debug=True,
            update_tree=True,
            raise_exception_on_error=raise_exception_on_error
        )

    @tracer.start_as_current_span("TS: Update")
    def start_time_serie_update(self, update_tracker: object, debug_mode: bool,
                                raise_exceptions: bool = True, update_tree: bool = False,
                                local_time_series_map: Optional[Dict[str, "LocalTimeSerie"]] = None, update_only_tree: bool = False, force_update: bool = False,
                                use_state_for_update: bool = False) -> bool:
        """
        Main update method for a TimeSerie that interacts with the graph node.

        Args:
            update_tracker: The update tracker object.
            debug_mode: Whether to run in debug mode.
            raise_exceptions: Whether to raise exceptions on errors.
            update_tree: Whether to update the entire dependency tree.
            local_time_series_map: A map of local time series.
            update_only_tree: If True, only updates the dependency tree structure.
            force_update: If True, forces an update.
            use_state_for_update: If True, uses the current state for the update.

        Returns:
            True if there was an error on the last update, False otherwise.
        """
        try:
            local_time_serie_historical_update =self.local_persist_manager.local_metadata.set_start_of_execution(
                                                active_update_scheduler_uid=update_tracker.scheduler_uid)
        except Exception as e:
            raise e

        latest_value, must_update = local_time_serie_historical_update.last_time_index_value, local_time_serie_historical_update.must_update
        update_statistics = local_time_serie_historical_update.update_statistics
        error_on_last_update = False
        exception_raised= None

        if force_update == True or update_statistics.max_time_index_value is None:
            must_update = True

        #Update statistics and build and rebuild localmetadata with foreign relations
        self.local_persist_manager.set_local_metadata_lazy(include_relations_detail=True)
        update_statistics = self.set_update_statistics(update_statistics)

        try:
            if must_update == True:
                max_update_time_days = os.getenv("TDAG_MAX_UPDATE_TIME_DAYS", None)
                update_on_batches = False
                if update_on_batches is not None:
                    max_update_time_days = datetime.timedelta(days=update_on_batches)
                    update_on_batches = True

                self.update_local(update_tree=update_tree, debug_mode=debug_mode,
                                  overwrite_latest_value=latest_value, local_time_series_map=local_time_series_map,
                                  update_tracker=update_tracker, update_only_tree=update_only_tree,
                                  use_state_for_update=use_state_for_update, update_statistics=update_statistics
                                  )

                self.local_persist_manager.local_metadata.set_end_of_execution(
                    historical_update_id=local_time_serie_historical_update.id,
                                                    error_on_update=error_on_last_update)
            else:
                self.logger.info("Already updated, waiting until next update time")

        except Exception as e:
            error_on_last_update = True
            raise e
        finally:
            self.local_persist_manager.local_metadata.set_end_of_execution(
                historical_update_id=local_time_serie_historical_update.id,
                error_on_update=error_on_last_update)
            #always set last relations details
            self.local_persist_manager.set_local_metadata_lazy(include_relations_detail=True,
                                                               )
            self._run_post_update_routines(error_on_last_update=error_on_last_update,
                                           update_statistics=update_statistics
                                           )

            self._set_column_metadata()

        return error_on_last_update


class DataPersistanceMethods(ABC):

    # sets
    def get_metadatas_and_set_updates(self, *args, **kwargs) -> Any:
        from mainsequence.client import LocalTimeSerie
        return LocalTimeSerie.get_metadatas_and_set_updates(*args, **kwargs)

    def patch_update_details(self, local_hash_id: Optional[str] = None, *args, **kwargs) -> Any:
        return self.local_persist_manager.patch_update_details(local_hash_id=local_hash_id, **kwargs)

    def reset_dependencies_states(self, hash_id_list: list) -> Any:
        return self.local_persist_manager.reset_dependencies_states(hash_id_list=hash_id_list)

    def update_details_in_dependecy_tree(self, set_relation_tree: bool = True, include_head: bool = False, *args, **kwargs) -> None:
        """
        Updates the schedule for all time series in the dependency tree.

        Args:
            set_relation_tree: Whether to set the relation tree first.
            include_head: Whether to include the head node in the update.
        """
        if set_relation_tree == True:
            self.set_relation_tree()
        dependants_df = self.get_all_local_dependencies()

        dependants_records = []
        if len(dependants_df) > 0:
            dependants_records = dependants_df[["local_hash_id", "data_source_id"]].to_dict("records")

        if include_head:
            dependants_records.append({"local_hash_id": self.local_hash_id, "data_source_id": self.data_source.id})

        all_metadatas = self.get_metadatas_and_set_updates(local_hash_id__in=dependants_records,
                                                           multi_index_asset_symbols_filter=self.multi_index_asset_symbols_filter,
                                                           update_priority_dict=None,
                                                           update_details_kwargs=kwargs)

    # direct contact
    @property
    def update_details(self) -> Any:
        return self.local_persist_manager.update_details

    @property
    def run_configuration(self) -> Any:
        return self.local_persist_manager.run_configuration

    @property
    def metadata(self) -> Any:
        return self.local_persist_manager.metadata

    @property
    def local_metadata(self) -> Any:
        return self.local_persist_manager.local_metadata

    @property
    def source_table_configuration(self) -> Any:
        return self.local_persist_manager.source_table_configuration

    def set_policy(self, interval: str, comp_type: str, overwrite: bool = False) -> None:
        """Sets a policy (e.g., retention) for the time series."""
        self.local_persist_manager.set_policy(interval, overwrite=overwrite, comp_type=comp_type)

    def verify_tree_compression_policy_is_set(self) -> None:
        """Verifies that the compression policy is set for the entire dependency tree."""
        deps = self.dependencies_df
        for _, ts_row in deps.iterrows():
            try:
                ts = TimeSerie.rebuild_from_configuration(hash_id=ts_row["hash_id"],
                                                          )
                ts.set_compression_policy()
            except Exception as e:
                self.logger.exception(f"{ts_row['hash_id']} compression policy not set")

    def upsert_data(self, data_df: pd.DataFrame) -> None:
        """
        Updates and inserts data into the database.

        Args:
            data_df: The DataFrame to upsert.
        """
        self.local_persist_manager.upsert_data(data_df=data_df)

    @property
    def persist_size(self) -> int:
        return self.local_persist_manager.persist_size

    def get_update_statistics(self, unique_identifier_list: Optional[List[str]] = None) -> DataUpdates:
        """
        Gets the latest update statistics from the database.

        Args:
            unique_identifier_list: An optional list of unique identifiers to filter by.

        Returns:
            A DataUpdates object with the latest statistics.
        """
        if isinstance(self.metadata,int):
            self.local_persist_manager.set_local_metadata_lazy(force_registry=True,include_relations_detail=True)

        if self.metadata.sourcetableconfiguration is None:
            return DataUpdates()

        update_stats = self.metadata.sourcetableconfiguration.get_data_updates()
        return update_stats

    def get_earliest_value(self) -> datetime.datetime:
        earliest_value = self.local_persist_manager.get_earliest_value()
        return earliest_value

    @property
    def local_parquet_file(self) -> str:
        try:
            return self.data_folder + "/time_series_data.parquet"
        except Exception as e:
            raise

    def is_persisted(self, session: Optional[object] = None) -> bool:
        """ Checks if the time series is persisted """
        ip = self.local_persist_manager.time_serie_exist()
        return ip

    def filter_by_assets_ranges(self, asset_ranges_map: dict) -> pd.DataFrame:
        """
        Filters data by asset ranges.

        Args:
            asset_ranges_map: A dictionary mapping assets to their date ranges.

        Returns:
            A DataFrame with the filtered data.
        """
        df = self.local_persist_manager.filter_by_assets_ranges(asset_ranges_map)
        return df
    
    def get_ranged_data_per_asset(self, range_descriptor: Optional[UniqueIdentifierRangeMap]) -> pd.DataFrame:
        """
        Gets data based on a range descriptor.

        Args:
            range_descriptor: A UniqueIdentifierRangeMap object.

        Returns:
            A DataFrame with the ranged data.
        """
        return self.get_df_between_dates(unique_identifier_range_map=range_descriptor)

    def get_ranged_data(self, start_date: Union[datetime.datetime, None] = None,
                        end_date: Union[datetime.datetime, None] = None,
                        great_or_equal: bool = True,
                        less_or_equal: bool = True,
                        ) -> pd.DataFrame:
        """
        Gets data based on a range descriptor.

        Args:
            range_descriptor: A UniqueIdentifierRangeMap object.

        Returns:
            A DataFrame with the ranged data.
        """
        return self.get_df_between_dates(unique_identifier_range_map=range_descriptor)
    
    def get_df_between_dates(
            self,
            start_date: Union[datetime.datetime, None] = None,
            end_date: Union[datetime.datetime, None] = None,
            unique_identifier_list: Union[None, list] = None,
            great_or_equal: bool = True,
            less_or_equal: bool = True,
            unique_identifier_range_map: Optional[UniqueIdentifierRangeMap] = None,
    ) -> pd.DataFrame:
        """
        Retrieve rows from this TimeSerie whose `time_index` (and optional `unique_identifier`) fall within the specified date ranges.

        **Note:** If `unique_identifier_range_map` is provided, **all** other filters
        (`start_date`, `end_date`, `unique_identifier_list`, `great_or_equal`, `less_or_equal`)
        are ignored, and only the per-identifier ranges in `unique_identifier_range_map` apply.

        Filtering logic (when `unique_identifier_range_map` is None):
          - If `start_date` is provided, include rows where
            `time_index > start_date` (if `great_or_equal=False`)
            or `time_index >= start_date` (if `great_or_equal=True`).
          - If `end_date` is provided, include rows where
            `time_index < end_date` (if `less_or_equal=False`)
            or `time_index <= end_date` (if `less_or_equal=True`).
          - If `unique_identifier_list` is provided, only include rows whose
            `unique_identifier` is in that list.

        Filtering logic (when `unique_identifier_range_map` is provided):
          - For each `unique_identifier`, apply its own `start_date`/`end_date`
            filters using the specified operands (`">"`, `">="`, `"<"`, `"<="`):
            {
              <uid>: {
                "start_date": datetime,
                "start_date_operand": ">=" or ">",
                "end_date": datetime,
                "end_date_operand": "<=" or "<"
              },
              ...
            }

        Parameters
        ----------
        start_date : datetime.datetime or None
            Global lower bound for `time_index`. Ignored if `unique_identifier_range_map` is provided.
        end_date : datetime.datetime or None
            Global upper bound for `time_index`. Ignored if `unique_identifier_range_map` is provided.
        unique_identifier_list : list or None
            If provided, only include rows matching these IDs. Ignored if `unique_identifier_range_map` is provided.
        great_or_equal : bool, default True
            If True, use `>=` when filtering by `start_date`; otherwise use `>`. Ignored if `unique_identifier_range_map` is provided.
        less_or_equal : bool, default True
            If True, use `<=` when filtering by `end_date`; otherwise use `<`. Ignored if `unique_identifier_range_map` is provided.
        unique_identifier_range_map : UniqueIdentifierRangeMap or None
            Mapping of specific `unique_identifier` keys to their own sub-filters. When provided, this is the sole filter applied.

        Returns
        -------
        pd.DataFrame
            A DataFrame containing rows that satisfy the combined time and identifier filters.
        """
        func = self.local_persist_manager.get_df_between_dates
        sig = inspect.signature(func)
        kwargs = dict(
            start_date=start_date,
            end_date=end_date,
            unique_identifier_list=unique_identifier_list,
            great_or_equal=great_or_equal,
            less_or_equal=less_or_equal,
            unique_identifier_range_map=unique_identifier_range_map,
        )
        if 'time_serie' in sig.parameters:
            kwargs["time_serie"] = self

        filtered_data = self.local_persist_manager.get_df_between_dates(**kwargs)
        return filtered_data

    def get_persisted_ts(self) -> pd.DataFrame:
        """
        Gets the full persisted time series data.

        Returns:
            A DataFrame with the persisted data.
        """
        try:
            persisted_df = self.local_persist_manager.get_persisted_ts()
        except Exception as e:
            # if local file got corrupted flush local time serie withouth deleting configuration
            # self.logger.error(f"{self} local file got corrupted flush local time serie withouth deleting configuration")
            # self.flush_local_persisted()
            raise e

        return persisted_df

    def load_ts_df(self, session: Optional[object] = None, *args, **kwargs) -> pd.DataFrame:
        """
        Loads the time series DataFrame, updating it if it's not persisted.

        Args:
            session: An optional session object.

        Returns:
            A DataFrame with the time series data.
        """
        if self.is_persisted:
            pandas_df = self.get_persisted_ts(session=session)
        else:
            self.update_local(update_tree=False, *args, **kwargs)
            pandas_df = self.get_persisted_ts(session=session)

        return pandas_df

    def get_latest_update_by_assets_filter(self, asset_symbols: Optional[list], last_update_per_asset: dict) -> datetime.datetime:
        """
        Gets the latest update timestamp for a list of asset symbols.

        Args:
            asset_symbols: A list of asset symbols.
            last_update_per_asset: A dictionary mapping assets to their last update time.

        Returns:
            The latest update timestamp.
        """
        if asset_symbols is not None:
            last_update_in_table = np.max([timestamp for unique_identifier, timestamp in last_update_per_asset.items()
                                           if unique_identifier in asset_symbols
                                           ])
        else:
            last_update_in_table = np.max(last_update_per_asset.values)
        return last_update_in_table

    def last_update_per_unique_identifier(self, unique_identifier_list: Optional[list],
                                          last_update_per_asset: dict) -> datetime.datetime:
        """
        Gets the earliest last update time for a list of unique identifiers.

        Args:
            unique_identifier_list: A list of unique identifiers.
            last_update_per_asset: A dictionary mapping assets to their last update times.

        Returns:
            The earliest last update timestamp.
        """
        if unique_identifier_list is not None:
            last_update_in_table = min(
                [t for a in last_update_per_asset.values() for t in a.values() if a in unique_identifier_list])
        else:
            last_update_in_table = min([t for a in last_update_per_asset.values() for t in a.values()])
        return last_update_in_table

    def get_last_observation(self, unique_identifier_list: Optional[list] = None) -> Optional[pd.DataFrame]:
        """
        Gets the last observation from the time series.

        Args:
            unique_identifier_list: An optional list of unique identifiers to filter by.

        Returns:
            A DataFrame with the last observation, or None if not found.
        """
        update_statistics = self.get_update_statistics(unique_identifier_list=unique_identifier_list)
        if update_statistics.is_empty():
            return None

        # todo request specific endpoint
        last_observation = self.get_df_between_dates(
            start_date=update_statistics._max_time_in_update_statistics, great_or_equal=True,
            unique_identifier_list=unique_identifier_list
        )

        if last_observation.empty:
            return None

        return last_observation

    @property
    def last_observation(self) -> Optional[pd.DataFrame]:
        last_observation = self.get_last_observation()
        return last_observation

    def delete_table(self) -> None:
        if self.local_persist_manager.data_source.related_resource.class_type=="duck_db":
            from mainsequence.client.data_sources_interfaces.duckdb import DuckDBInterface
            db_interface = DuckDBInterface()
            db_interface.drop_table(self.local_persist_manager.metadata.hash_id)

        self.local_persist_manager.metadata.delete()

    @tracer.start_as_current_span("TS: Persist Data")
    def persist_updated_data(self, temp_df: pd.DataFrame, update_tracker: object, overwrite: bool = False) -> bool:
        """
        Persists the updated data to the database.

        Args:
            temp_df: The DataFrame with updated data.
            update_tracker: The update tracker object.
            overwrite: If True, overwrites existing data.

        Returns:
            True if data was persisted, False otherwise.
        """
        persisted = False
        if temp_df.shape[0] > 0:
            if overwrite == True:
                self.logger.warning(f"Values will be overwritten")
            self.local_persist_manager.persist_updated_data(temp_df=temp_df,
                                                            update_tracker=update_tracker,
                                                            historical_update_id=None,
                                                            overwrite=overwrite)
            persisted = True
        return persisted


class TimeSerieConfigKwargs(dict):
    """
    Necessary class for configuration
    """
    pass


class ModelList(list):
    """
    Necessary for configuration
    """
    pass


def data_source_dir_path(data_source_id: int) -> str:
    path = ogm.pickle_storage_path
    return f"{path}/{data_source_id}"

def data_source_pickle_path(data_source_id: int) -> str:
    return f"{data_source_dir_path(data_source_id)}/data_source.pickle"

@tracer.start_as_current_span("TS: load_from_pickle")
def load_from_pickle(pickle_path: str) -> "TimeSerie":
    """
    Loads a TimeSerie object from a pickle file, handling both standard and API types.

    Args:
        pickle_path: The path to the pickle file.

    Returns:
        The loaded TimeSerie object.
    """
    import cloudpickle
    from pathlib import Path

    directory = os.path.dirname(pickle_path)
    filename = os.path.basename(pickle_path)
    prefixed_path = os.path.join(directory, f"{APITimeSerie.PICKLE_PREFIFX}{filename}")
    if os.path.isfile(prefixed_path) and os.path.isfile(pickle_path):
        raise FileExistsError("Both default and API timeseries pickle exist - cannot decide which to load")

    if os.path.isfile(prefixed_path):
        pickle_path = prefixed_path

    try:
        with open(pickle_path, 'rb') as handle:
            time_serie = cloudpickle.load(handle)
    except Exception as e:
        raise e

    if isinstance(time_serie, APITimeSerie):
        return time_serie

    data_source = time_serie.load_data_source_from_pickle(pickle_path=pickle_path)
    time_serie.set_data_source(data_source=data_source)
    # verify pickle
    time_serie.verify_backend_git_hash_with_pickle()
    return time_serie


class CommonMethodsMixin:

    def get_logger_context_variables(self) -> Dict[str, Any]:
        return dict(local_hash_id=self.local_hash_id,
                    local_hash_id_data_source=self.data_source_id,
                    api_time_series=self.__class__.__name__ == "APITimeSerie")

    @property
    def logger(self) -> logging.Logger:
        """Gets a logger instance with bound context variables."""
        # import structlog.contextvars as cvars
        # cvars.bind_contextvars(local_hash_id=self.local_hash_id,
        #                      local_hash_id_data_source=self.data_source_id,
        #                      api_time_series=True,)
        global logger
        if hasattr(self, "_logger") == False:
            cvars.bind_contextvars(**self.get_logger_context_variables() )
            self._logger = logger

        return self._logger

    def unbind_context_variables_from_logger(self) -> None:
        cvars.unbind_contextvars(*self.get_logger_context_variables().keys())


class APITimeSerie(CommonMethodsMixin):
    PICKLE_PREFIFX = "api-"

    @classmethod
    def build_from_local_time_serie(cls, source_table: "LocalTimeSerie") -> "APITimeSerie":
        return cls(data_source_id=source_table.data_source.id,
                   source_table_hash_id=source_table.hash_id
                   )

    @classmethod
    def build_from_unique_identifier(cls, unique_identifier: str) -> "APITimeSerie":
        from mainsequence.client import MarketsTimeSeriesDetails
        tdag_api_data_source = MarketsTimeSeriesDetails.get(unique_identifier=unique_identifier)
        ts = cls(
            data_source_id=tdag_api_data_source.source_table.data_source,
            source_table_hash_id=tdag_api_data_source.source_table.hash_id
        )
        return ts

    def __init__(self,
                 data_source_id: int, source_table_hash_id: str,
                 data_source_local_lake: Optional[DataSource] = None):
        """
        Initializes an APITimeSerie.

        Args:
            data_source_id: The ID of the data source.
            local_hash_id: The local hash ID of the time series.
            data_source_local_lake: Optional local data source for the lake.
        """
        if data_source_local_lake is not None:
            assert data_source_local_lake.data_type in CONSTANTS.DATA_SOURCE_TYPE_LOCAL_DISK_LAKE, "data_source_local_lake should be of type CONSTANTS.DATA_SOURCE_TYPE_LOCAL_DISK_LAKE"

        self.data_source_id = data_source_id
        self.source_table_hash_id = source_table_hash_id
        self.data_source = data_source_local_lake
        self._local_persist_manager: APIPersistManager = None

    @staticmethod
    def _get_local_hash_id(hash_id):
        return "API_"+f"{hash_id}"
    @property
    def local_hash_id(self):
        return  self._get_local_hash_id(hash_id=self.source_table_hash_id)

    def __getstate__(self) -> Dict[str, Any]:
        """Prepares the state for pickling."""
        state = self.__dict__.copy()
        # Remove unpicklable/transient state specific to APITimeSerie
        names_to_remove = [
            "_local_persist_manager", # APIPersistManager instance
        ]
        cleaned_state = {k: v for k, v in state.items() if k not in names_to_remove}
        return cleaned_state

    @property
    def local_persist_manager(self) -> Any:
        """Gets the local persistence manager, initializing it if necessary."""
        if self._local_persist_manager is None:
            self._set_local_persist_manager()
            self.logger.debug(f"Setting local persist manager for {self.source_table_hash_id}")
        return self._local_persist_manager

    def set_relation_tree(self) -> None:
        pass  # do nothing  for API Time Series

    def _verify_local_data_source(self) -> None:
        """Verifies and sets the local data source from environment variables if available."""
        pod_source = os.environ.get("POD_DEFAULT_DATA_SOURCE", None)
        if pod_source != None:
            from mainsequence.client import models as models
            pod_source = json.loads(pod_source)
            ModelClass = pod_source["tdag_orm_class"]
            pod_source.pop("tdag_orm_class", None)
            ModelClass = getattr(models, ModelClass)
            pod_source = ModelClass(**pod_source)
            self.data_source = pod_source

    def build_data_source_from_configuration(self, data_config: Dict[str, Any]) -> DataSource:
        """
        Builds a data source object from a configuration dictionary.

        Args:
            data_config: The data source configuration.

        Returns:
            A DataSource object.
        """
        ModelClass = DynamicTableDataSource.get_class(data_config['data_type'])
        pod_source = ModelClass.get(data_config["id"])
        return pod_source

    def _set_local_persist_manager(self) -> None:
        self._verify_local_data_source()
        self._local_persist_manager = APIPersistManager(source_table_hash_id=self.source_table_hash_id, data_source_id=self.data_source_id)
        metadata = self._local_persist_manager.metadata

        assert metadata is not None, f"Verify that the table {self.source_table_hash_id} exists "


    def get_df_between_dates(self, start_date: Optional[datetime.datetime] = None,
                             end_date: Optional[datetime.datetime] = None,
                             unique_identifier_list: Optional[list] = None,unique_identifier_range_map:Optional[UniqueIdentifierRangeMap] = None,
                             great_or_equal: bool = True, less_or_equal: bool = True) -> pd.DataFrame:
        """
        Retrieves a DataFrame of time series data between specified dates.

        Args:
            start_date: The start date of the data range.
            end_date: The end date of the data range.
            unique_identifier_list: An optional list of unique identifiers to filter by.
            unique_identifier_range_map: An optional map of ranges for unique identifiers.
            great_or_equal: Whether to include the start date.
            less_or_equal: Whether to include the end date.

        Returns:
            A pandas DataFrame with the requested data.
        """
        filtered_data = self.local_persist_manager.get_df_between_dates(start_date=start_date,
                                                                        end_date=end_date,
                                                                        unique_identifier_list=unique_identifier_list,
                                                                        great_or_equal=great_or_equal,
                                                                        less_or_equal=less_or_equal,
                                                                        unique_identifier_range_map=unique_identifier_range_map,
                                                                        )
        if len(filtered_data) == 0:
            logger.info(f"Data from {self.source_table_hash_id} is empty in request ")
            logger.debug(
                f"Calling get_data_between_dates_from_api with arguments: "
                f"start_date={start_date}, end_date={end_date}, "
                f"great_or_equal={great_or_equal}, less_or_equal={less_or_equal}, "
                f"unique_identifier_list={unique_identifier_list}, "
                f"unique_identifier_range_map={unique_identifier_range_map}"
            )
            return filtered_data

        # fix types
        stc = self.local_persist_manager.metadata.sourcetableconfiguration
        filtered_data[stc.time_index_name] = pd.to_datetime(filtered_data[stc.time_index_name], utc=True)
        for c, c_type in stc.column_dtypes_map.items():
            if c != stc.time_index_name:
                if c_type == "object":
                    c_type = "str"
                filtered_data[c] = filtered_data[c].astype(c_type)
        filtered_data = filtered_data.set_index(stc.index_names)
        return filtered_data

    def filter_by_assets_ranges(self, unique_identifier_range_map: UniqueIdentifierRangeMap) -> pd.DataFrame:
        """
        Filters data by asset ranges.

        Args:
            unique_identifier_range_map: A map of unique identifiers to their date ranges.

        Returns:
            A pandas DataFrame with the filtered data.
        """
        df = self.local_persist_manager.filter_by_assets_ranges(unique_identifier_range_map, time_serie=self)
        return df

    @property
    def pickle_path(self) -> str:
        pp = data_source_dir_path(self.data_source_id)
        path = f"{pp}/{self.PICKLE_PREFIFX}{self.local_hash_id}.pickle"
        return path

    @classmethod
    def get_pickle_path(cls, source_table_hash_id: str, data_source_id: int) -> str:
        return f"{ogm.pickle_storage_path}/{data_source_id}/{cls.PICKLE_PREFIFX}{cls._get_local_hash_id()}.pickle"

    def persist_to_pickle(self, overwrite: bool = False) -> Tuple[str, str]:
        path = self.pickle_path
        # after persisting pickle , build_hash and source code need to be patched
        self.logger.debug(f"Persisting pickle")

        pp = data_source_pickle_path(self.data_source_id)
        if os.path.isfile(pp) == False or overwrite == True:
            self.data_source.persist_to_pickle(pp)

        if os.path.isfile(path) == False or overwrite == True:
            with open(path, 'wb') as handle:
                cloudpickle.dump(self, handle)
        return path, path.replace(ogm.pickle_storage_path + "/", "")

    def get_update_statistics(self, asset_symbols: Optional[list] = None) -> Tuple[Optional[datetime.datetime], Optional[Dict[str, datetime.datetime]]]:
        """
        Gets update statistics from the database.

        Args:
            asset_symbols: An optional list of asset symbols to filter by.

        Returns:
            A tuple containing the last update time for the table and a dictionary of last update times per asset.
        """
        last_update_in_table, last_update_per_asset = self.local_persist_manager.get_update_statistics(
            remote_table_hash_id=self.remote_table_hashed_name,
            asset_symbols=asset_symbols, time_serie=self)
        return last_update_in_table, last_update_per_asset

    def get_earliest_updated_asset_filter(self, unique_identifier_list: list,
                                          last_update_per_asset: dict) -> datetime.datetime:
        """
        Gets the earliest last update time for a list of unique identifiers.

        Args:
            unique_identifier_list: A list of unique identifiers.
            last_update_per_asset: A dictionary mapping assets to their last update times.

        Returns:
            The earliest last update timestamp.
        """
        if unique_identifier_list is not None:
            last_update_in_table = min(
                [t for a in last_update_per_asset.values() for t in a.values() if a in unique_identifier_list])
        else:
            last_update_in_table = min([t for a in last_update_per_asset.values() for t in a.values()])
        return last_update_in_table

    def update(self, *args, **kwargs) -> pd.DataFrame:
        self.logger.info("Not updating series")
        pass

class TimeSerieInitMeta(BaseModel):
    ...

class TimeSerie(CommonMethodsMixin, DataPersistanceMethods, GraphNodeMethods, TimeSerieRebuildMethods):
    """
    Base TimeSerie class
    """
    OFFSET_START = datetime.datetime(2018, 1, 1, tzinfo=pytz.utc)

    def __init__(
            self,
            init_meta:Optional[TimeSerieInitMeta] = None,
            build_meta_data: Union[dict, None] = None,
            local_kwargs_to_ignore: Union[List[str], None] = None,
            *args,
            **kwargs):
        """
        Initializes the TimeSerie object with the provided metadata and configurations. For extension of the method
        the method should always be decorated by TimeSerie._post_init_routines

        This method sets up the time series object, loading the necessary configurations
        and metadata.

        Each TimeSerie instance will create a table in the Main Sequence Data Engine by uniquely hashing
        the arguments with exception of:

        - init_meta
        - build_meta_data
        - local_kwargs_to_ignore

        Each TimeSerie instance will create a local_hash_id and a LocalTimeSerie instance in the Data Engine by uniquely hashing
        the same arguments as the table but excluding the arguments inside local_kwargs_to_ignore


        allowed type of arguments can only be str,list, int or  Pydantic objects inlcuding lists of Pydantic Objects.

        The OFFSET_START property can be overridend and markts the minimum date value where the table will insert data

        Parameters
        ----------
        init_meta : dict, optional
            Metadata for initializing the time series instance.
        build_meta_data : dict, optional
            Metadata related to the building process of the time series.
        local_kwargs_to_ignore : list, optional
            List of keyword arguments to ignore during configuration.
        *args : tuple
            Additional arguments.
        **kwargs : dict
            Additional keyword arguments.
        """

        self.init_meta = init_meta
        self.build_meta_data = self.sanitize_default_build_metadata(build_meta_data)
        self.local_kwargs_to_ignore = local_kwargs_to_ignore

        self.pre_load_routines_run = False
        self._data_source: Optional[DynamicTableDataSource] = None # is set later
        self._local_persist_manager: Optional[PersistManager] = None

        # asser that method is decorated
        if not len(self.__init__.__closure__) == 1:
            logger.error("init method is not decorated with @TimeSerie._post_init_routines()")
            raise Exception

    @staticmethod
    def set_context_in_logger(logger_context: Dict[str, Any]) -> None:
        """
        Binds context variables to the global logger.

        Args:
            logger_context: A dictionary of context variables.
        """
        global logger
        for key, value in logger_context.items():
            logger.bind(**dict(key=value))

    @staticmethod
    def get_time_serie_source_code(TimeSerieClass: "TimeSerie") -> str:
        """
        Gets the source code of a TimeSerie class.

        Args:
            TimeSerieClass: The class to get the source code for.

        Returns:
            The source code as a string.
        """
        global logger
        try:
            # First try the standard approach.
            source = inspect.getsource(TimeSerieClass)
            if source.strip():
                return source
        except Exception:
            logger.warning("Your TimeSeries is not in a python module this will likely bring exceptions when running in a pipeline")
        from IPython import get_ipython
        # Fallback: Scan IPython's input history.
        ip = get_ipython()  # Get the current IPython instance.
        if ip is not None:
            # Retrieve the full history as a single string.
            history = "\n".join(code for _, _, code in ip.history_manager.get_range())
            marker = f"class {TimeSerieClass.__name__}"
            idx = history.find(marker)
            if idx != -1:
                return history[idx:]
        return "Source code unavailable."

    @staticmethod
    def get_time_serie_source_code_git_hash(TimeSerieClass: "TimeSerie") -> str:
        """
        Hashes the source code of a TimeSerie class using SHA-1 (Git style).

        Args:
            TimeSerieClass: The class to hash.

        Returns:
            The Git-style hash of the source code.
        """
        time_serie_class_source_code = TimeSerieClass.get_time_serie_source_code(TimeSerieClass)
        # Prepare the content for Git-style hashing
        # Git hashing format: "blob <size_of_content>\0<content>"
        content = f"blob {len(time_serie_class_source_code)}\0{time_serie_class_source_code}"
        # Compute the SHA-1 hash (Git hash)
        hash_object = hashlib.sha1(content.encode('utf-8'))
        git_hash = hash_object.hexdigest()
        return git_hash

    def _post_init_routines() -> Callable:
        """
        A decorator to run routines after the __init__ method of a TimeSerie.
        It handles argument processing, configuration creation, and other setup tasks.
        """
        def wrap(init_method):
            @wraps(init_method)
            def run_init(*args, **kwargs):
                import inspect
                expected_arguments = [i for i in inspect.getfullargspec(init_method).args if i != "self"]
                signature = inspect.signature(init_method)
                default_arguments = {k: v.default
                                     for k, v in signature.parameters.items()
                                     if v.default is not inspect.Parameter.empty}
                post_init_log_messages = []
                for a in expected_arguments:
                    if a not in kwargs.keys():
                        if a in default_arguments.keys():
                            kwargs[a] = default_arguments[a]
                            post_init_log_messages.append(
                                f' In {args[0].__class__.__name__} Used default value for {a} in __init__({a}={default_arguments[a]} ,*args,**kwargs)')
                        else:
                            raise Exception(
                                f' In {args[0].__class__.__name__} explicitly declare argument {a} in __init__({a}= ,*args,**kwargs')
                init_method(*args, **kwargs)

                self = args[0]

                kwargs["time_series_class_import_path"] = {"module": self.__class__.__module__,
                                                           "qualname": self.__class__.__qualname__}

                self._create_config(kwargs=kwargs, post_init_log_messages=post_init_log_messages)
                # create logger
                self.run_after_post_init_routines()

                # patch if necesary build configuration
                self.patch_build_configuration()

            return run_init

        return wrap

    @staticmethod
    def sanitize_default_build_metadata(build_meta_data: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Sanitizes the build metadata, providing default values if necessary.
        """
        if build_meta_data is None:
            build_meta_data = {"initialize_with_default_partitions": True}

        if "initialize_with_default_partitions" not in build_meta_data.keys():
            build_meta_data["initialize_with_default_partitions"] = True
        return build_meta_data

    def get_html_description(self) -> Optional[str]:
        """
        Returns an HTML description of the TimeSerie instance.

        Returns:
            An HTML string with the description.
        """
        description = f"""<p>Time Serie Instance of {self.__class__.__name__} updating table {self.remote_table_hashed_name}</p>"""
        return description

    @property
    def hash_id(self) -> str:
        """The remote table hash name."""
        return self.remote_table_hashed_name

    @property
    def data_source_id(self) -> int:
        return self.data_source.id

    @property
    def local_hash_id(self) -> str:
        return self.hashed_name

    def _run_pre_load_routines(self) -> None:
        """
        Override this method to execute code after loading and before dependencies are updated.
        """
        self.pre_load_routines_run = True

    def get_data_source_from_orm(self) -> Any:
        from mainsequence.client import SessionDataSource
        if SessionDataSource.data_source.related_resource is None:
            raise Exception("This Pod does not have a default data source")
        return SessionDataSource.data_source


    @property
    def data_source(self) -> Any:
        if self._data_source is not None:
            return self._data_source
        else:
            raise Exception("Data source has not been set")

    def set_data_source(self, data_source: Optional[object] = None) -> None:
        """
        Sets the data source for the time series.

        Args:
            data_source: The data source object. If None, the default is fetched from the ORM.
        """
        if data_source is None:
            self._data_source = self.get_data_source_from_orm()
        else:
            self._data_source = data_source

    def run_after_post_init_routines(self) -> None:
        pass

    def __repr__(self) -> str:
        try:
            local_id = self.local_metadata.id
        except:
            local_id = 0
        repr = self.__class__.__name__ + f" {os.environ['TDAG_ENDPOINT']}/local-time-series/details/?local_time_serie_id={local_id}"
        return repr

    def _get_target_time_index(self, idx: Any) -> Any:
        raise NotImplementedError

    @property
    def local_time_serie(self) -> Any:
        """The local time series metadata object."""
        return self.local_persist_manager.local_metadata

    def _sanitize_init_meta(self, kwargs: Dict[str, Any]) -> Tuple[Dict[str, Any], "TimeSerieInitMeta"]:
        """
        Handles initial configuration in the pre-configuration process.

        Args:
            kwargs: The keyword arguments dictionary.

        Returns:
            A tuple containing the sanitized kwargs and the init_meta object.
        """
        if "init_meta" in kwargs.keys():
            init_meta = kwargs["init_meta"] if kwargs["init_meta"] is not None else TimeSerieInitMeta()
            kwargs.pop("init_meta", None)
        else:
            init_meta = TimeSerieInitMeta()
        return kwargs, init_meta

    def run(
            self,
            debug_mode: bool,
            *,
            update_tree: bool = True,
            force_update: bool = False,
            update_only_tree: bool = False,
            remote_scheduler: Union[object, None] = None
    ):
        """

        Args:
            debug_mode: if the time serie is run in debug mode the DAG will be run node by node in the same process
            update_tree: if set to False then only the selected time series will be run, default is True
            force_update: Force an update even if the time serie schedule does not require an update
            update_only_tree: If set to True then only the dependency graph of the selected time serie will be updated
            remote_scheduler:
        """
        from mainsequence.instrumentation import TracerInstrumentator
        from mainsequence.tdag.config import configuration
        from mainsequence.tdag.time_series.update.utils import UpdateInterface, wait_for_update_time
        from mainsequence.tdag.time_series.update.ray_manager import RayUpdateManager
        from mainsequence.client import Scheduler
        import gc
        global logger
        if update_tree:
            update_only_tree = False

        # set tracing
        tracer_instrumentator = TracerInstrumentator()
        tracer = tracer_instrumentator.build_tracer()
        error_on_update = None
        # 1 Create Scheduler for this time serie

        if remote_scheduler == None:
            name_prefix = "DEBUG_" if debug_mode else ""
            scheduler = Scheduler.build_and_assign_to_ts(
                scheduler_name=f"{name_prefix}{self.local_hash_id}_{self.data_source.id}",
                local_hash_id_list=[(self.local_hash_id, self.data_source.id)],
                remove_from_other_schedulers=True,
                running_in_debug_mode=debug_mode

            )
            scheduler.start_heart_beat()
        else:
            scheduler = remote_scheduler

        cvars.bind_contextvars(scheduler_name=scheduler.name,
                               head_local_ts_hash_id=self.local_hash_id)


        error_to_raise = None
        with tracer.start_as_current_span(f"Scheduler TS Head Update ") as span:
            span.set_attribute("time_serie_local_hash_id", self.local_hash_id)
            span.set_attribute("remote_table_hashed_name", self.remote_table_hashed_name)
            span.set_attribute("head_scheduler", scheduler.name)

            # 2 add actor manager for distributed
            distributed_actor_manager = RayUpdateManager(scheduler_uid=scheduler.uid,
                                                         skip_health_check=True
                                                         )
            try:
                local_time_series_map, state_data = self.pre_update_setting_routines(scheduler=scheduler,
                                                                               set_time_serie_queue_status=False,
                                                                               update_tree=update_tree)
                self.set_actor_manager(actor_manager=distributed_actor_manager)
                self.logger.debug("state set with dependencies metadatas")

                # 3 build update tracker
                update_tracker = UpdateInterface(head_hash=self.local_hash_id, trace_id=None,
                                                 logger=self.logger,
                                                 state_data=state_data, debug=debug_mode,
                                                 scheduler_uid=scheduler.uid
                                                 )
                self.update_tracker = update_tracker
                if force_update == False:
                    wait_for_update_time(local_hash_id=self.local_hash_id,
                                         data_source_id=self.data_source.id,
                                         logger=self.logger,
                                         force_next_start_of_minute=False)
                error_on_update = self.start_time_serie_update(debug_mode=debug_mode,
                                              raise_exceptions=True,
                                              force_update=force_update,
                                              update_tree=update_tree,
                                              update_only_tree=update_only_tree,
                                              local_time_series_map=local_time_series_map,
                                              update_tracker=self.update_tracker,
                                              use_state_for_update=True
                                              )
                del self.update_tracker
                gc.collect()
            except TimeoutError as te:
                self.logger.error("TimeoutError Error on update")
                error_to_raise = te
            except DependencyUpdateError as de:
                self.logger.error("DependecyError on update")
                error_to_raise = de
            except Exception as e:
                self.logger.exception(e)
                error_to_raise = e

        if remote_scheduler == None:
            scheduler.stop_heart_beat()
        if error_to_raise != None:
            raise error_to_raise

    @property
    def local_persist_manager(self) -> PersistManager:
        if self._local_persist_manager is None:
            self.logger.debug(f"Setting local persist manager for {self.hash_id}")
            self._set_local_persist_manager(local_hash_id=self.local_hash_id,
                                            remote_table_hashed_name=self.remote_table_hashed_name,

                                            )
        return self._local_persist_manager

    def _set_local_persist_manager(self, local_hash_id: str, remote_table_hashed_name: str,
                                   local_metadata: Union[None, dict] = None,
                                   time_serie_meta_build_configuration: Union[None, dict] = None,
                                   verify_local_run=True,
                                   ) -> None:
        """
        Initializes the local persistence manager for the time series. It sets up
        the necessary configurations and checks for existing metadata. If the metadata doesn't
        exist or is incomplete, it sets up the initial configuration and builds the update details.

        Args:
           hashed_name : str
               The local hash ID for the time series.
           remote_table_hashed_name : str
               The remote table hash name for the time series.
           local_metadata : Union[None, dict], optional
               Local metadata for the time series, if available.
        """
        self._local_persist_manager = PersistManager.get_from_data_type(
            local_hash_id=local_hash_id,
            class_name=self.__class__.__name__,
            local_metadata=local_metadata,
            data_source=self.data_source
        )

    def _verify_and_build_remote_objects(self) -> None:
        """Verifies and builds remote objects if they don't exist."""
        time_serie_source_code_git_hash = self.get_time_serie_source_code_git_hash(self.__class__)
        time_serie_source_code = self.get_time_serie_source_code(self.__class__)
        self._local_persist_manager.local_persist_exist_set_config(
            remote_table_hashed_name=self.remote_table_hashed_name,
            local_configuration=self.local_initial_configuration,
            remote_configuration=self.remote_initial_configuration,
            remote_build_metadata=self.remote_build_metadata,
            time_serie_source_code_git_hash=time_serie_source_code_git_hash,
            time_serie_source_code=time_serie_source_code,
            data_source=self.data_source,

        ) #this method always creates


    def flush_pickle(self) -> None:
        """Deletes the pickle file for this time series."""
        if os.path.isfile(self.pickle_path):
            os.remove(self.pickle_path)

    def patch_build_configuration(self) -> None:
        """
        Patches the build configuration for the time series and its dependencies.
        """
        patch_build = os.environ.get("PATCH_BUILD_CONFIGURATION", False) in ["true", "True", 1]
        if patch_build == True:
            self.local_persist_manager # ensure lpm exists
            self._verify_and_build_remote_objects()  # just call it before to initilaize dts
            self.logger.warning(f"Patching build configuration for {self.hash_id}")
            self.flush_pickle()

            self.local_persist_manager.patch_build_configuration(local_configuration=self.local_initial_configuration,
                                                                 remote_configuration=self.remote_initial_configuration,
                                                                 remote_build_metadata=self.remote_build_metadata,
                                                                 )

    def _create_config(self, kwargs: Dict[str, Any], post_init_log_messages: list) -> None:
        """
        Creates the configuration and hashes for the TimeSerie instance.

        Args:
            kwargs: The keyword arguments from __init__.
            post_init_log_messages: A list of log messages to be displayed.
        """
        global logger
        kwargs, init_meta = self._sanitize_init_meta(kwargs=kwargs)
        config_serializer = ConfigSerializer()

        kwargs = config_serializer.serialize_init_kwargs(kwargs)

        # hash config dict
        local_ts_hash, remote_table_hash = hash_signature(kwargs)
        local_hashed_name = self.__class__.__name__ + "_" + local_ts_hash
        local_hashed_name = local_hashed_name.lower()

        remote_hashed_name = self.__class__.__name__ + "_" + remote_table_hash
        remote_hashed_name = remote_hashed_name.lower()

        self.set_data_source()
        logger.bind(local_hash_id=local_hashed_name)
        for m in post_init_log_messages:
            logger.warning(m)

        if len(remote_hashed_name) > 60:
            logger.error(f"hashed name {remote_hashed_name} is to long {len(remote_hashed_name)} limit i 60 ")
            raise AssertionError
        self.hashed_name = local_hashed_name
        self.remote_table_hashed_name = remote_hashed_name
        self.local_initial_configuration = kwargs
        remote_initial_configuration = copy.deepcopy(kwargs)
        if "local_kwargs_to_ignore" in remote_initial_configuration:
            for k in remote_initial_configuration["local_kwargs_to_ignore"]:
                remote_initial_configuration.pop(k, None)
            remote_initial_configuration.pop("local_kwargs_to_ignore", None)
        self.remote_initial_configuration = remote_initial_configuration
        self.remote_build_metadata = self.sanitize_default_build_metadata(kwargs.get("build_meta_data", None))
        self.init_meta = init_meta

        logger.debug(f"local/remote {self.hashed_name}/{self.remote_table_hashed_name}")

    def set_update_uses_parallel(self, value: bool) -> None:
        """
        Sets the 'update_uses_parallel' metadata flag.
        """
        self.local_persist_manager.set_meta("update_uses_parallel", value)

    def set_dependencies_df(self) -> None:
        """Sets the dependencies DataFrame for the time series."""
        self.logger.debug("Initializing update priority ... ")
        depth_df = self.local_persist_manager.get_all_dependencies_update_priority()
        self.depth_df = depth_df

        self.dependencies_df = depth_df.copy()

        if depth_df.shape[0] > 0:
            self.dependencies_df = self.depth_df[self.depth_df["local_time_serie_id"] != self.local_persist_manager.local_metadata.id].copy()

    def pre_update_setting_routines(self, scheduler: "Scheduler", set_time_serie_queue_status: bool, update_tree: bool,
                                    metadata: Optional[dict] = None, local_metadata: Optional[dict] = None) -> Tuple[Dict, Any]:
        """
        Routines to execute before an update.

        Args:
            scheduler: The scheduler object.
            set_time_serie_queue_status: Whether to set the queue status.
            update_tree: Whether to update the tree.
            metadata: Optional remote metadata.
            local_metadata: Optional local metadata.

        Returns:
            A tuple containing the local metadata map and state data.
        """
        # reset metadata
        self.local_persist_manager.synchronize_metadata(local_metadata=local_metadata)
        self.set_relation_tree()
        if hasattr(self, "logger") == False:
            self._set_logger(self.hashed_name)

        update_priority_dict = None
        # build priority update

        if hasattr(self, "depth_df") == False:
            self.set_dependencies_df()

            self.logger.debug("Setting dependencies in scheduler active_tree")
            # only set once
            all_local_time_series_ids_in_tree = []

            if self.depth_df.shape[0] > 0:
                all_local_time_series_ids_in_tree = self.depth_df["local_time_serie_id"].to_list()
                if update_tree == True:
                    scheduler.in_active_tree_connect(local_time_series_ids=all_local_time_series_ids_in_tree + [self.local_persist_manager.local_metadata.id])

        depth_df = self.depth_df.copy()
        # set active tree connections

        if self.depth_df.shape[0] > 0:
            all_local_time_series_ids_in_tree = depth_df[["local_time_serie_id"]].to_dict("records")
        all_local_time_series_ids_in_tree.append({"local_time_serie_id":self.local_persist_manager.local_metadata.id})

        update_details_batch = dict(error_on_last_update=False,
                                    active_update_scheduler_uid=scheduler.uid)
        if set_time_serie_queue_status == True:
            update_details_batch['active_update_status'] = "Q"
        all_metadatas = self.get_metadatas_and_set_updates(local_time_series_ids=[i["local_time_serie_id"] for i in all_local_time_series_ids_in_tree],

                                                           update_details_kwargs=update_details_batch,
                                                           update_priority_dict=update_priority_dict,
                                                           )
        state_data, local_metadatas, source_table_config_map = all_metadatas['state_data'], all_metadatas[
            "local_metadatas"], all_metadatas["source_table_config_map"]
        local_metadatas = {m.id: m for m in local_metadatas}

        self.scheduler = scheduler

        self.update_details_tree = {key: v.run_configuration for key, v in local_metadatas.items()}
        return local_metadatas, state_data

    @tracer.start_as_current_span("Verify time series tree update")
    def _verify_tree_is_updated(
            self,
            local_time_series_map: Dict[int, "LocalTimeSerie"],
            debug_mode: bool,
            use_state_for_update: bool = False
    ) -> None:
        """
        Verifies that the dependency tree is updated.

        Args:
            local_time_series_map: A map of local time series objects.
            debug_mode: Whether to run in debug mode.
            use_state_for_update: If True, uses the current state for the update.
        """
        # build tree
        if self.is_local_relation_tree_set == False:
            start_tree_relationship_update_time = time.time()
            self.set_relation_tree()
            self.logger.debug(
                f"relationship tree updated took {time.time() - start_tree_relationship_update_time} seconds ")

        else:
            self.logger.debug("Tree is not updated as is_local_relation_tree_set== True")

        update_map = {}
        if use_state_for_update == True:
            update_map = self.get_update_map()

        self.logger.debug(f"Updating tree with update map {list(update_map.keys())} and dependencies {self.dependencies_df['local_hash_id'].to_list()}")

        if debug_mode == False:
            tmp_ts = self.dependencies_df.copy()
            if tmp_ts.shape[0] == 0:
                self.logger.debug("No dependencies in this time serie")
                return None
            tmp_ts = tmp_ts[tmp_ts["source_class_name"] != "WrapperTimeSerie"]

            if tmp_ts.shape[0] > 0:
                self._execute_parallel_distributed_update(tmp_ts=tmp_ts,
                                                          local_time_series_map=local_time_series_map,
                                                          )
        else:
            updated_uids = []
            if self.dependencies_df.shape[0] > 0:
                unique_priorities = self.dependencies_df["update_priority"].unique().tolist()
                unique_priorities.sort()

                local_time_series_list = self.dependencies_df[
                    self.dependencies_df["source_class_name"] != "WrapperTimeSerie"
                    ][["local_hash_id", "data_source_id"]].values.tolist()
                for prioriity in unique_priorities:
                    # get hierarchies ids
                    tmp_ts = self.dependencies_df[self.dependencies_df["update_priority"] == prioriity].sort_values(
                        "number_of_upstreams", ascending=False).copy()

                    tmp_ts = tmp_ts[tmp_ts["source_class_name"] != "WrapperTimeSerie"]
                    tmp_ts = tmp_ts[~tmp_ts.index.isin(updated_uids)]

                    # update on the same process
                    for row, ts_row in tmp_ts.iterrows():

                        if (ts_row["local_hash_id"], ts_row["data_source_id"]) in update_map.keys():
                            ts = update_map[(ts_row["local_hash_id"], ts_row["data_source_id"])]["ts"]
                        else:
                            try:

                                ts, _ = self.rebuild_and_set_from_local_hash_id(local_hash_id=ts_row["local_hash_id"],
                                                                                data_source_id=ts_row["data_source_id"]
                                                                                )

                            except Exception as e:
                                self.logger.exception(
                                    f"Error updating dependency {ts_row['local_hash_id']} when loading pickle")
                                raise e

                        try:

                            error_on_last_update = ts.start_time_serie_update(debug_mode=debug_mode,
                                                             raise_exceptions=True,
                                                             update_tree=False,
                                                             update_tracker=self.update_tracker
                                                             )

                            
                        except Exception as e:
                            self.logger.exception(f"Error updating dependencie {ts.local_hash_id}")
                            raise e
                    updated_uids.extend(tmp_ts.index.to_list())
        self.logger.debug(f'Dependency Tree evaluated for  {self}')

    def set_actor_manager(self, actor_manager: object) -> None:
        """
        Sets the actor manager for distributed updates.

        Args:
            actor_manager: The actor manager object.
        """
        self.update_actor_manager = actor_manager

    @tracer.start_as_current_span("Execute distributed parallel update")
    def _execute_parallel_distributed_update(self, tmp_ts: pd.DataFrame,
                                             local_time_series_map: Dict[int, "LocalTimeSerie"]) -> None:
        """
        Executes a parallel distributed update of dependencies.

        Args:
            tmp_ts: A DataFrame of time series to update.
            local_time_series_map: A map of local time series objects.
        """
        telemetry_carrier = tracer_instrumentator.get_telemetry_carrier()

        pre_loaded_ts = [t.hash_id for t in self.scheduler.pre_loads_in_tree]
        tmp_ts = tmp_ts.sort_values(["update_priority", "number_of_upstreams"], ascending=[True, False])
        pre_load_df = tmp_ts[tmp_ts["local_time_serie_id"].isin(pre_loaded_ts)].copy()
        tmp_ts = tmp_ts[~tmp_ts["local_time_serie_id"].isin(pre_loaded_ts)].copy()
        tmp_ts = pd.concat([pre_load_df, tmp_ts], axis=0)

        futures_ = []

        local_time_series_list = self.dependencies_df[
            self.dependencies_df["source_class_name"] != "WrapperTimeSerie"
            ]["local_time_serie_id"].values.tolist()

        for counter, (uid, data) in enumerate(tmp_ts.iterrows()):
            local_time_serie_id = data['local_time_serie_id']
            data_source_id = data['data_source_id']
            local_hash_id=data['local_hash_id']

            kwargs_update = dict(local_time_serie_id=local_time_serie_id,
                                 local_hash_id=local_hash_id,
                                 data_source_id=data_source_id,
                                 telemetry_carrier=telemetry_carrier,
                                 scheduler_uid=self.scheduler.uid
                                 )

            update_details = self.update_details_tree[local_time_serie_id]
            run_configuration=local_time_series_map[local_time_serie_id].run_configuration
            num_cpus = run_configuration.required_cpus

            task_kwargs = dict(task_options={"num_cpus": num_cpus,
                                             "name": f"local_time_serie_id_{local_time_serie_id}",

                                             "max_retries": run_configuration.retry_on_error},
                               kwargs_update=kwargs_update)

            p = self.update_actor_manager.launch_update_task(**task_kwargs)

            # p = self.update_actor_manager.launch_update_task_in_process( **task_kwargs  )
            # continue
            # logger.warning("REMOVE LINES ABOVE FOR DEBUG")

            futures_.append(p)

            # are_dependencies_updated, all_dependencies_nodes, pending_nodes, error_on_dependencies = self.update_tracker.get_pending_update_nodes(
            #     hash_id_list=list(all_start_data.keys()))
            # self.are_dependencies_updated( target_nodes=all_dependencies_nodes)
            # raise Exception

        # block update until termination
        try:
            self._run_pre_load_routines()
        except Exception as e:
            self.logger.exception("Error running pre_load_routines")
            error_on_dependencies = True

        tasks_with_errors = self.update_actor_manager.get_results_from_futures_list(futures=futures_)
        if len(tasks_with_errors) > 0:
            raise DependencyUpdateError(f"Update Stop from error in Ray in tasks {tasks_with_errors}")
        # verify there is no error in hierarchy. this prevents to updating next level if dependencies fails

        dependencies_update_details = LocalTimeSerieUpdateDetails.filter(
            related_table__id__in=tmp_ts["local_time_serie_id"].astype(str).to_list())
        ts_with_errors = []
        for local_ts_update_details in dependencies_update_details:
            if local_ts_update_details.error_on_last_update == True:
                ts_with_errors.append(local_ts_update_details.related_table.id)
        # Verify there are no errors after finishing hierarchy
        if len(ts_with_errors) > 0:
            raise DependencyUpdateError(f"Update Stop from error in children \n {ts_with_errors}")

    @tracer.start_as_current_span("TimeSerie.update_local")
    def update_local(self, update_tree: bool, update_tracker: object, debug_mode: bool,
                     update_statistics: DataUpdates,
                     local_time_series_map: Optional[dict] = None,
                     overwrite_latest_value: Optional[datetime.datetime] = None, update_only_tree: bool = False,
                     use_state_for_update: bool = False,
                     *args, **kwargs) -> Optional[bool]:
        """
        Performs a local update of the time series data.

        Args:
            update_tree: Whether to update the dependency tree.
            update_tracker: The update tracker object.
            debug_mode: Whether to run in debug mode.
            update_statistics: The data update statistics.
            local_time_series_map: A map of local time series objects.
            overwrite_latest_value: An optional timestamp to overwrite the latest value.
            update_only_tree: If True, only updates the dependency tree structure.
            use_state_for_update: If True, uses the current state for the update.

        Returns:
            True if data was persisted, False otherwise.
        """
        from mainsequence.instrumentation.utils import Status, StatusCode
        persisted = False
        if update_tree == True:

            self._verify_tree_is_updated(debug_mode=debug_mode,
                                         local_time_series_map=local_time_series_map,
                                         use_state_for_update=use_state_for_update,
                                         )
            if update_only_tree == True:
                self.logger.info(f'Local Time Series  {self} only tree updated')
                return None

        # hardcore check to fix missing values
        # from mainsequence.tdag_client.utils import read_one_value_from_table
        #
        # if self.local_persist_manager.metadata["sourcetableconfiguration"] is None:
        #     r=read_one_value_from_table(self.hashed_name)
        #     if len(r)>0:
        #         #there is data but not source table configuration overwrite
        #         overwrite_latest_value=datetime.datetime(2023,6,23).replace(tzinfo=pytz.utc)

        with tracer.start_as_current_span("Update Calculation") as update_span:

            if overwrite_latest_value is not None:  # overwrite latest values is passed form def_update method to reduce calls to api
                latest_value = overwrite_latest_value

                self.logger.info(f'Updating Local Time Series for  {self}  since {latest_value}')
                temp_df = self.update(update_statistics)

                if temp_df.shape[0] == 0:
                    # concatenate empty

                    self.logger.info(f'Local Time Series Nothing to update  {self}  updated')
                    return False

                for col, ddtype in temp_df.dtypes.items():
                    if "datetime64" in str(ddtype):
                        self.logger.info(f"WARNING DATETIME TYPE IN {self}")
                        raise Exception(f"""Datetime in {col}
                                            {temp_df}""")
                self.logger.info(f'Persisting Time Series for  {self}  since {latest_value} ')

            else:
                if not update_statistics:
                    self.logger.info(f'Updating Local Time Series for  {self}  for first time')
                try:
                    temp_df = self.update(update_statistics)
                    temp_df = update_statistics.filter_df_by_latest_value(temp_df)
                    temp_df.replace([np.inf, -np.inf], np.nan, inplace=True)

                except Exception as e:
                    import traceback
                    traceback.print_exc()
                    raise e

                if not temp_df.empty:
                    lvl0 = temp_df.index.get_level_values(0)
                    is_dt64_utc = str(lvl0.dtype) == "datetime64[ns, UTC]"
                    assert is_dt64_utc, f"Time index must be datetime64[ns, UTC] ({lvl0} is used)"
                else:
                    self.logger.warning(f"Time Series {self} does not return data from update")

                for col in temp_df.columns:
                    assert col.islower(), f"Error Column '{col}': Column names must be lower case"

                for col, ddtype in temp_df.dtypes.items():
                    if "datetime64" in str(ddtype):
                        self.logger.info(f"WARNING DATETIME TYPE IN {self}")
                        raise Exception

            try:

                # verify index order is correct
                overwrite = True if overwrite_latest_value is not None else False
                persisted = self.persist_updated_data(temp_df,
                                                      update_tracker=update_tracker,
                                                      overwrite=overwrite)

                update_span.set_status(Status(StatusCode.OK))
            except Exception as e:
                self.logger.exception("Error updating time serie")
                update_span.set_status(Status(StatusCode.ERROR))
                raise e
            self.logger.info(f'Local Time Series  {self}  updated')

            return persisted

    def _run_post_update_routines(self, error_on_last_update: bool, update_statistics: DataUpdates) -> None:
        """ Should be overwritten by subclass """
        pass

    def _get_column_metadata(self) -> Optional[List[ColumnMetaData]]:
        """
        This Method should return a list for ColumnMetaData to add extra context to each time series
        Examples:
            from mainsequence.client.models_tdag import ColumnMetaData
        columns_metadata = [ColumnMetaData(column_name="instrument",
                                          dtype="str",
                                          label="Instrument",
                                          description=(
                                              "Unique identifier provided by Valmer; it’s a composition of the "
                                              "columns `tv_emisora_serie`, and is also used as a ticker for custom "
                                              "assets in Valmer."
                                          )
                                          ),
                            ColumnMetaData(column_name="currency",
                                           dtype="str",
                                           label="Currency",
                                           description=(
                                               "Corresponds to  code for curries be aware this may not match Figi Currency assets"
                                           )
                                           ),

                            ]
        Returns:
            A list of ColumnMetaData objects, or None.
        """
        return None

    def _set_column_metadata(self) -> None:
        if self.metadata:
            if self.metadata.sourcetableconfiguration != None:
                if self.metadata.sourcetableconfiguration.columns_metadata is not None:
                    columns_metadata=self._get_column_metadata()
                    if columns_metadata is None:
                        self.logger.info(f"_get_column_metadata method not implemented")
                        return

                    self.metadata.sourcetableconfiguration.set_or_update_columns_metadata(columns_metadata=columns_metadata)

    def _get_asset_list(self) -> Optional[List["Asset"]]:
        """
        Provide the list of assets that this TimeSerie should include when updating.

        By default, this method returns `self.asset_list` if defined.
        Subclasses _must_ override this method when no `asset_list` attribute was set
        during initialization, to supply a dynamic list of assets for update_statistics.

        Use Case:
          - For category-based series, return all Asset unique_identifiers in a given category
            (e.g., `AssetCategory(unique_identifier="investable_assets")`), so that only those
            assets are updated in this TimeSerie.

        Returns
        -------
        list or None
            - A list of asset unique_identifiers to include in the update.
            - `None` if no filtering by asset is required (update all assets by default).
        """
        if hasattr(self, "asset_list"):
            return self.asset_list

        return None

    def set_update_statistics(self, update_statistics: DataUpdates) -> DataUpdates:
        """
        Default method to narrow down update statistics un local time series,
        the method will filter using asset_list if the attribute exists as well as the init fallback date
        :param update_statistics:
        :return:
        """
        # Filter update_statistics to include only assets in self.asset_list.

        asset_list = self._get_asset_list()
        self._setted_asset_list = asset_list

        update_statistics = update_statistics.update_assets(
            asset_list, init_fallback_date=self.OFFSET_START
        )
        return update_statistics

    def update(self, update_statistics: DataUpdates) -> pd.DataFrame:
        """
        Fetch and ingest only the new rows for this TimeSerie based on prior update checkpoints.

        DataUpdates provides the last-ingested positions:
          - For a single-index series (time_index only), `update_statistics.max_time` is either:
              - None: no prior data—fetch all available rows.
              - a datetime: fetch rows where `time_index > max_time`.
          - For a dual-index series (time_index, unique_identifier), `update_statistics.max_time_per_id` is either:
              - None: single-index behavior applies.
              - dict[str, datetime]: for each `unique_identifier` (matching `Asset.unique_identifier`), fetch rows where
                `time_index > max_time_per_id[unique_identifier]`.

        Requirements:
          - `time_index` **must** be a `datetime.datetime` instance with UTC timezone.
          - Column names **must** be all lowercase.
          - No column values may be Python `datetime` objects; if date/time storage is needed, convert to integer
            timestamps (e.g., UNIX epoch in seconds or milliseconds).

        After retrieving the incremental rows, this method inserts or upserts them into the Main Sequence Data Engine.

        Parameters
        ----------
        update_statistics : DataUpdates
            Object capturing the previous update state. Must expose:
              - `max_time` (datetime | None)
              - `max_time_per_id` (dict[str, datetime] | None)

        Returns
        -------
        pd.DataFrame
            A DataFrame containing only the newly added or updated records.
        """
        raise NotImplementedError


class WrapperTimeSerie(TimeSerie):
    """A wrapper class for managing multiple TimeSerie objects."""

    @TimeSerie._post_init_routines()
    def __init__(self, translation_table: AssetTranslationTable, *args, **kwargs):
        """
        Initialize the WrapperTimeSerie.

        Args:
            time_series_dict: Dictionary of TimeSerie objects.
        """
        super().__init__(*args, **kwargs)

        def get_time_serie_from_markets_unique_id(market_time_serie_unique_identifier: str) -> TimeSerie:
            """
            Returns the appropriate bar time series based on the asset list and source.
            """
            from mainsequence.client import DoesNotExist
            try:
                hbs = MarketsTimeSeriesDetails.get(unique_identifier=market_time_serie_unique_identifier, include_relations_detail=True)
            except DoesNotExist as e:
                logger.exception(f"HistoricalBarsSource does not exist for {market_time_serie_unique_identifier}")
                raise e
            api_ts = APITimeSerie(
                data_source_id=hbs.source_table.data_source,
                source_table_hash_id=hbs.source_table.hash_id
            )
            return api_ts

        translation_table = copy.deepcopy(translation_table)

        self.api_ts_map = {}
        for rule in translation_table.rules:
            if rule.markets_time_serie_unique_identifier not in self.api_ts_map:
                self.api_ts_map[rule.markets_time_serie_unique_identifier] = get_time_serie_from_markets_unique_id(
                    market_time_serie_unique_identifier=rule.markets_time_serie_unique_identifier)

        self.translation_table = translation_table
    
    
    def get_ranged_data(self, range_descriptor: Optional[UniqueIdentifierRangeMap]) -> pd.DataFrame:
        """
        Gets data based on a range descriptor.

        Args:
            range_descriptor: A UniqueIdentifierRangeMap object.

        Returns:
            A DataFrame with the ranged data.
        """
        return self.get_df_between_dates(unique_identifier_range_map=range_descriptor)
    
    def get_df_between_dates(
            self,
            start_date: Optional[datetime.datetime] = None,
            end_date: Optional[datetime.datetime] = None,
            unique_identifier_list: Optional[list] = None,
            great_or_equal: bool = True,
            less_or_equal: bool = True,
            unique_identifier_range_map: Optional[UniqueIdentifierRangeMap] = None,
    ) -> pd.DataFrame:
        """
        Retrieves a DataFrame of time series data between specified dates, handling asset translation.

        Args:
            start_date: The start date of the data range.
            end_date: The end date of the data range.
            unique_identifier_list: An optional list of unique identifiers to filter by.
            great_or_equal: Whether to include the start date.
            less_or_equal: Whether to include the end date.
            unique_identifier_range_map: An optional map of ranges for unique identifiers.

        Returns:
            A pandas DataFrame with the requested data.
        """
        if (unique_identifier_list is None) == (unique_identifier_range_map is None):
            raise ValueError(
                "Pass **either** unique_identifier_list **or** unique_identifier_range_map, but not both."
            )

        if unique_identifier_list is not None:
            wanted_src_uids = set(unique_identifier_list)
        else:  # range‑map path
            wanted_src_uids = set(unique_identifier_range_map.keys())

        if not wanted_src_uids:
            return pd.DataFrame()

        # evaluate the rules for each asset
        from mainsequence.client import Asset
        assets = Asset.filter(unique_identifier__in=list(wanted_src_uids))
        asset_translation_dict = {}
        for asset in assets:
            asset_translation_dict[asset.unique_identifier] = self.translation_table.evaluate_asset(asset)

        # we grouped the assets for the same rules together and now query all assets that have the same target
        translation_df = pd.DataFrame.from_dict(asset_translation_dict, orient="index")
        try:
            grouped = translation_df.groupby(
                ["markets_time_serie_unique_identifier", "execution_venue_symbol", "exchange_code"],
                dropna=False
            )
        except Exception as e:
            raise e

        data_df = []
        for (mkt_ts_id, target_execution_venue_symbol, target_exchange_code), group_df in grouped:
            # get the correct TimeSerie instance from our pre-built map
            api_ts = self.api_ts_map[mkt_ts_id]

            # figure out which assets belong to this group
            grouped_unique_ids = group_df.index.tolist()
            source_assets = [
                a for a in assets
                if a.unique_identifier in grouped_unique_ids
            ]

            # get correct target assets based on the share classes
            main_sequence_share_classes = [a.main_sequence_share_class for a in assets]



            asset_query = dict(
                execution_venue__symbol=target_execution_venue_symbol,
                main_sequence_share_class__in=main_sequence_share_classes
            )
            if not pd.isna(target_exchange_code):
                asset_query["exchange_code"] = target_exchange_code

            target_assets = Asset.filter(**asset_query)

            if len(main_sequence_share_classes) != len( target_assets):
                self.logger.warning(f"Not all assets were found in backend with translation information with query {asset_query}")

            # create the source-target mapping
            source_asset_share_class_map = {}
            for a in source_assets:
                if a.main_sequence_share_class in source_asset_share_class_map:
                    raise ValueError(f"Share class {a.main_sequence_share_class} cannot be duplicated")
                source_asset_share_class_map[a.main_sequence_share_class] = a.unique_identifier

            source_target_map = {}
            for a in target_assets:
                main_sequence_share_class = a.main_sequence_share_class
                source_unique_identifier = source_asset_share_class_map[main_sequence_share_class]
                source_target_map[source_unique_identifier] = a.unique_identifier

            target_source_map = {v: k for k, v in source_target_map.items()}
            if unique_identifier_range_map is not None:
                # create the correct unique identifier range map
                unique_identifier_range_map_target = {}
                for a_unique_identifier, asset_range in unique_identifier_range_map.items():
                    if a_unique_identifier not in source_target_map.keys(): continue
                    target_key = source_target_map[a_unique_identifier]
                    unique_identifier_range_map_target[target_key] = asset_range

                if not unique_identifier_range_map_target:
                    self.logger.warning(
                        f"Unique identifier map is empty for group assets {source_assets} and unique_identifier_range_map {unique_identifier_range_map}")
                    continue

                tmp_data = api_ts.get_df_between_dates(
                    unique_identifier_range_map=unique_identifier_range_map_target,
                    start_date=start_date,
                    end_date=end_date,
                    great_or_equal=great_or_equal,
                    less_or_equal=less_or_equal,
                )
            else:
                tmp_data = api_ts.get_df_between_dates(
                    start_date=start_date,
                    end_date=end_date,
                    unique_identifier_list=list(source_target_map.values()),
                    great_or_equal=great_or_equal,
                    less_or_equal=less_or_equal,
                )

            if tmp_data.empty:
                continue

            tmp_data = tmp_data.rename(index=target_source_map, level="unique_identifier")
            data_df.append(tmp_data)

        if not data_df:
            return pd.DataFrame()

        data_df = pd.concat(data_df, axis=0)
        return data_df

    def update(self, update_statistics):
        """ WrapperTimeSeries does not update """
        pass