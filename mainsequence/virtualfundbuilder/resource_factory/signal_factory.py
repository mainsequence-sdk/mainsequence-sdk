import ast
import inspect
import os
from datetime import timedelta

import numpy as np
import pandas as pd

from mainsequence.tdag.data_nodes import DataNode
from mainsequence.virtualfundbuilder.enums import ResourceType
from mainsequence.virtualfundbuilder.models import AssetsConfiguration
from mainsequence.virtualfundbuilder.resource_factory.base_factory import (
    BaseFactory,
    BaseResource,
    insert_in_registry,
)
from mainsequence.virtualfundbuilder.utils import get_vfb_logger

logger = get_vfb_logger()


class WeightsBase(BaseResource):
    TYPE = ResourceType.SIGNAL_WEIGHTS_STRATEGY

    def __init__(self, signal_assets_configuration: AssetsConfiguration, *args, **kwargs):
        """
        Base Class for all signal weights

        Attributes:
            assets_configuration (AssetsConfiguration): Configuration details for signal assets.
        """
        self.assets_configuration = signal_assets_configuration
        super().__init__()

    def get_explanation(self):
        info = f"""
        <p>{self.__class__.__name__}: Signal weights class.</p>
        """
        return info

    def maximum_forward_fill(self) -> timedelta:
        raise NotImplementedError

    def get_asset_uid_to_override_portfolio_price(self):

        return None

    def interpolate_index(self, new_index: pd.DatetimeIndex):
        """
        Get interpolated weights for a time index. Weights are only valid for a certain time, therefore forward fill is limited.
        Especially needed for gaps within the weights
        """
        # get values between new index
        try:
            weights = self.get_df_between_dates(
                start_date=new_index.min(), end_date=new_index.max()
            )
        except Exception as e:
            raise e

        # if we need more data before to interpolate first value of new_index
        if (
            len(weights) == 0
        ):  # or (weights.index.get_level_values("time_index").min() > new_index.min()):

            if self.update_statistics.asset_time_statistics is None:
                raise Exception("Signal has not been updated")


            unique_identifier_range_map = {
                a: {"start_date": d}
                for a, d in self.update_statistics.asset_time_statistics.items()
            }
            last_observation = self.get_df_between_dates(
                unique_identifier_range_map=unique_identifier_range_map
            )
            if last_observation is None or last_observation.empty:
                return pd.DataFrame()
            last_date = last_observation.index.get_level_values("time_index")[0]

            if last_date < new_index.min():
                self.logger.warning(
                    f"No weights data at start of the portfolio at {new_index.min()}"
                    f" will use last available weights {last_date}"
                )
                weights = self.get_df_between_dates(start_date=last_date, end_date=new_index.max())

        if len(weights) == 0:
            self.logger.warning("No weights data in index interpolation")
            return pd.DataFrame()

        weights_pivot = (
            weights.reset_index()
            .pivot(index="time_index", columns=["unique_identifier"], values="signal_weight")
            .fillna(0)
        )
        weights_pivot["last_weights"] = weights_pivot.index.get_level_values(level="time_index")

        # combine existing index with new index
        combined_index = weights_pivot.index.union(new_index)
        combined_index.name = "time_index"
        weights_reindex = weights_pivot.reindex(combined_index)

        # check which dates are outside of valid forward filling range
        weights_reindex["last_weights"] = weights_reindex["last_weights"].ffill()
        weights_reindex["diff_to_last_weights"] = (
            weights_reindex.index.get_level_values(level="time_index")
            - weights_reindex["last_weights"]
        )

        invalid_forward_fills = (
            weights_reindex["diff_to_last_weights"] >= self.maximum_forward_fill()
        )  # source_frequency is the duration a weight is valid
        weights_reindex.drop(columns=["last_weights", "diff_to_last_weights"], inplace=True)

        # forward fill and set dates that are outside of valid range to nan
        weights_reindex = weights_reindex.ffill()
        weights_reindex[invalid_forward_fills] = np.nan

        if weights_reindex.isna().values.any():
            self.logger.info("Could not fully interpolate for signal weights")

        weights_reindex = weights_reindex.loc[new_index]
        weights_reindex.index.name = "time_index"

        return weights_reindex


def _get_class_source_code(cls):
    import inspect
    import sys

    try:
        # Get the source code of the module where the class is defined
        module = sys.modules[cls.__module__]
        source = inspect.getsource(module)
    except Exception as e:
        logger.warning(f"Could not get source code for module {cls.__module__}: {e}")
        return None

    # Parse the module's source code
    try:
        module_ast = ast.parse(source)
        class_source_code = None

        # Iterate through the module's body to find the class definition
        for node in module_ast.body:
            if isinstance(node, ast.ClassDef) and node.name == cls.__name__:
                # Get the lines corresponding to the class definition
                lines = source.splitlines()
                # Get the lines for the class definition
                class_source_lines = lines[node.lineno - 1 : node.end_lineno]
                class_source_code = "\n".join(class_source_lines)
                break

        if not class_source_code:
            logger.warning(
                f"Class definition for {cls.__name__} not found in module {cls.__module__}"
            )
            return None

        return class_source_code

    except Exception as e:
        logger.warning(f"Could not parse source code for module {cls.__module__}: {e}")
        return None


SIGNAL_CLASS_REGISTRY = SIGNAL_CLASS_REGISTRY if "SIGNAL_CLASS_REGISTRY" in globals() else {}


def register_signal_class(name=None, register_in_agent=True):
    """
    Decorator to register a model class in the factory.
    If `name` is not provided, the class's name is used as the key.
    """

    def decorator(cls):
        if os.environ.get("IGNORE_MS_AGENT", "false").lower() == "true":
            logger.info("Ignoring MS agent registration")
            return cls
        code = inspect.getsource(cls)
        attributes = {"code": code}
        return insert_in_registry(
            SIGNAL_CLASS_REGISTRY, cls, register_in_agent, attributes=attributes
        )

    return decorator


class SignalWeightsFactory(BaseFactory):
    @staticmethod
    def get_signal_weights_strategy(signal_weights_name) -> DataNode:
        """
        Creates an instance of the appropriate SignalWeights class based on the provided name.
        """
        if signal_weights_name not in SIGNAL_CLASS_REGISTRY:
            SignalWeightsFactory.get_signal_weights_strategies()

        return SIGNAL_CLASS_REGISTRY[signal_weights_name]

    @staticmethod
    def get_signal_weights_strategies():
        """
        Scans the given directory for Python files, imports the classes,
        and returns all classes that are subclasses of WeightsBase.
        """
        SignalWeightsFactory.import_module("data_nodes")
        return SIGNAL_CLASS_REGISTRY
