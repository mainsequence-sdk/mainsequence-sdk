import copy
import os
import re

import pandas as pd
import yaml

from mainsequence.client import (
    Portfolio,
    PortfolioIndexAsset,
)
from mainsequence.tdag.utils import write_yaml

from .config_handling import configuration_sanitizer
from .data_nodes import PortfolioFromDF, PortfolioStrategy
from .models import PortfolioConfiguration, PortfolioMarketsConfig
from .utils import get_vfb_logger


class PortfolioInterface:
    """
    Manages the overall strategy of investing. It initializes the tree and runs it either within the scheduler or
    directly with a full tree update.
    """

    def __init__(
        self,
        portfolio_config_template: dict,
        configuration_name: str = None,
        is_portfolio_from_df=False,
    ):
        """
        Initializes the portfolio strategy with the necessary configurations.
        """
        if is_portfolio_from_df == True:
            return None
        if configuration_name:
            self.check_valid_configuration_name(configuration_name)
        self.portfolio_config_template = portfolio_config_template
        self.portfolio_config = configuration_sanitizer(portfolio_config_template)
        self.configuration_name = configuration_name

        self.portfolio_markets_config = self.portfolio_config.portfolio_markets_configuration
        self.portfolio_build_configuration = self.portfolio_config.portfolio_build_configuration
        self.logger = get_vfb_logger()
        self._is_initialized = False

    def __str__(self):
        configuration_name = self.configuration_name or "-"
        str_configuration = yaml.dump(self.portfolio_config_template, default_flow_style=False)
        return f"Configuration Name: {configuration_name}\n{str_configuration}"

    def __repr__(self):
        return self.__str__()

    @classmethod
    def build_from_portfolio_node(
        cls,
        portfolio_node: PortfolioStrategy,portfolio_markets_config:PortfolioMarketsConfig,
    ) -> pd.DataFrame:
        assert isinstance(portfolio_node, PortfolioStrategy)
        interface = cls(portfolio_config_template=None, is_portfolio_from_df=True)
        interface._is_initialized = True
        interface.portfolio_strategy_data_node = portfolio_node
        interface.portfolio_markets_config=portfolio_markets_config
        interface.portfolio_build_configuration=portfolio_node.portfolio_build_configuration
        interface.logger = get_vfb_logger()
        return interface

    @classmethod
    def build_and_run_portfolio_from_df(
        cls,
        portfolio_node: PortfolioFromDF,
        debug_mode=True,
        force_update=True,
        update_tree=True,
        portfolio_tags: list[str] = None,
        add_portfolio_to_markets_backend=False,
    ):
        assert isinstance(portfolio_node, PortfolioFromDF)

        interface = cls(portfolio_config_template=None, is_portfolio_from_df=True)
        interface._is_initialized = True
        interface.portfolio_strategy_data_node = portfolio_node
        interface.logger = get_vfb_logger()
        interface.run(
            patch_build_configuration=False,
            debug_mode=debug_mode,
            force_update=force_update,
            update_tree=update_tree,
            portfolio_tags=portfolio_tags,
            add_portfolio_to_markets_backend=False,
        )

        ## manualely
        target_portfolio = Portfolio.get_or_none(
            data_node_update__id=portfolio_node.data_node_update.id
        )
        standard_kwargs = dict(
            portfolio_name=portfolio_node.portfolio_name,
            data_node_update_id=portfolio_node.data_node_update.id,
            signal_data_node_update_id=None,
            calendar_name=portfolio_node.calendar_name,
            target_portfolio_about=dict(
                description=portfolio_node.target_portfolio_about,
                signal_name=None,
                signal_description=None,
                rebalance_strategy_name=None,
            ),
            backtest_table_price_column_name="close",
        )
        if target_portfolio is None:
            target_portfolio, index_asset = Portfolio.create_from_time_series(**standard_kwargs)
        else:
            # patch timeserie of portfolio to guaranteed recreation
            target_portfolio.patch(**standard_kwargs)

            index_asset = PortfolioIndexAsset.get(reference_portfolio__id=target_portfolio.id)
        return target_portfolio, index_asset

    def _initialize_nodes(self, patch_build_configuration=True) -> None:
        """
        Initializes the portfolio strategy for backtesting and for live prediction.
        Also, forces an update of the build configuration in tdag to guarantee that assets are properly rebuilt
        patch_build_configuration:defaults to True as we want to patch the configuration while we test but for production can be set to False
        """
        patch = os.environ.get("PATCH_BUILD_CONFIGURATION", "False")
        os.environ["PATCH_BUILD_CONFIGURATION"] = (
            "True" if patch_build_configuration else "False"
        )  # It always needs to be true as we always want to overwrite the build
        self.portfolio_strategy_data_node = PortfolioStrategy(
            portfolio_build_configuration=copy.deepcopy(self.portfolio_build_configuration)
        )

        os.environ["PATCH_BUILD_CONFIGURATION"] = patch
        self._is_initialized = True

    def build_target_portfolio_in_backend(
        self, portfolio_tags=None
    ) -> tuple[Portfolio, PortfolioIndexAsset]:
        """
        This method creates a portfolio in VAM with configm file settings.

        Returns:
        """
        if not self._is_initialized:
            self._initialize_nodes()

        portfolio_ts = self.portfolio_strategy_data_node

        def build_markets_portfolio(ts, portfolio_tags):
            # when is live target portfolio
            signal_weights_ts = ts.signal_weights

            # timeseries can be running in local lake so need to request the id
            standard_kwargs = dict(
                data_node_update_id=ts.data_node_update.id,
                signal_data_node_update_id=signal_weights_ts.data_node_update.id,
            )

            user_kwargs = self.portfolio_markets_config.model_dump()
            user_kwargs.pop("front_end_details", None)

            standard_kwargs.update(user_kwargs)
            standard_kwargs["calendar_name"] = (
                self.portfolio_build_configuration.backtesting_weights_configuration.rebalance_strategy_configuration[
                    "calendar_key"
                ]
            )

            if portfolio_tags is not None:
                standard_kwargs["tags"] = portfolio_tags
                # front end details
            standard_kwargs["target_portfolio_about"] = {
                "description": ts.get_portfolio_about_text(),
                "signal_name": ts.backtesting_weights_config.signal_weights_name,
                "signal_description": ts.signal_weights.get_explanation(),
                "rebalance_strategy_name": ts.backtesting_weights_config.rebalance_strategy_name,
            }

            standard_kwargs["backtest_table_price_column_name"] = "close"

            target_portfolio = Portfolio.get_or_none(data_node_update__id=ts.data_node_update.id)
            if target_portfolio is None:
                target_portfolio, index_asset = Portfolio.create_from_time_series(**standard_kwargs)
            else:
                # patch timeserie of portfolio to guaranteed recreation
                target_portfolio.patch(**standard_kwargs)
                self.logger.debug(
                    f"Target portfolio {target_portfolio.portfolio_ticker} for local time serie {ts.data_node_update.update_hash} already exists in Backend"
                )
                index_asset = PortfolioIndexAsset.get(reference_portfolio__id=target_portfolio.id)

            return target_portfolio, index_asset

        target_portfolio, index_asset = build_markets_portfolio(
            portfolio_ts, portfolio_tags=portfolio_tags
        )

        self.index_asset = index_asset
        self.target_portfolio = target_portfolio
        return target_portfolio, index_asset

    def run(
        self,
        patch_build_configuration=True,
        debug_mode=True,
        force_update=True,
        update_tree=True,
        portfolio_tags: list[str] = None,
        add_portfolio_to_markets_backend=False,
        *args,
        **kwargs,
    ) -> pd.DataFrame:

        if not self._is_initialized or patch_build_configuration == True:
            self._initialize_nodes(patch_build_configuration=patch_build_configuration)

        self.portfolio_strategy_data_node.run(
            debug_mode=debug_mode, update_tree=update_tree, force_update=force_update, **kwargs
        )
        if add_portfolio_to_markets_backend:
            self.build_target_portfolio_in_backend(portfolio_tags=portfolio_tags)

        res = self.portfolio_strategy_data_node.get_df_between_dates()
        if len(res) > 0:
            res = res.sort_values("time_index")
        return res

    @classmethod
    @property
    def configuration_folder_path(self):
        vfb_project_path = os.getenv("VFB_PROJECT_PATH")
        if not vfb_project_path:
            raise ValueError(
                "VFB_PROJECT_PATH environment variable is not set. "
                "Please set it before using 'configuration_path'."
            )
        return os.path.join(vfb_project_path, "configurations")

    @staticmethod
    def check_valid_configuration_name(s: str) -> bool:
        if not bool(re.match(r"^[A-Za-z0-9_]+$", s)):
            raise ValueError(f"Name {s} not valid")

    def store_configuration(self, configuration_name: str | None = None):
        """
        Stores the current configuration as a YAML file under the configuration_name
        """
        if configuration_name and not self.configuration_name:
            self.configuration_name = configuration_name

        if not self.configuration_name:
            raise ValueError(
                "No configuration name was set. Provide a `configuration_name` "
                "argument or load/set one before storing."
            )

        config_file = os.path.join(
            self.configuration_folder_path, f"{self.configuration_name}.yaml"
        )

        write_yaml(dict_file=self.portfolio_config_template, path=config_file)
        self.logger.info(f"Configuration stored under {config_file}")
        return config_file

    @classmethod
    def load_configuration(cls, configuration_name) -> PortfolioConfiguration:
        config_file = os.path.join(cls.configuration_folder_path, f"{configuration_name}.yaml")
        portfolio_config = PortfolioConfiguration.read_portfolio_configuration_from_yaml(
            config_file
        )
        return PortfolioConfiguration(**portfolio_config)

    @classmethod
    def load_from_configuration(cls, configuration_name, config_file: str | None = None):
        if config_file is None:
            config_file = os.path.join(cls.configuration_folder_path, f"{configuration_name}.yaml")
        if not os.path.exists(config_file):
            raise FileNotFoundError(f"Configuration file '{config_file}' does not exist.")

        portfolio_config = PortfolioConfiguration.read_portfolio_configuration_from_yaml(
            config_file
        )
        portfolio = cls(
            portfolio_config_template=portfolio_config, configuration_name=configuration_name
        )
        return portfolio

    @classmethod
    def list_configurations(cls):
        """
        Lists all YAML configuration files found in the configuration_path.
        """
        if not os.path.exists(cls.configuration_folder_path):
            return []

        files = os.listdir(cls.configuration_folder_path)
        yaml_files = [f for f in files if f.endswith(".yaml")]
        # Strip off the '.yaml' extension to return just the base names
        return [os.path.splitext(f)[0] for f in yaml_files]

    def delete_stored_configuration(self):
        """
        Removes a saved configuration file from the configuration folder
        """
        if not self.configuration_name:
            raise ValueError("No configuration name set. Cannot delete an unnamed configuration.")
        config_file = os.path.join(
            self.configuration_folder_path, f"{self.configuration_name}.yaml"
        )
        if not os.path.exists(config_file):
            raise FileNotFoundError(f"Configuration file '{config_file}' does not exist.")
        os.remove(config_file)
        self.logger.info(f"Deleted configuration file '{config_file}'.")
