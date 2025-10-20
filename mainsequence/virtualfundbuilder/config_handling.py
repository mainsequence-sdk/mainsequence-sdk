import logging

from .models import *

logger = get_vfb_logger()


def replace_none_and_empty_dict_with_python_none(config):
    """
    Recursively replace all string 'None' with Python None in the given dictionary
    and log the path where replacements occur.

    Args:
        config (dict): The configuration dictionary.

    Returns:
        dict: Updated dictionary with 'None' replaced by Python None.
    """
    logging.basicConfig(level=logging.INFO, format="%(message)s")

    def recursive_replace(d, path="root"):
        if isinstance(d, dict):
            for key, value in d.items():
                current_path = f"{path}.{key}"
                if isinstance(value, dict) and not value:
                    d[key] = None
                    logger.info(f"Replaced empty dict {{}} with Python None at: {current_path}")
                elif isinstance(value, dict):
                    recursive_replace(value, current_path)
                elif isinstance(value, list):
                    for i, item in enumerate(value):
                        recursive_replace(item, f"{current_path}[{i}]")
                elif isinstance(value, str) and value.lower() in ["none", "null"]:
                    d[key] = None
                    logger.info(f"Replaced 'None' in configuration with None at {current_path}")

        elif isinstance(d, list):
            for i, item in enumerate(d):
                recursive_replace(item, f"{path}[{i}]")

    recursive_replace(config)
    return config


def configuration_sanitizer(configuration: dict) -> PortfolioConfiguration:
    """
    Verifies that a configuration has all the required attributes.
    Args:
        configuration (dict): The configuration dictionary to sanitize.
    Returns:
        PortfolioConfiguration: The sanitized portfolio configuration.
    """
    configuration = copy.deepcopy(configuration)
    configuration = replace_none_and_empty_dict_with_python_none(configuration)
    portfolio_build_config = configuration["portfolio_build_configuration"]
    for key in [
        "assets_configuration",
        "backtesting_weights_configuration",
        "execution_configuration",
    ]:
        if key not in portfolio_build_config:
            raise KeyError(f"Missing required key {key}")

    if portfolio_build_config["assets_configuration"] is not None:
        if "prices_configuration" not in portfolio_build_config["assets_configuration"]:
            raise Exception(
                "Missing prices configuration in portfolio_build_config['assets_configuration']"
            )

    if (
        "rebalance_strategy_configuration"
        not in portfolio_build_config["backtesting_weights_configuration"]
    ):
        raise Exception(
            "Missing 'rebalance_strategy_configuration' in 'backtesting_weights_configuration'"
        )

    if (
        "calendar_key"
        not in portfolio_build_config["backtesting_weights_configuration"][
            "rebalance_strategy_configuration"
        ]
        or not portfolio_build_config["backtesting_weights_configuration"][
            "rebalance_strategy_configuration"
        ]["calendar_key"]
    ):
        raise Exception("Missing 'calendar_key' in 'rebalance_strategy_configuration'")

    if (
        "signal_weights_configuration"
        not in portfolio_build_config["backtesting_weights_configuration"]
    ):
        raise Exception(
            "Missing 'signal_weights_configuration' in 'backtesting_weights_configuration'"
        )

    if (
        "signal_assets_configuration"
        not in portfolio_build_config["backtesting_weights_configuration"][
            "signal_weights_configuration"
        ]
    ):
        raise Exception(
            "Missing 'signal_weights_configuration' in 'backtesting_weights_configuration'"
        )

    if "portfolio_prices_frequency" not in portfolio_build_config:
        raise Exception("Missing 'portfolio_prices_frequency' in 'portfolio_build_config'")

    return PortfolioConfiguration.parse_portfolio_configurations(
        portfolio_build_configuration=portfolio_build_config,
        portfolio_markets_configuration=configuration["portfolio_markets_configuration"],
    )
