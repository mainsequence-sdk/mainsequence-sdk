import inspect
import json
from datetime import datetime
from enum import Enum
from typing import Any, Union, get_args, get_origin, get_type_hints

import docstring_parser
import numpy as np
import pandas as pd
from joblib import Parallel, delayed
from numpy.linalg import LinAlgError
from numpy.typing import NDArray
from pydantic import BaseModel
from tqdm import tqdm

from mainsequence.client import Asset
from mainsequence.logconf import logger


def get_vfb_logger():
    global logger

    # If the logger doesn't have any handlers, create it using the custom function
    logger.bind(sub_application="virtualfundbuilder")
    return logger


logger = get_vfb_logger()

# Symbol mapping for CoinGecko API
GECKO_SYMBOL_MAPPING = {
    "ADA": "cardano",
    "AVAX": "avalanche-2",
    "BCH": "bitcoin-cash",
    "DOT": "polkadot",
    "LINK": "chainlink",
    "LTC": "litecoin",
    "MATIC": "matic-network",
    "SOL": "solana",
    "ATOM": "cosmos",
    "BTC": "bitcoin",
    "ETH": "ethereum",
}

# Small time delta for precision operations
TIMEDELTA = pd.Timedelta("5ms")


def runs_in_main_process() -> bool:
    import multiprocessing

    return multiprocessing.current_process().name == "MainProcess"


def reindex_df(
    df: pd.DataFrame, start_time: datetime, end_time: datetime, freq: str
) -> pd.DataFrame:
    """
    Aligns two DataFrames on a new index based on a specified frequency, filling missing entries with the last known values.

    Args:
        df (pd.DataFrame): Reference DataFrame used to determine the new index range.
        start_time (datetime): start of index
        end_time (datetime): end of index
        freq (str): Frequency string (e.g., '1T' for one minute) to define the interval of the new index.

    Returns:
        pd.DataFrame: The df_to_align DataFrame reindexed to match the new timeline and filled with forward filled values.
    """
    new_index = pd.date_range(start=start_time, end=end_time, freq=freq)
    return df.reindex(new_index).ffill()


def convert_to_binance_frequency(freq: str) -> str:
    """
    Converts a generic frequency format to a format compatible with Binance API requirements.

    Args:
        freq (str): The generic frequency format (e.g., '1m', '1h').

    Returns:
        str: A frequency string adapted for Binance API (e.g., '1m', '1h').
    """
    frequency_mappings = {"min": "m", "h": "h", "d": "d", "w": "w"}  # TODO extend
    for unit, binance_unit in frequency_mappings.items():
        if freq.endswith(unit):
            return freq[: -len(unit)] + binance_unit
    raise NotImplementedError(f"Frequency of {freq} not supported")


def get_last_query_times_per_asset(
    latest_value: datetime,
    data_node_storage: dict,
    asset_list: list[Asset],
    max_lookback_time: datetime,
    current_time: datetime,
    query_frequency: str,
) -> dict[str, float | None]:
    """
    Determines the last query times for each asset based on metadata, a specified lookback limit, and a query frequency.

    Args:
        latest_value (datetime|None): Timestamp of the last value in the database for each asset.
        metadata (dict): Metadata containing previous query information for each coin.
        asset_list (List[Asset]): List of asset objects to process.
        max_lookback_time (datetime): Maximum historical lookback time allowed for the node.
        current_time (datetime): Current time to consider for the calculations.
        query_frequency (str): Query frequency as a pandas-parseable string to determine if new data needs fetching.

    Returns:
        Dict[str, Optional[float]]: A dictionary mapping asset IDs to their respective last query times expressed in UNIX timestamp.
    """
    if latest_value:
        last_query_times_per_asset = data_node_storage["sourcetableconfiguration"][
            "multi_index_stats"
        ]["max_per_asset_symbol"]
    else:
        last_query_times_per_asset = {}

    for asset in asset_list:
        asset_id = asset.unique_identifier
        if asset_id in last_query_times_per_asset:
            asset_start_time = pd.to_datetime(last_query_times_per_asset[asset_id])
        else:
            asset_start_time = max_lookback_time

        if asset_start_time >= (current_time - pd.Timedelta(query_frequency)):
            logger.info(
                f"no new data for asset {asset.name} from {asset_start_time} to {current_time}"
            )
            last_query_times_per_asset[asset_id] = None
        else:
            last_query_times_per_asset[asset_id] = (asset_start_time + TIMEDELTA).timestamp()

    return last_query_times_per_asset


def do_single_regression(
    xx: NDArray,
    XTX_inv_list: list,
    rolling_window: int,
    col_name: str,
    tmp_y: NDArray,
    XTX_inv_diag: list,
) -> pd.DataFrame:
    """
    Performs a single regression analysis on a sliding window of data points for a specific column.

    Args:
        xx (NDArray): An array of independent variable data with a sliding window applied.
        XTX_inv_list (list): A list of precomputed inverse matrices of X.T @ X for each window.
        rolling_window (int): The number of observations per window.
        col_name (str): The name of the column being analyzed, used for labeling the output.
        tmp_y (NDArray): The dependent variable data.
        XTX_inv_diag (list): Diagonals of the precomputed inverse matrices, used for standard error calculation.

    Returns:
        pd.DataFrame: A DataFrame containing the regression results with coefficients, R-squared, and t-statistics.
    """
    mean_y = tmp_y.mean(axis=1).reshape(-1, 1)
    SST = np.sum((tmp_y - mean_y) ** 2, axis=1)
    precompute_y_ = {"SST": SST, "mean_y": mean_y}

    results = []
    for i in tqdm(range(xx.shape[0]), desc=f"building regression {col_name}"):
        xxx = xx[i].reshape(rolling_window, xx[i].shape[-1])
        tmpy_ = tmp_y[i]
        x_mult = XTX_inv_list[i] @ (xxx.T)
        coefs = x_mult @ tmpy_.T
        y_estimates = (xxx @ coefs.reshape(-1, 1)).ravel()
        residuals = tmpy_ - y_estimates
        SSR = np.sum((y_estimates - precompute_y_["mean_y"][i]) ** 2)
        rsquared = SSR / precompute_y_["SST"][i]
        residuals_var = np.sum(residuals**2) / (rolling_window - coefs.shape[0] + 1)
        standard_errors = np.sqrt(XTX_inv_diag[i] * residuals_var)
        ts = coefs / standard_errors
        results.append(
            dict(
                beta=coefs[0],
                intercept=coefs[1],
                rsquared=rsquared,
                t_intercept=ts[1],
                t_beta=ts[0],
            )
        )
    results = pd.concat([pd.DataFrame(results)], keys=[col_name], axis=1)

    return results


def build_rolling_regression_from_df(
    x: NDArray, y: NDArray, rolling_window: int, column_names: list, threads: int = 5
) -> pd.DataFrame:
    """
    Builds rolling regressions for multiple variables in parallel using a specified rolling window.

    Args:
        x (NDArray): An array of independent variables.
        y (NDArray): An array of dependent variables.
        rolling_window (int): The size of the rolling window for each regression.
        column_names (list): Names of the dependent variables, used for labeling the output.
        threads (int): Number of threads to use for parallel processing.

    Returns:
        pd.DataFrame: A DataFrame containing the regression results for all variables.
    """
    XX = np.concatenate([x.reshape(-1, 1), np.ones((x.shape[0], 1))], axis=1)
    xx = np.lib.stride_tricks.sliding_window_view(XX, (rolling_window, XX.shape[1]))

    XTX_inv_list, XTX_inv_diag = (
        [],
        [],
    )  # pre multiplication of x before y and diagonal for standard errros

    # precompute for x
    for i in tqdm(range(xx.shape[0]), desc="building x precomputes"):
        xxx = xx[i].reshape(rolling_window, xx[i].shape[-1])
        try:
            XTX_inv = np.linalg.inv(xxx.T @ xxx)
            XTX_inv_list.append(XTX_inv)
            XTX_inv_diag.append(np.diag(XTX_inv))
        except LinAlgError:
            XTX_inv_list.append(XTX_inv_list[-1] * np.nan)
            XTX_inv_diag.append(XTX_inv_diag[-1] * np.nan)

    y_views = {
        i: np.lib.stride_tricks.sliding_window_view(y[:, i], (rolling_window,))
        for i in range(y.shape[1])
    }

    work_details = dict(n_jobs=threads, prefer="threads")
    reg_results = Parallel(**work_details)(
        delayed(do_single_regression)(
            xx=xx,
            tmp_y=tmp_y,
            XTX_inv_list=XTX_inv_list,
            rolling_window=rolling_window,
            XTX_inv_diag=XTX_inv_diag,
            col_name=column_names[y_col],
        )
        for y_col, tmp_y in y_views.items()
    )

    reg_results = pd.concat(reg_results, axis=1)
    reg_results.columns = reg_results.columns.swaplevel()
    return reg_results


def parse_google_docstring(docstring):
    parsed = docstring_parser.parse(docstring)
    return {
        "description": parsed.description,
        "args_descriptions": {param.arg_name: param.description for param in parsed.params},
        "returns": parsed.returns.description if parsed.returns else None,
        "example": "\n".join([param.description for param in parsed.examples]),
        "raises": {exc.type_name: exc.description for exc in parsed.raises},
    }


def extract_code(output_string):
    import re

    # Use regex to find content between triple backticks
    match = re.search(r"```[^\n]*\n(.*?)```", output_string, re.DOTALL)
    if match:
        code = match.group(1)
        return code
    else:
        return ""


def _convert_unknown_to_string(obj):
    """Converts unsupported/unknown types to strings."""
    try:
        return str(obj)
    except Exception:
        raise TypeError(f"Object of type {type(obj).__name__} is not JSON serializable")


def is_jupyter_environment():
    try:
        from IPython import get_ipython

        return "ipykernel" in str(get_ipython())
    except ImportError:
        return False


def type_to_json_schema(py_type: type, definitions: dict[str, Any]) -> dict[str, Any]:
    """
    Recursively converts a Python type annotation to a JSON schema dictionary.
    Handles Pydantic models, Enums, Lists, Unions, and basic types.

    Args:
        py_type: The Python type to convert.
        definitions: A dict to store schemas of nested models, used for $defs.

    Returns:
        A dictionary representing the JSON schema for the given type.
    """
    origin = get_origin(py_type)
    args = get_args(py_type)

    # Handle Optional[T] by making the inner type nullable
    if origin is Union and len(args) == 2 and type(None) in args:
        non_none_type = args[0] if args[1] is type(None) else args[1]
        schema = type_to_json_schema(non_none_type, definitions)
        # Add null type to anyOf or create a new anyOf
        if "anyOf" in schema:
            if not any(sub.get("type") == "null" for sub in schema["anyOf"]):
                schema["anyOf"].append({"type": "null"})
        else:
            schema = {"anyOf": [schema, {"type": "null"}]}
        return schema

    if origin is Union:
        return {"anyOf": [type_to_json_schema(arg, definitions) for arg in args]}
    if origin in (list, list):
        item_schema = type_to_json_schema(args[0], definitions) if args else {}
        return {"type": "array", "items": item_schema}
    if origin in (dict, dict):
        value_schema = type_to_json_schema(args[1], definitions) if len(args) > 1 else {}
        return {"type": "object", "additionalProperties": value_schema}

    # Handle Pydantic Models by creating a reference
    if inspect.isclass(py_type) and issubclass(py_type, BaseModel):
        model_name = py_type.__name__
        if model_name not in definitions:
            definitions[model_name] = {}  # Placeholder to break recursion
            model_schema = py_type.model_json_schema(ref_template="#/$defs/{model}")
            if "$defs" in model_schema:
                for def_name, def_schema in model_schema.pop("$defs").items():
                    if def_name not in definitions:
                        definitions[def_name] = def_schema
            definitions[model_name] = model_schema
        return {"$ref": f"#/$defs/{model_name}"}

    # Handle Enums
    if inspect.isclass(py_type) and issubclass(py_type, Enum):
        return {"type": "string", "enum": [e.value for e in py_type]}

    # Handle basic types
    type_map = {str: "string", int: "integer", float: "number", bool: "boolean"}
    if py_type in type_map:
        return {"type": type_map[py_type]}
    if py_type is Any:
        return {}  # Any type, no constraint

    # Fallback for unknown types
    return {
        "type": "string",
        "description": f"Unrecognized type: {getattr(py_type, '__name__', str(py_type))}",
    }


def create_schema_from_signature(func: callable) -> dict[str, Any]:
    """
    Parses a function's signature (like __init__) and creates a JSON schema.

    Args:
        func: The function or method to parse.

    Returns:
        A dictionary representing the JSON schema of the function's signature.
    """
    try:
        signature = inspect.signature(func)
        type_hints = get_type_hints(func)
    except (TypeError, NameError):  # Handles cases where hints can't be resolved
        return {}

    parsed_doc = docstring_parser.parse(func.__doc__ or "")
    arg_descriptions = {p.arg_name: p.description for p in parsed_doc.params}

    properties = {}
    required = []
    definitions = {}  # For nested models

    for name, param in signature.parameters.items():
        if name in ("self", "cls", "args", "kwargs"):
            continue

        param_type = type_hints.get(name, Any)
        prop_schema = type_to_json_schema(param_type, definitions)
        prop_schema["title"] = name.replace("_", " ").title()

        if name in arg_descriptions:
            prop_schema["description"] = arg_descriptions[name]

        if param.default is inspect.Parameter.empty:
            required.append(name)
        else:
            default_value = param.default
            try:
                # Ensure default is JSON serializable
                json.dumps(default_value)
                prop_schema["default"] = default_value
            except TypeError:
                if isinstance(default_value, Enum):
                    prop_schema["default"] = default_value.value
                else:
                    # Fallback for non-serializable defaults
                    prop_schema["default"] = str(default_value)

        properties[name] = prop_schema

    schema = {
        "title": getattr(func, "__name__", "Schema"),
        "type": "object",
        "properties": properties,
    }
    if required:
        schema["required"] = required
    if definitions:
        schema["$defs"] = definitions

    return schema
