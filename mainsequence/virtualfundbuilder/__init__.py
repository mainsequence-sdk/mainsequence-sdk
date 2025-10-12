__version__ = '0.1.0'

from pathlib import Path
import os
import sys

from mainsequence.virtualfundbuilder.__main__ import get_pod_configuration

def load_env():

    assert os.environ.get("VFB_PROJECT_PATH", None) is not None, "VFB_PROJECT_PATH environment variable not set"

    from mainsequence.tdag.config import Configuration
    # this step is needed to assure env variables are passed to ray cluster
    Configuration.add_env_variables_to_registry(["VFB_PROJECT_PATH"])

    sys.path.append(str(Path(os.environ.get("VFB_PROJECT_PATH")).parent))

load_env()
from mainsequence.virtualfundbuilder.utils import (
    GECKO_SYMBOL_MAPPING,
    TIMEDELTA,
    reindex_df,
    convert_to_binance_frequency,
    get_last_query_times_per_asset,
    build_rolling_regression_from_df,
    runs_in_main_process
)

def register_default_strategies():
    # Keep this in a function to not clutter the libs namespace
    import mainsequence.virtualfundbuilder.contrib.apps
    import mainsequence.virtualfundbuilder.contrib.data_nodes
    import mainsequence.virtualfundbuilder.contrib.rebalance_strategies

if os.getenv("PROJECT_LIBRARY_NAME") is None:
    # TODO workaround for now to make local execution work
    os.environ["PROJECT_LIBRARY_NAME"] = Path(os.environ.get("VFB_PROJECT_PATH")).name

RUNS_IN_JOB = os.getenv("JOB_ID", None)
if RUNS_IN_JOB:
    # register_default_strategies() #
    pass

if runs_in_main_process():
    get_pod_configuration()