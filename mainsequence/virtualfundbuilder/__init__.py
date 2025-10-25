__version__ = "0.1.0"

import os
import sys
from pathlib import Path


from .utils import get_vfb_logger

logger = get_vfb_logger()
def load_env():

    assert (
        os.environ.get("VFB_PROJECT_PATH", None) is not None
    ), "VFB_PROJECT_PATH environment variable not set"

    from mainsequence.tdag.config import Configuration

    # this step is needed to assure env variables are passed to ray cluster
    Configuration.add_env_variables_to_registry(["VFB_PROJECT_PATH"])

    sys.path.append(str(Path(os.environ.get("VFB_PROJECT_PATH")).parent))


load_env()
from mainsequence.virtualfundbuilder.utils import (
    GECKO_SYMBOL_MAPPING,
    TIMEDELTA,
    build_rolling_regression_from_df,
    convert_to_binance_frequency,
    get_last_query_times_per_asset,
    reindex_df,
    runs_in_main_process,
)


def register_default_strategies():
    # Keep this in a function to not clutter the libs namespace
    import mainsequence.virtualfundbuilder.contrib.apps
    import mainsequence.virtualfundbuilder.contrib.data_nodes
    import mainsequence.virtualfundbuilder.contrib.rebalance_strategies




RUNS_IN_JOB = os.getenv("JOB_ID", None)
if RUNS_IN_JOB:
    # register_default_strategies() #
    pass

if runs_in_main_process():
   pass