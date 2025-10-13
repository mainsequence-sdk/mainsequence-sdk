import os
from enum import Enum
from pathlib import Path

from .utils import read_key_from_yaml, read_yaml, write_yaml

DEFAULT_RETENTION_POLICY = dict(scheduler_name="default", retention_policy_time="90 days")

API_TS_PICKLE_PREFIFX = "api-"

TIME_SERIES_SOURCE_TIMESCALE = "timescale"
TIME_SERIES_SOURCE_PARQUET = "parquet"

TDAG_PATH = os.environ.get("TDAG_ROOT_PATH", f"{str(Path.home())}/tdag")
TDAG_CONFIG_PATH = os.environ.get("TDAG_CONFIG_PATH", f"{TDAG_PATH}/config.yml")

TDAG_DATA_PATH = f"{TDAG_PATH}/data"
GT_TEMP_PATH = f"{TDAG_PATH}/temp"
GT_RAY_FOLDER = f"{TDAG_PATH}/ray"

TIME_SERIES_FOLDER = f"{TDAG_DATA_PATH}/time_series_data"
os.makedirs(TIME_SERIES_FOLDER, exist_ok=True)
Path(GT_TEMP_PATH).mkdir(parents=True, exist_ok=True)
Path(GT_RAY_FOLDER).mkdir(parents=True, exist_ok=True)

dir_path = os.path.dirname(os.path.realpath(__file__))


class bcolors:
    HEADER = "\033[95m"
    OKBLUE = "\033[94m"
    OKCYAN = "\033[96m"
    OKGREEN = "\033[92m"
    IMPORTANT = "\033[45m"
    WARNING = "\033[93m"
    FAIL = "\033[91m"
    ENDC = "\033[0m"
    BOLD = "\033[1m"
    UNDERLINE = "\033[4m"


class RunningMode(Enum):
    TRAINING = "train"
    LIVE = "live"


class Configuration:
    OBLIGATORY_ENV_VARIABLES = [
        "TDAG_ENDPOINT",
        "MAINSEQUENCE_TOKEN",
    ]

    def __init__(self):
        self.set_gt_configuration()
        self._assert_env_variables()

    @classmethod
    def add_env_variables_to_registry(cls, env_vars: list):
        cls.OBLIGATORY_ENV_VARIABLES.extend(env_vars)

    def set_gt_configuration(self):
        if not os.path.isfile(TDAG_CONFIG_PATH):
            self._build_template_yaml()

        self.configuration = read_yaml(TDAG_CONFIG_PATH)

    def _assert_env_variables(self):
        do_not_check = os.environ.get("DO_NOT_CHECK_TDAG", "false").lower() == "true"
        if do_not_check == True:
            return None
        for ob_var in self.OBLIGATORY_ENV_VARIABLES:
            assert ob_var in os.environ, f"{ob_var} not in environment variables"

    def _build_template_yaml(self):
        config = {
            "time_series_config": {
                "ignore_update_timeout": False,
            },
            "instrumentation_config": {
                "grafana_agent_host": "localhost",
                "export_trace_to_console": False,
            },
        }
        write_yaml(path=TDAG_CONFIG_PATH, dict_file=config)


configuration = Configuration()


class TimeSeriesOGM:
    def __init__(self):
        os.makedirs(self.time_series_config["LOCAL_DATA_PATH"], exist_ok=True)

    @property
    def time_series_config(self):
        ts_config = read_key_from_yaml("time_series_config", path=TDAG_CONFIG_PATH)
        ts_config["LOCAL_DATA_PATH"] = TIME_SERIES_FOLDER
        return ts_config

    def verify_exist(self, target_path):
        os.makedirs(target_path, exist_ok=True)

    @property
    def time_series_folder(self):
        target_path = self.time_series_config["LOCAL_DATA_PATH"]
        self.verify_exist(target_path=target_path)
        return target_path

    @property
    def temp_folder(self):
        target_path = os.path.join(f"{self.time_series_folder}", "temp")
        self.verify_exist(target_path=target_path)
        return target_path

    @property
    def data_node_update_path(self):
        target_path = os.path.join(f"{self.time_series_folder}", "data_node_update")
        self.verify_exist(target_path=target_path)
        return target_path

    @property
    def pickle_storage_path(self):
        target_path = os.path.join(f"{self.time_series_folder}", "pickled_ts")
        self.verify_exist(target_path=target_path)
        return target_path

    def get_ts_pickle_path(self, update_hash: str):
        return os.path.join(f"{self.pickle_storage_path}", f"{update_hash}.pickle")


ogm = TimeSeriesOGM()
