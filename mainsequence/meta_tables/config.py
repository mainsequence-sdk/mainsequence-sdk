import os
from enum import Enum
from pathlib import Path

from .utils import read_key_from_yaml, read_yaml, write_yaml

DEFAULT_RETENTION_POLICY = dict(scheduler_name="default", retention_policy_time="90 days")

TIME_SERIES_SOURCE_TIMESCALE = "timescale"
TIME_SERIES_SOURCE_PARQUET = "parquet"

META_TABLES_PATH = os.environ.get("META_TABLES_ROOT_PATH", f"{str(Path.home())}/meta_tables")
META_TABLES_CONFIG_PATH = os.environ.get(
    "META_TABLES_CONFIG_PATH",
    f"{META_TABLES_PATH}/config.yml",
)

META_TABLES_DATA_PATH = f"{META_TABLES_PATH}/data"
META_TABLES_TEMP_PATH = f"{META_TABLES_PATH}/temp"
META_TABLES_RAY_FOLDER = f"{META_TABLES_PATH}/ray"

TIME_SERIES_FOLDER = f"{META_TABLES_DATA_PATH}/time_series_data"
os.makedirs(TIME_SERIES_FOLDER, exist_ok=True)
Path(META_TABLES_TEMP_PATH).mkdir(parents=True, exist_ok=True)
Path(META_TABLES_RAY_FOLDER).mkdir(parents=True, exist_ok=True)

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
        "MAINSEQUENCE_ENDPOINT",
    ]

    def __init__(self):
        self.set_gt_configuration()
        self._assert_env_variables()

    @classmethod
    def add_env_variables_to_registry(cls, env_vars: list):
        cls.OBLIGATORY_ENV_VARIABLES.extend(env_vars)

    def set_gt_configuration(self):
        if not os.path.isfile(META_TABLES_CONFIG_PATH):
            self._build_template_yaml()

        self.configuration = read_yaml(META_TABLES_CONFIG_PATH)

    def _assert_env_variables(self):
        do_not_check = os.environ.get("DO_NOT_CHECK_META_TABLES", "false").lower() == "true"
        if do_not_check:
            return None
        for ob_var in self.OBLIGATORY_ENV_VARIABLES:
            assert ob_var in os.environ, f"{ob_var} not in environment variables"
        assert os.environ.get("MAINSEQUENCE_ACCESS_TOKEN") or os.environ.get(
            "MAINSEQUENCE_REFRESH_TOKEN"
        ), (
            "Authentication env is missing. Set MAINSEQUENCE_ACCESS_TOKEN / "
            "MAINSEQUENCE_REFRESH_TOKEN."
        )

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
        write_yaml(path=META_TABLES_CONFIG_PATH, dict_file=config)


configuration = Configuration()


class TimeSeriesOGM:
    def __init__(self):
        os.makedirs(self.time_series_config["LOCAL_DATA_PATH"], exist_ok=True)

    @property
    def time_series_config(self):
        ts_config = read_key_from_yaml("time_series_config", path=META_TABLES_CONFIG_PATH)
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


ogm = TimeSeriesOGM()
