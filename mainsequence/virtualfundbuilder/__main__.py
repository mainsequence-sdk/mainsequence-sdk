import time

import fire

import json

import runpy
import tempfile
import os
from pathlib import Path

import requests
import yaml
from requests.structures import CaseInsensitiveDict


def get_tdag_headers():
    headers = CaseInsensitiveDict()
    headers["Content-Type"] = "application/json"
    headers["Authorization"] = "Token " + os.getenv("MAINSEQUENCE_TOKEN")
    return headers

def update_job_status(status_message):
    url = f"{os.getenv('TDAG_ENDPOINT')}/orm/api/pods/job/job_run_status/"

    payload = {
        "status": status_message,
        "git_hash": os.getenv("GIT_HASH"),
        "command_id": os.getenv("COMMAND_ID")
    }

    response = requests.post(url, json=payload, headers=get_tdag_headers())

    if response.status_code == 200:
        data = response.json()
        print("Update success:", data)
        return data
    else:
        print("Error updating pod:", response.status_code, response.text)
        return None

def run_configuration(configuration_name):
    from mainsequence.virtualfundbuilder.portfolio_interface import PortfolioInterface
    from mainsequence.virtualfundbuilder.utils import get_vfb_logger
    logger = get_vfb_logger()
    logger.info(f"Run Timeseries Configuration {configuration_name}")
    portfolio = PortfolioInterface.load_from_configuration(configuration_name)
    res = portfolio.run()
    print(res.head())


def run_app(app_name, configuration):
    from mainsequence.virtualfundbuilder.resource_factory.app_factory import APP_REGISTRY
    from mainsequence.virtualfundbuilder.utils import get_vfb_logger
    from mainsequence.virtualfundbuilder.config_handling import replace_none_and_empty_dict_with_python_none

    logger = get_vfb_logger()
    logger.info(f"Start App {app_name} with configuration\n{configuration}")
    try:
        app_cls = APP_REGISTRY[app_name]

        configuration_json = yaml.load(configuration, Loader=yaml.UnsafeLoader)
        configuration_json = replace_none_and_empty_dict_with_python_none(configuration_json)

        # Pull out the dict under "configuration" (or flatten it, your choice)
        actual_config = configuration_json.get("configuration", {})

        # Now pass the unpacked dictionary:
        pydantic_config = app_cls.configuration_class(**actual_config)

        app_instance = app_cls(pydantic_config)
        results = app_instance.run()
    except Exception as e:
        logger.error("Error running app", exc_info=True)
        raise
    logger.info(f"Finished App {app_name} run with results: {results}")

def run_notebook(execution_object):
    from mainsequence.virtualfundbuilder.notebook_handling import convert_notebook_to_python_file
    from mainsequence.virtualfundbuilder.utils import get_vfb_logger
    logger = get_vfb_logger()
    logger.info(f"Run Notebook {execution_object}")
    notebook_file_path = Path(os.getenv("VFB_PROJECT_PATH")).parent / execution_object
    python_notebook_file = convert_notebook_to_python_file(notebook_file_path)
    runpy.run_path(python_notebook_file, run_name="__main__")

def run_script(execution_object):
    from mainsequence.virtualfundbuilder.utils import get_vfb_logger
    logger = get_vfb_logger()
    logger.info(f"Run script {execution_object}")
    python_file_path = Path(os.getenv("VFB_PROJECT_PATH")).parent / execution_object
    runpy.run_path(python_file_path, run_name="__main__")

def get_py_modules(folder_path):
    if not os.path.isdir(folder_path): return []
    files = os.listdir(folder_path)
    files = [f for f in files if f[0] not in ["_", "."] and f.endswith(".py")]
    return [f.split(".")[0] for f in files]

def get_pod_configuration():
    print("Get pod configuration")
    #todo register nodes by running the tree




    project_path = os.getenv("VFB_PROJECT_PATH")
    repo_path = os.path.dirname(project_path)

    # Gather all submodules in data_nodes
    # data_nodes_package = f"{project_library}.data_nodes"
    # data_nodes_modules = get_py_modules(os.path.join(project_path, "data_nodes"))

    # Gather all submodules in rebalance_strategies
    # rebalance_package = f"{project_library}.rebalance_strategies"
    # rebalance_modules = get_py_modules(os.path.join(project_path, "rebalance_strategies"))

    # Gather all submodules in apps
    agent_tools_package = "agent_tools"
    agent_tools_modules =get_py_modules(os.path.join(repo_path, agent_tools_package))
    # Build the temporary Python script to import all files
    script_lines = []

    # script_lines.append("# -- Auto-generated imports for data_nodes --")
    # for mod in data_nodes_modules:
    #     script_lines.append(f"import {data_nodes_package}.{mod}")

    # script_lines.append("# -- Auto-generated imports for rebalance_strategies --")
    # for mod in rebalance_modules:
    #     script_lines.append(f"import {rebalance_package}.{mod}")

    script_lines.append("# -- Auto-generated imports for agent tools --")
    for mod in agent_tools_modules:
        script_lines.append(f"import {agent_tools_package}.{mod}")


    #debugin stuff remove
    print(f"Project Path: {project_path}")
    print(f"Repo Path: {repo_path}")
    print(f"Contents of repo_path ({repo_path}):")
    for item in os.listdir(repo_path):
        print(item)

    script_lines.append("")
    script_lines.append("from mainsequence.virtualfundbuilder.agent_interface import TDAGAgent")
    script_lines.append("print('Initialize TDAGAgent')")
    script_lines.append("tdag_agent = TDAGAgent()")

    TMP_SCRIPT = "\n".join(script_lines)
    print(f"Executing Script: \n{TMP_SCRIPT}")

    # Write out to a temporary .py file and run
    temp_dir = tempfile.mkdtemp()
    python_file_path = Path(temp_dir) / "load_pod_configuration.py"
    with open(python_file_path, "w", encoding="utf-8") as f:
        f.write(TMP_SCRIPT)
    try:
        runpy.run_path(str(python_file_path), run_name="__main__")
    except Exception:
        from mainsequence.virtualfundbuilder.utils import get_vfb_logger
        logger = get_vfb_logger()
        logger.exception("--- ERROR: Failed to import one or more project modules. ---")
        raise

def import_project_configuration():
    from mainsequence.client import ProjectConfiguration, Job
    from mainsequence.virtualfundbuilder.utils import get_vfb_logger
    logger = get_vfb_logger()

    # load project configuration if exists
    config_file = Path(os.getenv("VFB_PROJECT_PATH")).parent / "project_configuration.yaml"
    logger.info(f"Try to import project configuration from {config_file}")

    if not config_file.exists():
        logger.info("No project configuration found")
        return

    with open(config_file, "r") as f:
        project_configuration_raw = yaml.safe_load(f)
    project_configuration = ProjectConfiguration(**project_configuration_raw)

    logger.info(f"Create or update {project_configuration.jobs} jobs in backend")
    for job in project_configuration.jobs:
        Job.create_from_configuration(job_configuration=job.model_dump())


def prerun_routines():
    data = update_job_status("RUNNING")
    env_update = data.get("environment_update", {})
    for key, val in env_update.items():
        os.environ[key] = val

def postrun_routines(error_on_run: bool):
    if error_on_run:
        update_job_status("FAILED")
    else:
        update_job_status("SUCCEEDED")

class VirtualFundLauncher:

    def __init__(self):
        from mainsequence.virtualfundbuilder.utils import get_vfb_logger
        self.logger = get_vfb_logger()

    def run_resource(self, execution_type, execution_object=None):
        error_on_run = False

        try:
            prerun_routines()
            if execution_type == "configuration":
                run_configuration(execution_object)
            elif execution_type == "script":
                run_script(execution_object)
            elif execution_type == "notebook":
                run_notebook(execution_object)
            elif execution_type == "system_job":
                self.logger.info("Update project resources and import project configuration")
                import_project_configuration()
                # get_pod_configuration already called from import
                pass
            elif execution_type == "app":
                run_app(app_name=os.getenv("APP_NAME"), configuration=os.getenv("APP_CONFIGURATION"))
            elif execution_type == "standby":
                sleep_seconds = int(os.getenv("STANDBY_DURATION_SECONDS"))
                self.logger.info(f"Sleep for {sleep_seconds} seconds")
                time.sleep(sleep_seconds)
            else:
                raise NotImplementedError(f"Unknown execution type {execution_type}")

        except Exception as e:
            self.logger.exception(f"Exception during job run occured {e}")
            import traceback
            traceback.print_exc()
            error_on_run = True

        finally:
            postrun_routines(error_on_run)


if __name__ == "__main__":
    fire.Fire(VirtualFundLauncher)

