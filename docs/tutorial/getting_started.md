# Getting Started with Main Sequence Part 1

This tutorial walks you through creating a project, setting it up on your Windows machine, and building your first data nodes. The goal is to make each step clear and actionable while preserving all the examples and screenshots from your original guide.

you can see the final repository here: https://github.com/mainsequence-sdk/tutorial-project/blob/main/README.md 

## 1. Create a Project

Log in to Main Sequence. You'll land on the **Projects** page. Projects help you organize work, data, and compute. Let's create the first one: choose **Create New Project** and name it **Tutorial Project**.

![img.png](../img/tutorial/projects_search.png)

![img.png](../img/tutorial/create_new_project.png)

After a few seconds, your new project should appear with a checkmark indicating it's initialized. Click the project to open it.

![img.png](../img/tutorial/project_tutorial_search.png)

On the **Project Details** page you'll see:
- A green status indicator confirming the project was set up correctly.
- The repository and branch (e.g., `tutorial-project/main`) and the latest commit.
- Two **Jobs** representing background processes—no action needed for now.

![img.png](../img/tutorial/project_detail.png)

## 2. Work on the Project Locally

We'll use **Visual Studio Code** for the tutorial. If you don't have it, download it from the official site.
Also make sure you have Python 3.11 or later installed or download it from the official site and follow the installation instructions.

Open **PowerShell** terminal (Windows) or your preferred terminal (macOS/Linux) and enter the next commands.

First, install the Main Sequence Python package in your environment:

```powershell
pip install mainsequence
```

With the package installed, you can use the CLI from your machine:

```powershell
mainsequence --help
# or if your system does not allow automatic additions to the path
python -m mainsequence --help
```

![img.png](../img/tutorial/cli_help.png)

Now log in via the CLI:

```powershell
mainsequence login [USER_NAME]
```

You should see a list of your projects:

```text
Projects:
ID  Project                       Data Source  Class         Status     Local  Path                                                                  
--  -------                       -----------  -----         ------     -----  ----                                                                  
60  TutorialProject                Default DB   timescale_db  AVAILABLE  —      —                                                                     
```

The **Path** column is empty because the project isn't mapped locally yet. Use the project command to see your options:

```powershell
mainsequence project --help
```

Output:
```text
 Usage: mainsequence project [OPTIONS] COMMAND [ARGS]...                                                                                                                                                                                      
                                                                                                                                                                                                                                              
 Project commands                                                                                                                                                                                                                             
                                                                                                                                                                                                                                              
╭─ Options ──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────╮
│ --help          Show this message and exit.                                                                                                                                                                                                │
╰────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────╯
╭─ Commands ─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────╮
│ list                   List projects with Local status and path.                                                                                                                                                                           │
│ open                   Open the local folder in the OS file manager.                                                                                                                                                                       │
│ delete-local           Unlink the mapped folder, optionally delete it.                                                                                                                                                                     │
│ open-signed-terminal   Open a terminal window in the project directory with ssh-agent started and the repo's key added.                                                                                                                    │
│ set-up-locally         Set up project locally.                                                                                                                                                                                             │
╰────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────╯
```

Map the project to your machine and list again to confirm the mapping:

```powershell
mainsequence project set-up-locally [PROJECT_ID]

mainsequence project list
```

Output:

**Windows:**
```text
ID  Project                       Data Source  Class         Status     Local  Path                                                                  
--  -------                       -----------  -----         ------     -----  ----                                                                  
60  Tutorial Project              Default DB   timescale_db  AVAILABLE  Local  C:\Users\YourName\mainsequence\my_organization\projects\tutorial-project   
```

**macOS/Linux:**
```text
ID  Project                       Data Source  Class         Status     Local  Path                                                                  
--  -------                       -----------  -----         ------     -----  ----                                                                  
60  Tutorial Project              Default DB   timescale_db  AVAILABLE  Local  /home/user/mainsequence/my_organization/projects/tutorial-project   
```

Once mapped, you'll see the project under your `mainsequence` folder structure (for example, a `src` directory with a `data_nodes` module, plus typical files like `pyproject.toml`, `README.md`, and `requirements.txt`).

Open your project in VS Code and select your Python environment (the tutorial was written using Python 3.11.9). We'll use **uv** to manage dependencies and dev workflow.

Open PowerShell terminal in VS Code (`` Ctrl+` ``), create a virtual environment, then activate it and install `uv`:

**Windows PowerShell:**
```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install uv
```

**macOS/Linux:**
```bash
python -m venv .venv
source .venv/bin/activate
pip install uv
```

Select the Python interpreter from your new virtual environment in VS Code (`` Ctrl+Shift+P `` > `Python: Select Interpreter`).

Sync dependencies from `requirements.txt`:

```powershell
uv sync
```

From now on, add libraries with:

```powershell
uv add library_name
```

If your project depends on environment variables, verify they're set (for example, `VFB_PROJECT_PATH`). You can check environment variables with:

**Windows PowerShell:**
```powershell
$env:VFB_PROJECT_PATH
```

**macOS/Linux:**
```bash
echo $VFB_PROJECT_PATH
```

To set an environment variable temporarily (for current session):

**Windows PowerShell:**
```powershell
$env:VFB_PROJECT_PATH = "C:\Users\YourName\mainsequence\my_organization\projects\tutorial-project"
```

**macOS/Linux:**
```bash
export VFB_PROJECT_PATH="/home/user/mainsequence/my_organization/projects/tutorial-project"
```

## 3. Build Your First Data Nodes

**Key concepts:** data DAGs, `DataNode`, dependencies, `update_hash`, and `storage_hash`.

Main Sequence encourages you to model workflows as data DAGs (directed acyclic graphs), composing your work into small steps called **data nodes**, each performing a single transformation.

Create a new file at `src\data_nodes\example_nodes.py` (Windows) or `src/data_nodes/example_nodes.py` (macOS/Linux), and define your first node, `DailyRandomNumber`, by subclassing `DataNode`.

You can find the complete code for the subsequent data nodes in the [examples folder](https://github.com/mainsequence-sdk/mainsequence-sdk/tree/main/examples).

```python
from typing import Dict, Union

import pandas as pd

from mainsequence.tdag.data_nodes import DataNode, APIDataNode
import mainsequence.client as msc
import numpy as np
from pydantic import BaseModel, Field


class VolatilityConfig(BaseModel):
    center: float = Field(
        ...,
        title="Standard Deviation",
        description="Standard deviation of the normal distribution (must be > 0).",
        examples=[0.1, 1.0, 2.5],
        gt=0,  # constraint: strictly positive
        le=1e6,  # example upper bound (optional)
        multiple_of=0.0001,  # example precision step (optional)
    )
    skew: bool


class RandomDataNodeConfig(BaseModel):
    mean: float = Field(..., ignore_from_storage_hash=False, title="Mean",
                        description="Mean for the random normal distribution generator")
    std: VolatilityConfig = Field(VolatilityConfig(center=1, skew=True), ignore_from_storage_hash=True,
                                  title="Vol Config",
                                  description="Vol Configuration")


class DailyRandomNumber(DataNode):
    """
    Example Data Node that generates one random number every day
    """

    def __init__(self, node_configuration: RandomDataNodeConfig, *args, **kwargs):
        """
        :param node_configuration: Configuration containing mean and std parameters
        :param kwargs: Additional keyword arguments
        """
        self.node_configuration = node_configuration
        self.mean = node_configuration.mean
        self.std = node_configuration.std
        super().__init__(*args, **kwargs)

    def get_table_metadata(self) -> msc.TableMetaData:
        TS_ID = f"example_random_number_{self.mean}_{self.std}"
        meta = msc.TableMetaData(identifier=TS_ID,
                                description="Example Data Node")

        return meta

    def update(self) -> pd.DataFrame:
        """Draw daily samples from N(mean, std) since last run (UTC days)."""
        today = pd.Timestamp.now("UTC").normalize()
        last = self.update_statistics.max_time_index_value
        if last is not None and last >= today:
            return pd.DataFrame()
        return pd.DataFrame(
            {"random_number": [np.random.normal(self.mean, self.std.center)]},
            index=pd.DatetimeIndex([today], name="time_index", tz="UTC"),
        )

    def dependencies(self) -> Dict[str, Union["DataNode", "APIDataNode"]]:
        """
        This node does not depend on any other data nodes.
        """
        return {}
```

### DataNode Recipe

To create a data node we must follow the same recipe every time:

1. Extend the base class `mainsequence.tdag.DataNode`
2. Implement the constructor method `__init__()`
3. Implement the `dependencies()` method
4. Implement the `update()` method

#### The update() Method

The update method has only one requirement: it should return a `pandas.DataFrame` with the following characteristics:

* Update method always needs to return a `pd.DataFrame()`
* Your first index should always be of type `datetime.datetime(timezone="UTC")` and should not have duplicates
* Your columns should always be lowercase and no more than 63 characters
* Your column types are only allowed to be float, int, str; for dates you need to transform to int or float
* The DataFrame should not be empty; if there is no new data to return, return `pd.DataFrame()`

Next, create `scripts\random_number_launcher.py` to run the node:

```python
from src.data_nodes.example_nodes import DailyRandomNumber, RandomDataNodeConfig


daily_node = DailyRandomNumber(node_configuration=RandomDataNodeConfig(mean=0.0))
daily_node.run(debug_mode=True, force_update=True)
```

To run and debug in VS Code, you can configure a launch file at `.vscode\launch.json`:

**Windows:**
```json
{
    "version": "0.2.0",
    "configurations": [
        {
            "name": "Debug random_number_launcher",
            "type": "debugpy",
            "request": "launch",
            "program": "${workspaceFolder}\\scripts\\random_number_launcher.py",
            "console": "integratedTerminal",
            "env": {
                "PYTHONPATH": "${workspaceFolder}"
            },
            "python": "${workspaceFolder}\\.venv\\Scripts\\python.exe"
        }
    ]
}
```

**macOS/Linux:**
```json
{
    "version": "0.2.0",
    "configurations": [
        {
            "name": "Debug random_number_launcher",
            "type": "debugpy",
            "request": "launch",
            "program": "${workspaceFolder}/scripts/random_number_launcher.py",
            "console": "integratedTerminal",
            "env": {
                "PYTHONPATH": "${workspaceFolder}"
            },
            "python": "${workspaceFolder}/.venv/bin/python"
        }
    ]
}
```

Back to your `random_number_launcher.py`, and at the top right corner of VS Code you will see **Run Python File** dropdown, click on the **Python Debugger: Debug using launch.json** option and finally select the debug configuration you just created.

This will execute the configuration. Then open:

https://main-sequence.app/dynamic-table-metadatas/

Search for `dailyrandom`. You should see your data node and its table.

![img.png](../img/tutorial/table_search.png)

Click the **storage hash**, then in the table's context menu (the **…** button), select **Explore Table Data** to confirm that your node persisted data.

![img.png](../img/tutorial/random_number_table.png)

### Add a Dependent Data Node

Now extend the workflow with a node that depends on `DailyRandomNumber`. Add the following to `src\data_nodes\example_nodes.py`:

```python
class DailyRandomAdditionAPI(DataNode):
    def __init__(self,mean:float,std:float,
                 dependency_identifier:int,
                 *args, **kwargs):
        self.mean=mean
        self.std=std

        self.daily_random_number_data_node=APIDataNode.build_from_identifier(identifier=dependency_identifier)
        super().__init__(*args, **kwargs)
    def dependencies(self):
        return {"number_generator":self.daily_random_number_data_node}
    def update(self) -> pd.DataFrame:
        """Draw daily samples from N(mean, std) since last run (UTC days)."""
        today = pd.Timestamp.now("UTC").normalize()
        last = self.update_statistics.max_time_index_value
        if last is not None and last >= today:
            return pd.DataFrame()
        random_number=np.random.normal(self.mean, self.std)
        dependency_noise=self.daily_random_number_data_node.\
                get_df_between_dates(start_date=today, great_or_equal=True).iloc[0]["random_number"]
        self.logger.info(f"random_number={random_number} dependency_noise={dependency_noise}")

        return pd.DataFrame(
            {"random_number": [random_number+dependency_noise]},
            index=pd.DatetimeIndex([today], name="time_index", tz="UTC"),
        )
```

This simply defines a **dependent** node (`DailyRandomAddition`) that references and uses the output of `DailyRandomNumber`.

Create a launcher at `scripts\random_daily_addition_launcher.py`:

```python
from src.data_nodes.example_nodes import DailyRandomAddition


daily_node = DailyRandomAddition(mean=0.0, std=1.0)
daily_node.run(debug_mode=True, force_update=True)
```

Run it, then return to the Dynamic Table Metadatas page:

https://main-sequence.app/dynamic-table-metadatas/?search=dailyrandom&storage_hash=&identifier=

Open the `dailyrandomaddition_XXXXX` table to explore it. For a visual of the dependency structure, click the **update process** arrow and then the **update hash**.

![img.png](../img/tutorial/update_hash.png)

You'll see the dependency graph for this workflow:

![img.png](../img/tutorial/update_hash_detail.png)

## 4. `update_hash` vs. `storage_hash`

A `DataNode` does two critical things in Main Sequence:

1. Controls the **update process** for your data (sequential or time-series based).  
2. Persists data in the **Data Engine** (think of it as a managed database—no need to handle schemas, sessions, etc.).

To support both, each `DataNode` uses two identifiers:

- **`update_hash`**: a unique hash derived from the combination of arguments that define an update process. In the random-number example, that might include `mean` and `std`.
- **`storage_hash`**: an identifier for where data is stored. It can ignore specific arguments so multiple update processes can write to the **same** table.

Why do this? Sometimes you want to store data from different processes in a single table. While the simple example here is contrived, this pattern becomes very useful with multi-index tables.

Now update your **daily random number launcher** to run two update processes with different volatility configurations but the **same** storage. You'll still see **two update processes**, but they'll write to the **same underlying table** for the daily random number node.

![img.png](../img/tutorial/update_vs_storage.png)