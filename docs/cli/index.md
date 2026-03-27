# MainSequence CLI

This page gives a practical overview of the `mainsequence` command-line interface.
For command-by-command behavior, use `--help` (for example: `mainsequence project --help`).
For a deeper workflow guide, see [CLI Deep Dive](../knowledge/cli.md).

## Installation

```bash
pip install mainsequence
```

## Authentication

```bash
mainsequence login you@company.com
mainsequence login you@company.com 127.0.0.1:8000 mainsequence-dev
mainsequence logout
```

Backend/base-folder overrides passed to `login` are terminal-session only. They do not rewrite the persisted CLI settings for other terminals.

If you prefer shell-managed environment variables:

```bash
mainsequence login you@company.com --export
mainsequence logout --export
```

## Core Command Groups

## Top-Level Commands

```bash
mainsequence --help
mainsequence doctor
mainsequence constants --help
mainsequence secrets --help
mainsequence simple_table --help
mainsequence organization --help
mainsequence data-node list
mainsequence markets --help
mainsequence user
mainsequence settings show
mainsequence sdk latest
```

## Project Commands

```bash
mainsequence project --help
```

Most frequently used flows:

```bash
# Markets
mainsequence constants list
mainsequence constants list --show-filters
mainsequence constants create APP__MODE production
mainsequence constants create ASSETS__MASTER '{"dataset":"bloomberg"}'
mainsequence constants can_view 42
mainsequence constants can_edit 42
mainsequence constants add_to_view 42 7
mainsequence constants add_to_edit 42 7
mainsequence constants add_team_to_view 42 9
mainsequence constants add_team_to_edit 42 9
mainsequence constants remove_from_view 42 7
mainsequence constants remove_from_edit 42 7
mainsequence constants remove_team_from_view 42 9
mainsequence constants remove_team_from_edit 42 9
mainsequence constants delete 42
mainsequence secrets list
mainsequence secrets list --show-filters
mainsequence secrets create API_KEY super-secret-value
mainsequence secrets can_view 42
mainsequence secrets can_edit 42
mainsequence secrets add_to_view 42 7
mainsequence secrets add_to_edit 42 7
mainsequence secrets add_team_to_view 42 9
mainsequence secrets add_team_to_edit 42 9
mainsequence secrets remove_from_view 42 7
mainsequence secrets remove_from_edit 42 7
mainsequence secrets remove_team_from_view 42 9
mainsequence secrets remove_team_from_edit 42 9
mainsequence secrets delete 42
mainsequence organization project-names
mainsequence organization teams list
mainsequence organization teams list --show-filters
mainsequence organization teams create Research --description "Model validation"
mainsequence organization teams edit 9 --name "Research Core" --inactive
mainsequence organization teams can_view 9
mainsequence organization teams can_edit 9
mainsequence organization teams add_to_view 9 7
mainsequence organization teams add_to_edit 9 7
mainsequence organization teams remove_from_view 9 7
mainsequence organization teams remove_from_edit 9 7
mainsequence organization teams delete 9
mainsequence simple_table list
mainsequence simple_table detail 41
mainsequence simple_table delete 41
mainsequence data-node list
mainsequence data-node list --show-filters
mainsequence data-node list --filter id__in=42,43
mainsequence data-node list --data-source-id 2
mainsequence data-node org-unique-identifiers
mainsequence data_node search "close price"
mainsequence data-node search "close price" --data-source-id 2
mainsequence data-node search "portfolio weights" --mode description
mainsequence data-node search close --mode column
mainsequence data-node detail 123
mainsequence data-node refresh-search-index 123
mainsequence data-node can_view 123
mainsequence data-node can_edit 123
mainsequence data-node add_to_view 123 7
mainsequence data-node add_to_edit 123 7
mainsequence data-node add_team_to_view 123 9
mainsequence data-node add_team_to_edit 123 9
mainsequence data-node remove_from_view 123 7
mainsequence data-node remove_from_edit 123 7
mainsequence data-node remove_team_from_view 123 9
mainsequence data-node remove_team_from_edit 123 9
mainsequence data-node delete 123
mainsequence data-node delete 123 --full-delete-selected
mainsequence data-node delete 123 --full-delete-selected --override-protection
mainsequence markets portfolios list
mainsequence markets portfolios list --filter id__in=42
mainsequence markets asset-translation-table list
mainsequence markets asset-translation-table list --show-filters
mainsequence markets asset-translation-table detail 12

# 1) List and create
mainsequence project list
mainsequence project can_view 123
mainsequence project can_edit 123
mainsequence project add_to_view 123 7
mainsequence project add_to_edit 123 7
mainsequence project add_team_to_view 123 9
mainsequence project add_team_to_edit 123 9
mainsequence project remove_from_view 123 7
mainsequence project remove_from_edit 123 7
mainsequence project remove_team_from_view 123 9
mainsequence project remove_team_from_edit 123 9
mainsequence project images list
mainsequence project images list 123
mainsequence project images list --show-filters
mainsequence project images list --filter project_repo_hash__in=4a1b2c3d,5e6f7a8b
mainsequence project create tutorial-project
mainsequence project images create
mainsequence project images create 123
mainsequence project images create 123 4a1b2c3d
mainsequence project images create 123 --timeout 600 --poll-interval 15
mainsequence project jobs list
mainsequence project jobs runs list 91
mainsequence project jobs runs logs 501
mainsequence project jobs runs logs 501 --max-wait-seconds 900
mainsequence project jobs run 91
mainsequence project jobs create --name daily-run --execution-path scripts/test.py
mainsequence project data-node-updates list
mainsequence project data-node-updates list 123
mainsequence project project_resource list
mainsequence project project_resource list --show-filters
mainsequence project project_resource list --filter resource_type=dashboard

# 2) Set up locally
mainsequence project set-up-locally 123
mainsequence project refresh_token

# 3) Environment setup
mainsequence project build_local_venv
mainsequence project build_local_venv --path .
mainsequence project freeze-env --path .

# 4) Day-to-day sync
mainsequence project sync "Update environment"
mainsequence project sync --path . -m "Update environment"

# 5) Docker/devcontainer
mainsequence project build-docker-env --path .

# 6) SDK maintenance
mainsequence project sdk-status --path .
mainsequence project update-sdk --path .
```

## List Filters

Most `list` commands accept the same generic filter interface:

```bash
mainsequence <...> list --show-filters
mainsequence <...> list --filter KEY=VALUE
mainsequence <...> list --filter KEY=VALUE --filter OTHER_KEY=VALUE
```

Rules:

- Allowed filters are taken from the backing SDK model `FILTERSET_FIELDS`.
- Value expectations are derived from `FILTER_VALUE_NORMALIZERS`.
- `__in` filters accept comma-separated values such as `id__in=1,2,3`.
- Some commands always apply scoping filters internally and will reject attempts to override them.
  - `mainsequence project images list` always scopes by the selected project.
  - `mainsequence project project_resource list` always scopes by project and upstream remote `repo_commit_sha`.
  - `mainsequence project jobs runs list` always scopes by `job__id`.
- If a command's backing model does not expose filter metadata, `--show-filters` will tell you that no additional model filters are available.
- `mainsequence constants list` exposes filters from `Constant.FILTERSET_FIELDS`, currently `name` and `name__in`.
- `mainsequence secrets list` exposes filters from `Secret.FILTERSET_FIELDS`, currently `name` and `name__in`.

## Settings

```bash
mainsequence settings show
mainsequence settings set-base ~/mainsequence
```

## Troubleshooting

- Run `mainsequence doctor` to check config, auth visibility, and tool availability.
- If a command says not logged in, run `mainsequence login <email>` again.
- If your shell cannot use secure token storage, use `--export` mode.
- `mainsequence user` shows the authenticated MainSequence user through the SDK client `User.get_logged_user()` path.
- `mainsequence organization project-names` lists the project names visible to the authenticated user's organization through the SDK client `Project.get_org_project_names()` path.
- `mainsequence organization teams list` lists teams through the SDK client `Team.filter()` path.
- `mainsequence organization teams create`, `edit`, and `delete` use the SDK client `Team.create()`, `Team.patch()`, and `Team.delete()` paths.
- `mainsequence organization teams can_view` and `can_edit` inspect team access through the SDK `Team.can_view()` and `Team.can_edit()` paths.
- `mainsequence organization teams add_to_view`, `add_to_edit`, `remove_from_view`, and `remove_from_edit` mutate explicit user access on teams through the SDK `Team` permission-action paths.
- `mainsequence simple_table list` lists simple table storages through the SDK client `SimpleTableStorage.filter()` path.
- `mainsequence simple_table detail` fetches one simple table storage through `SimpleTableStorage.get()` and renders its schema/configuration in the terminal.
- `mainsequence simple_table delete` deletes a simple table storage through the SDK client `SimpleTableStorage.delete()` path and always requires typed verification before the delete call is sent.
- `mainsequence constants list` lists constants through the SDK client `Constant.filter()` path.
- `mainsequence constants create` creates a constant through the SDK client `Constant.create()` path and only accepts `name` and `value`.
- `mainsequence constants can_view` lists users returned by the SDK `ShareableObjectMixin.users_can_view()` path for `Constant`.
- `mainsequence constants can_edit` lists users returned by the SDK `ShareableObjectMixin.users_can_edit()` path for `Constant`.
- `mainsequence constants add_to_view`, `add_to_edit`, `remove_from_view`, and `remove_from_edit` mutate constant user sharing through the SDK `ShareableObjectMixin` paths and render the resulting permission state in the terminal.
- `mainsequence constants add_team_to_view`, `add_team_to_edit`, `remove_team_from_view`, and `remove_team_from_edit` mutate constant team sharing through the SDK `ShareableObjectMixin` team-action paths.
- `mainsequence constants delete` deletes a constant through the SDK client `Constant.delete()` path and always requires typed verification before the delete call is sent.
- Constant names that include a double underscore display the prefix before `__` as the terminal category. Example: `ASSETS__MASTER` is shown under category `ASSETS`.
- `mainsequence secrets list` lists secrets through the SDK client `Secret.filter()` path.
- `mainsequence secrets create` creates a secret through the SDK client `Secret.create()` path and only accepts `name` and `value`.
- `mainsequence secrets can_view` lists users returned by the SDK `ShareableObjectMixin.users_can_view()` path for `Secret`.
- `mainsequence secrets can_edit` lists users returned by the SDK `ShareableObjectMixin.users_can_edit()` path for `Secret`.
- `mainsequence secrets add_to_view`, `add_to_edit`, `remove_from_view`, and `remove_from_edit` mutate secret user sharing through the SDK `ShareableObjectMixin` paths and render the resulting permission state in the terminal.
- `mainsequence secrets add_team_to_view`, `add_team_to_edit`, `remove_team_from_view`, and `remove_team_from_edit` mutate secret team sharing through the SDK `ShareableObjectMixin` team-action paths.
- `mainsequence secrets delete` deletes a secret through the SDK client `Secret.delete()` path and always requires typed verification before the delete call is sent.
- Secret list and delete previews intentionally show metadata only, not secret values.
- `mainsequence data-node list` lists data node storages through the SDK client `DataNodeStorage.filter()` path.
- `mainsequence data-node list --show-filters` prints the filters exposed by `DataNodeStorage.FILTERSET_FIELDS` and the expected value shapes from `FILTER_VALUE_NORMALIZERS`.
- `mainsequence data-node list --data-source-id 2` is the first-class shortcut for the common `data_source__id` filter.
- `mainsequence data-node org-unique-identifiers` lists the organization-visible unique identifiers exposed by the SDK client `DataNodeStorage.get_org_unique_identifiers()` path.
- `mainsequence data-node search` is the public search command for data nodes. It can search descriptions, columns, or both through the SDK client `DataNodeStorage.description_search()` and `DataNodeStorage.column_search()` paths.
- `mainsequence data-node search --mode description` only uses `DataNodeStorage.description_search()`.
- `mainsequence data-node search --mode column` only uses `DataNodeStorage.column_search()`.
- `mainsequence data-node search --data-source-id 2` is the first-class shortcut for filtering search results by data source.
- `mainsequence data-node search` supports the same `--filter KEY=VALUE` and `--show-filters` pattern as `data-node list`, based on `DataNodeStorage.FILTERSET_FIELDS` and `FILTER_VALUE_NORMALIZERS`.
- `mainsequence data-node detail` fetches one storage through `DataNodeStorage.get()` and renders its configuration in the terminal.
- `mainsequence data-node refresh-search-index` calls the SDK instance method `DataNodeStorage.refresh_table_search_index()` for one storage and prints the backend response in the terminal.
- `mainsequence data-node can_view` lists users returned by the SDK `ShareableObjectMixin.can_view()` path for `DataNodeStorage`.
- `mainsequence data-node can_edit` lists users returned by the SDK `ShareableObjectMixin.can_edit()` path for `DataNodeStorage`.
- `mainsequence data-node add_to_view`, `add_to_edit`, `remove_from_view`, and `remove_from_edit` mutate data-node user sharing through the SDK `ShareableObjectMixin` paths and render the resulting permission state in the terminal.
- `mainsequence data-node add_team_to_view`, `add_team_to_edit`, `remove_team_from_view`, and `remove_team_from_edit` mutate data-node team sharing through the SDK `ShareableObjectMixin` team-action paths.
- `mainsequence data-node delete` executes the SDK client `DataNodeStorage.delete()` path and exposes the same delete flags as the client: `full_delete_selected`, `full_delete_downstream_tables`, `delete_with_no_table`, and `override_protection`.
- `mainsequence data-node delete` always requires typed verification before the delete call is sent.
- `mainsequence markets portfolios list` lists markets portfolios through the SDK client `Portfolio.filter()` path.
- `mainsequence markets asset-translation-table list` lists translation tables through the SDK client `AssetTranslationTable.filter()` path.
- `mainsequence markets asset-translation-table detail` fetches one translation table through `AssetTranslationTable.get()` and renders each rule as a readable `match => target` mapping in the terminal.
- `mainsequence project images list` lists project images using the SDK client `ProjectImage.filter()` path.
- All list commands share the same `--filter KEY=VALUE` and `--show-filters` pattern. Commands that already enforce scoping filters reject overriding those keys.
- `mainsequence project images create` only accepts pushed commits for `project_repo_hash`. If omitted, it lists commits from the current branch upstream (or remote refs as fallback), shows which commits already have image ids, and waits until `is_ready=true` by polling every 30 seconds for up to 5 minutes by default.
- `mainsequence project jobs list` lists project jobs through the SDK client `Job.filter()` path.
- `mainsequence project jobs list` shows a human-readable schedule summary from `task_schedule`.
- `mainsequence project data-node-updates list` lists data node updates through the SDK client `Project.get_data_nodes_updates()` path.
- `mainsequence project can_view` lists users returned by the SDK `ShareableObjectMixin.users_can_view()` path for `Project`.
- `mainsequence project can_edit` lists users returned by the SDK `ShareableObjectMixin.users_can_edit()` path for `Project`.
- `mainsequence project add_to_view`, `add_to_edit`, `remove_from_view`, and `remove_from_edit` mutate project user sharing through the SDK `ShareableObjectMixin` paths and render the resulting permission state in the terminal.
- `mainsequence project add_team_to_view`, `add_team_to_edit`, `remove_team_from_view`, and `remove_team_from_edit` mutate project team sharing through the SDK `ShareableObjectMixin` team-action paths.
- `mainsequence project project_resource list` lists project resources through the SDK client `ProjectResource.filter()` path and always applies `repo_commit_sha` from the current upstream branch head.
- `mainsequence project sync` performs the local uv/git sync flow and, after a successful push, calls the SDK client `Project.sync_project_after_commit()` path for the resolved project id.
- `mainsequence project jobs runs list` lists job-run history through the SDK client `JobRun.filter(job__id=[job_id])` path.
- `mainsequence project jobs runs logs` fetches logs through the SDK client `JobRun.get_logs()` path, polls every 30 seconds by default while the job run is `PENDING` or `RUNNING`, and stops after 10 minutes unless you override `--max-wait-seconds` or disable it with `--max-wait-seconds 0`.
- `mainsequence project jobs run` triggers a manual run through the SDK client `Job.run_job()` path.
- `mainsequence project jobs create` creates jobs through the SDK client `Job.create()` path, requires a project image, expects `execution_path` relative to the content root, for example `scripts/test.py`, builds interval or crontab schedules interactively when requested, and defaults compute settings to `cpu_request=0.25`, `memory_request=0.5`, `spot=false`, `max_runtime_seconds=86400` when omitted.
