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
mainsequence login --access-token "$TOKEN" --refresh-token "$REFRESH"
mainsequence login --access-token "$TOKEN" --refresh-token "$REFRESH" --backend http://127.0.0.1:80 --projects-base mainsequence-dev
mainsequence logout
```

Backend/base-folder overrides passed to `login` are terminal-session only. They do not rewrite the persisted CLI settings for other terminals.

By default, `mainsequence login` persists auth tokens for later CLI commands:

- macOS: secure OS storage
- Linux and other platforms without secure-store support: local CLI auth storage under the MainSequence config directory

You only need `--export` if you explicitly want shell-managed environment variables.

If you prefer shell-managed environment variables:

```bash
mainsequence login you@company.com --export
mainsequence login --access-token "$TOKEN" --refresh-token "$REFRESH" --export
mainsequence logout --export
```

## Structured Output

Commands that return a structured object or a list of objects also accept `--json`.

The flag is global and can be placed after the command you are running, for example:

```bash
mainsequence user --json
mainsequence agent list --json
mainsequence project images list --json
mainsequence cc workspace detail 7 --json
mainsequence sdk latest --json
mainsequence project current --json
mainsequence project sdk-status --path . --json
```

When the underlying SDK result is a Pydantic model, the CLI serializes it through the model's JSON dump path before printing.

## Core Command Groups

## Top-Level Commands

```bash
mainsequence --help
mainsequence doctor
mainsequence constants --help
mainsequence secrets --help
mainsequence agent --help
mainsequence simple_table --help
mainsequence cc --help
mainsequence organization --help
mainsequence skills list
mainsequence skills path
mainsequence skills path project_builder
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

## Command Center

```bash
mainsequence cc --help
mainsequence cc workspace list
mainsequence cc workspace detail 7
mainsequence cc workspace create "Rates Desk" --description "Shared workspace"
mainsequence cc workspace create --file workspace.json
mainsequence cc workspace update 7 --file workspace.json
mainsequence cc workspace delete 7
mainsequence cc workspace add-label 7 --label trading --label desk
mainsequence cc workspace remove-label 7 --label old-layout
mainsequence cc registered_widget_type list
mainsequence cc registered_widget_type detail main-sequence-data-node
mainsequence cc registered_widget_type list --filter widget_id=markdown-note
mainsequence cc registered_widget_type list --show-filters
```

Command Center commands are grouped under `cc`:

- `workspace`
  create, detail, update, list, and delete shared workspaces
- `registered_widget_type`
  inspect the widget catalog available to workspaces, including widget-type detail by unique `widget_id` rather than backend row `id`

For widget-specific workspace mutations, prefer the SDK workspace methods instead of rewriting the full workspace document:

- `Workspace.patch_workspace_widget(...)`
- `Workspace.delete_workspace_widget(...)`
- `Workspace.move_workspace_widget(...)`

Those methods mutate one mounted widget instance directly without requiring a full workspace fetch/update round-trip.

Most frequently used flows:

```bash
# Markets
mainsequence agent list
mainsequence agent detail 12
mainsequence agent create "Research Copilot" --agent-unique-id research-copilot --description "Desk agent"
mainsequence agent get_or_create "Research Copilot" --agent-unique-id research-copilot --description "Desk agent"
mainsequence agent start_new_session 12
mainsequence agent get_latest_session 12
mainsequence agent session detail 801
mainsequence agent can_view 12
mainsequence agent can_edit 12
mainsequence agent add_to_view 12 7
mainsequence agent add_to_edit 12 7
mainsequence agent add_team_to_view 12 9
mainsequence agent add_team_to_edit 12 9
mainsequence agent remove_from_view 12 7
mainsequence agent remove_from_edit 12 7
mainsequence agent remove_team_from_view 12 9
mainsequence agent remove_team_from_edit 12 9
mainsequence agent delete 12
mainsequence agent run list
mainsequence agent run detail 501
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
mainsequence simple_table list --filter namespace=pytest_alice
mainsequence simple_table detail 41
mainsequence simple_table add-label 41 --label reference-data
mainsequence simple_table remove-label 41 --label deprecated
mainsequence simple_table delete 41
mainsequence data-node list
mainsequence data-node list --show-filters
mainsequence data-node list --filter namespace=pytest_alice
mainsequence data-node list --filter id__in=42,43
mainsequence data-node list --data-source-id 2
mainsequence data_node search "close price"
mainsequence data-node search "close price" --data-source-id 2
mainsequence data-node search "portfolio weights" --mode description
mainsequence data-node search close --mode column
mainsequence data-node detail 123
mainsequence data-node refresh-search-index 123
mainsequence data-node add-label 123 --label curated
mainsequence data-node remove-label 123 --label legacy
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
mainsequence project add-label 123 --label rates --label research
mainsequence project remove-label 123 --label legacy
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
mainsequence project schedule_batch_jobs scheduled_jobs.yaml
mainsequence project schedule_batch_jobs scheduled_jobs.yaml --strict
mainsequence project data-node-updates list
mainsequence project data-node-updates list 123
mainsequence project project_resource list
mainsequence project project_resource list --show-filters
mainsequence project project_resource list --filter resource_type=dashboard
mainsequence project project_resource list --filter resource_type=fastapi
mainsequence project project_resource create_fastapi
mainsequence project project_resource create_fastapi 123
mainsequence project project_resource delete_fastapi 701
mainsequence project project_resource delete_fastapi 701 --yes
mainsequence project validate-name "Rates Platform"
mainsequence cc workspace add-label 7 --label trading --label desk
mainsequence cc workspace remove-label 7 --label old-layout

# 2) Set up locally
mainsequence project set-up-locally 123
mainsequence project refresh_token

# 3) Environment setup
mainsequence project build_local_venv
mainsequence project build_local_venv --path .
mainsequence project freeze-env --path .
mainsequence project update AGENTS.md
mainsequence project update AGENTS.md --path .
mainsequence project update_agent_skills
mainsequence project update_agent_skills --path .

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

## Skills

```bash
mainsequence skills list
mainsequence skills list --json
mainsequence skills path
mainsequence skills path project_builder
mainsequence skills path command_center/workspace_builder
mainsequence skills path workspace_builder
mainsequence skills path workspace_builder --json
```

## Troubleshooting

- Run `mainsequence doctor` to check config, auth visibility, and tool availability.
- If a command says not logged in, run `mainsequence login <email>` again.
- `mainsequence login` persists tokens for later CLI runs. Use `--export` only when you explicitly want shell-managed auth variables instead.
- `mainsequence skills list` lists installed scaffold skills from the current CLI installation by recursively discovering `SKILL.md` files under the installed `agent_scaffold` bundle.
- `mainsequence skills path` with no argument prints the installed `agent_scaffold/skills` directory for the current CLI installation.
- `mainsequence skills path <skill_name>` prints the installed `SKILL.md` path for one scaffold skill from the current CLI installation. It accepts full relative skill names such as `command_center/workspace_builder` and unique leaf names such as `workspace_builder`.
- `mainsequence user` shows the authenticated MainSequence user through the SDK client `User.get_logged_user()` path.
- `mainsequence organization project-names` lists the project names visible to the authenticated user's organization through the SDK client `Project.get_org_project_names()` path.
- `mainsequence organization teams list` lists teams through the SDK client `Team.filter()` path.
- `mainsequence organization teams create`, `edit`, and `delete` use the SDK client `Team.create()`, `Team.patch()`, and `Team.delete()` paths.
- `mainsequence organization teams can_view` and `can_edit` inspect team access through the SDK `Team.can_view()` and `Team.can_edit()` paths.
- `mainsequence organization teams add_to_view`, `add_to_edit`, `remove_from_view`, and `remove_from_edit` mutate explicit user access on teams through the SDK `Team` permission-action paths.
- `mainsequence agent list`, `detail`, `create`, `get_or_create`, `start_new_session`, `get_latest_session`, and `delete` use the SDK client `mainsequence.client.agent_runtime_models.Agent` paths.
- `mainsequence agent session detail` uses the SDK client `mainsequence.client.agent_runtime_models.AgentSession` path.
- `mainsequence agent can_view` and `can_edit` inspect agent sharing through the SDK `ShareableObjectMixin` access-state paths on `Agent`.
- `mainsequence agent add_to_view`, `add_to_edit`, `remove_from_view`, and `remove_from_edit` mutate explicit user access on agents through the SDK `ShareableObjectMixin` permission-action paths.
- `mainsequence agent add_team_to_view`, `add_team_to_edit`, `remove_team_from_view`, and `remove_team_from_edit` mutate explicit team access on agents through the SDK `ShareableObjectMixin` team-action paths.
- `mainsequence agent run list` and `detail` use the SDK client `mainsequence.client.agent_runtime_models.AgentRun` paths for runtime inspection.
- `mainsequence agent_runtime` is kept as a compatibility alias for the `agent run` command group.
- `mainsequence simple_table list` lists simple table storages through the SDK client `SimpleTableStorage.filter()` path.
- `mainsequence simple_table list --filter namespace=...` is the first-class CLI form for narrowing simple table storages by storage namespace.
- `mainsequence simple_table detail` fetches one simple table storage through `SimpleTableStorage.get()` and renders its schema/configuration in the terminal.
- `mainsequence simple_table add-label` and `remove-label` mutate `SimpleTableStorage` labels through the SDK `LabelableObjectMixin` path. Labels are organizational metadata only and do not affect runtime behavior or functionality.
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
- `mainsequence data-node list --filter namespace=...` is the first-class CLI form for narrowing data node storages by storage namespace.
- `mainsequence data-node list --data-source-id 2` is the first-class shortcut for the common `data_source__id` filter.
- `mainsequence data-node search` is the public search command for data nodes. It can search descriptions, columns, or both through the SDK client `DataNodeStorage.description_search()` and `DataNodeStorage.column_search()` paths.
- `mainsequence data-node search --mode description` only uses `DataNodeStorage.description_search()`.
- `mainsequence data-node search --mode column` only uses `DataNodeStorage.column_search()`.
- `mainsequence data-node search --data-source-id 2` is the first-class shortcut for filtering search results by data source.
- `mainsequence data-node search` supports the same `--filter KEY=VALUE` and `--show-filters` pattern as `data-node list`, based on `DataNodeStorage.FILTERSET_FIELDS` and `FILTER_VALUE_NORMALIZERS`.
- `mainsequence data-node detail` fetches one storage through `DataNodeStorage.get()` and renders its configuration in the terminal.
- `mainsequence data-node refresh-search-index` calls the SDK instance method `DataNodeStorage.refresh_table_search_index()` for one storage and prints the backend response in the terminal.
- `mainsequence data-node add-label` and `remove-label` mutate `DataNodeStorage` labels through the SDK `LabelableObjectMixin` path. Labels are organizational metadata only and do not affect runtime behavior or functionality.
- `mainsequence project validate-name "<PROJECT_NAME>"` validates a candidate project name through the SDK client `Project.validate_name()` path, prints normalized names and suggestions, and exits non-zero when the name is unavailable.
- `mainsequence project update AGENTS.md` is project-scoped. It resolves the target project first, then copies `AGENTS.md` from that project's installed SDK bundle in `.venv` into the project root, overwriting the existing `AGENTS.md` when present.
- `mainsequence project update_agent_skills` is project-scoped. It resolves the target project first, then copies every top-level skill folder from that project's installed `agent_scaffold/skills/` bundle in `.venv` into `.agents/skills/`, overwriting only folders with the same names. It does not copy bundle-root files such as `AGENTS.md`.
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
- `ProjectImage` responses include backend metadata such as `creation_date`, and the SDK model accepts that field.
- All list commands share the same `--filter KEY=VALUE` and `--show-filters` pattern. Commands that already enforce scoping filters reject overriding those keys.
- `mainsequence project images create` only accepts pushed commits for `project_repo_hash`. If omitted, it lists commits from the current branch upstream (or remote refs as fallback), shows which commits already have image ids, and waits until `is_ready=true` by polling every 30 seconds for up to 5 minutes by default.
- `mainsequence project jobs list` lists project jobs through the SDK client `Job.filter()` path.
- `mainsequence project jobs list` shows a human-readable schedule summary from `task_schedule`.
- `mainsequence project data-node-updates list` lists data node updates through the SDK client `Project.get_data_nodes_updates()` path.
- `mainsequence project add-label` and `remove-label` mutate `Project` labels through the SDK `LabelableObjectMixin` path. Labels are organizational metadata only and do not affect runtime behavior or functionality.
- `mainsequence project can_view` lists users returned by the SDK `ShareableObjectMixin.users_can_view()` path for `Project`.
- `mainsequence project can_edit` lists users returned by the SDK `ShareableObjectMixin.users_can_edit()` path for `Project`.
- `mainsequence project add_to_view`, `add_to_edit`, `remove_from_view`, and `remove_from_edit` mutate project user sharing through the SDK `ShareableObjectMixin` paths and render the resulting permission state in the terminal.
- `mainsequence project add_team_to_view`, `add_team_to_edit`, `remove_team_from_view`, and `remove_team_from_edit` mutate project team sharing through the SDK `ShareableObjectMixin` team-action paths.
- `mainsequence project project_resource list` lists project resources through the SDK client `ProjectResource.filter()` path and always applies `repo_commit_sha` from the current upstream branch head.
- `mainsequence cc workspace add-label` and `remove-label` mutate `Workspace` labels through the SDK `LabelableObjectMixin` path. Labels are organizational metadata only and do not affect runtime behavior or functionality.
- `mainsequence project sync` performs the local uv/git sync flow and, after a successful push, calls the SDK client `Project.sync_project_after_commit()` path for the resolved project id.
- `mainsequence project jobs runs list` lists job-run history through the SDK client `JobRun.filter(job__id=[job_id])` path.
- `mainsequence project jobs runs logs` fetches logs through the SDK client `JobRun.get_logs()` path, polls every 30 seconds by default while the job run is `PENDING` or `RUNNING`, and stops after 10 minutes unless you override `--max-wait-seconds` or disable it with `--max-wait-seconds 0`.
- `mainsequence project jobs run` triggers a manual run through the SDK client `Job.run_job()` path.
- `mainsequence project jobs create` creates jobs through the SDK client `Job.create()` path, requires a project image, expects `execution_path` relative to the content root, for example `scripts/test.py`, builds interval or crontab schedules interactively when requested, and defaults compute settings to `cpu_request=0.25`, `memory_request=0.5`, `spot=false`, `max_runtime_seconds=86400` when omitted.
- `mainsequence project schedule_batch_jobs` validates a repository-managed `scheduled_jobs.yaml` file and submits the batch through the SDK client `Job.bulk_get_or_create()` path.
- `mainsequence project schedule_batch_jobs` expects a top-level `jobs` list, resolves the project id from the argument or local `.env`, lets you choose one project image for the whole batch, and supports `--strict` when the file should act as the full desired state.
