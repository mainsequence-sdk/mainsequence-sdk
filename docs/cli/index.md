# MainSequence CLI

This page gives a practical overview of the `mainsequence` command-line interface.
For command-by-command behavior, use `--help` (for example:
`mainsequence code-repository --help`). The installed CLI exposes both `mainsequence`
and the shorter `ms` command; they point to the same command app.
For a deeper workflow guide, see [CLI Deep Dive](../knowledge/cli.md).

## Installation

```bash
pip install mainsequence
```

## Authentication

```bash
mainsequence login
mainsequence login 127.0.0.1:8000 mainsequence-dev
mainsequence login --no-open
mainsequence login --mcp
mainsequence login --access-token "$TOKEN" --refresh-token "$REFRESH"
mainsequence login --access-token "$TOKEN" --refresh-token "$REFRESH" --backend http://127.0.0.1:80 --projects-base mainsequence-dev
mainsequence logout
```

Backend/base-folder overrides passed to `login` are terminal-session only. They do not rewrite the persisted CLI settings for other terminals.
When no backend is provided, `mainsequence login` targets the currently configured backend shown by `mainsequence doctor`. An explicit `--backend` takes precedence; the standard production backend is used only when no other backend is configured.

By default, `mainsequence login` persists auth tokens for later CLI commands:

- macOS: secure OS storage
- Linux and other platforms without secure-store support: local CLI auth storage under the MainSequence config directory

You only need `--export` if you explicitly want shell-managed environment variables.
`--export` cannot be combined with `--mcp`.

`mainsequence login --mcp` is for a coding agent that already has an
authenticated Main Sequence MCP connection. The CLI creates PKCE state and a
challenge, asks the configured backend to create a short-lived handoff, and
prints the exact `auth.cli_authorize` tool invocation. The backend returns the
callback URI; the CLI does not create a localhost callback for this flow.
After the MCP tool authorizes the handoff, the backend returns the normal
tracked JWT pair directly to the waiting CLI, which persists it in the same
local auth storage used by browser login. Tokens never pass through the MCP
tool result or terminal output.

`mainsequence logout` now performs a hard CLI logout when a browser-login refresh token exists:

- it calls `POST /auth/cli/revoke/` to revoke the tracked CLI login session server-side
- on older backends without that endpoint, it falls back to JWT logout when possible
- in runtime credential mode, or whenever no CLI refresh token exists, it only clears local CLI auth state

If you prefer shell-managed environment variables:

```bash
mainsequence login --export
mainsequence login --access-token "$TOKEN" --refresh-token "$REFRESH" --export
mainsequence logout --export
```

## Structured Output

Commands that return a structured object or a list of objects also accept `--json`.

The flag is global and can be placed after the command you are running, for example:

```bash
mainsequence user --json
mainsequence agent list --environment-uid <ORGANIZATION_ENVIRONMENT_UID> --json
mainsequence code-repository images list --json
mainsequence sdk latest --json
mainsequence code-repository current --json
mainsequence code-repository sdk-status --path . --json
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
mainsequence organization --help
mainsequence skills list
mainsequence skills path
mainsequence skills path sdk_code_repository_execution
mainsequence skills path maintenance/code_repository_maintenance
mainsequence time-index-table list
mainsequence user
mainsequence settings show
mainsequence sdk latest
```

## Project Commands

```bash
mainsequence code-repository --help
```

Most frequently used flows:

```bash
# Agents
mainsequence agent list --environment-uid <ORGANIZATION_ENVIRONMENT_UID>
mainsequence agent search "data research copilot" --environment-uid <ORGANIZATION_ENVIRONMENT_UID>
mainsequence agent detail e0e75693-4110-464c-93e0-82c7fd9c9a23
mainsequence agent create "Research Copilot" --description "Desk agent"
mainsequence agent session list --agent-uid e0e75693-4110-464c-93e0-82c7fd9c9a23
mainsequence agent session get_or_create e0e75693-4110-464c-93e0-82c7fd9c9a23 --handle-unique-id portfolio-review-q2-2026 --name "Quarterly portfolio review"
mainsequence agent session get_or_create e0e75693-4110-464c-93e0-82c7fd9c9a23 --session-uid 3f1cc452-43ec-49cb-b2ba-87dbac164d29
mainsequence agent session a2a send 3f1cc452-43ec-49cb-b2ba-87dbac164d29 --message "Return a JSON object with summary and next_action." --strict-dictionary
mainsequence agent session detail 3f1cc452-43ec-49cb-b2ba-87dbac164d29
mainsequence agent can_view e0e75693-4110-464c-93e0-82c7fd9c9a23
mainsequence agent can_edit e0e75693-4110-464c-93e0-82c7fd9c9a23
mainsequence agent add_to_view e0e75693-4110-464c-93e0-82c7fd9c9a23 <USER_UID>
mainsequence agent add_to_edit e0e75693-4110-464c-93e0-82c7fd9c9a23 <USER_UID>
mainsequence agent add_team_to_view e0e75693-4110-464c-93e0-82c7fd9c9a23 <TEAM_UID>
mainsequence agent add_team_to_edit e0e75693-4110-464c-93e0-82c7fd9c9a23 <TEAM_UID>
mainsequence agent remove_from_view e0e75693-4110-464c-93e0-82c7fd9c9a23 <USER_UID>
mainsequence agent remove_from_edit e0e75693-4110-464c-93e0-82c7fd9c9a23 <USER_UID>
mainsequence agent remove_team_from_view e0e75693-4110-464c-93e0-82c7fd9c9a23 <TEAM_UID>
mainsequence agent remove_team_from_edit e0e75693-4110-464c-93e0-82c7fd9c9a23 <TEAM_UID>
mainsequence agent delete e0e75693-4110-464c-93e0-82c7fd9c9a23
mainsequence constants list
mainsequence constants list --show-filters
mainsequence constants create APP__MODE production
mainsequence constants create ASSETS__MASTER '{"dataset":"bloomberg"}'
mainsequence constants can_view <CONSTANT_UID>
mainsequence constants can_edit <CONSTANT_UID>
mainsequence constants add_to_view <CONSTANT_UID> <USER_UID>
mainsequence constants add_to_edit <CONSTANT_UID> <USER_UID>
mainsequence constants add_team_to_view <CONSTANT_UID> <TEAM_UID>
mainsequence constants add_team_to_edit <CONSTANT_UID> <TEAM_UID>
mainsequence constants remove_from_view <CONSTANT_UID> <USER_UID>
mainsequence constants remove_from_edit <CONSTANT_UID> <USER_UID>
mainsequence constants remove_team_from_view <CONSTANT_UID> <TEAM_UID>
mainsequence constants remove_team_from_edit <CONSTANT_UID> <TEAM_UID>
mainsequence constants delete <CONSTANT_UID>
mainsequence secrets list
mainsequence secrets list --show-filters
mainsequence secrets create API_KEY super-secret-value
mainsequence secrets can_view <SECRET_UID>
mainsequence secrets can_edit <SECRET_UID>
mainsequence secrets add_to_view <SECRET_UID> <USER_UID>
mainsequence secrets add_to_edit <SECRET_UID> <USER_UID>
mainsequence secrets add_team_to_view <SECRET_UID> <TEAM_UID>
mainsequence secrets add_team_to_edit <SECRET_UID> <TEAM_UID>
mainsequence secrets remove_from_view <SECRET_UID> <USER_UID>
mainsequence secrets remove_from_edit <SECRET_UID> <USER_UID>
mainsequence secrets remove_team_from_view <SECRET_UID> <TEAM_UID>
mainsequence secrets remove_team_from_edit <SECRET_UID> <TEAM_UID>
mainsequence secrets delete <SECRET_UID>
mainsequence code-repository search tutorial
mainsequence organization github-organizations
mainsequence organization teams list
mainsequence organization teams list --show-filters
mainsequence organization teams create Research --description "Model validation"
mainsequence organization teams edit <TEAM_UID> --name "Research Core" --inactive
mainsequence organization teams can_view <TEAM_UID>
mainsequence organization teams can_edit <TEAM_UID>
mainsequence organization teams add_to_view <TEAM_UID> <USER_UID>
mainsequence organization teams add_to_edit <TEAM_UID> <USER_UID>
mainsequence organization teams remove_from_view <TEAM_UID> <USER_UID>
mainsequence organization teams remove_from_edit <TEAM_UID> <USER_UID>
mainsequence organization teams delete <TEAM_UID>
mainsequence meta-table run_query <META_TABLE_UID> "SELECT 1 AS ok"
mainsequence time-index-table list
mainsequence time-index-table list --show-filters
mainsequence time-index-table list --filter namespace=pytest_alice
mainsequence time-index-table list --filter uid__in=<TIME_INDEX_META_TABLE_UID>
mainsequence time-index-table list --data-source-uid <DATA_SOURCE_UID>
mainsequence time-index-table search "close price"
mainsequence time-index-table search "close price" --data-source-uid <DATA_SOURCE_UID>
mainsequence time-index-table search close --mode column
mainsequence time-index-table detail <TIME_INDEX_META_TABLE_UID>
mainsequence time-index-table run_query <TIME_INDEX_META_TABLE_UID> "SELECT 1 AS ok"
mainsequence time-index-table refresh-search-index <TIME_INDEX_META_TABLE_UID>
mainsequence time-index-table add-label <TIME_INDEX_META_TABLE_UID> --label curated
mainsequence time-index-table remove-label <TIME_INDEX_META_TABLE_UID> --label legacy
mainsequence time-index-table can_view <TIME_INDEX_META_TABLE_UID>
mainsequence time-index-table can_edit <TIME_INDEX_META_TABLE_UID>
mainsequence time-index-table add_to_view <TIME_INDEX_META_TABLE_UID> <USER_UID>
mainsequence time-index-table add_to_edit <TIME_INDEX_META_TABLE_UID> <USER_UID>
mainsequence time-index-table add_team_to_view <TIME_INDEX_META_TABLE_UID> <TEAM_UID>
mainsequence time-index-table add_team_to_edit <TIME_INDEX_META_TABLE_UID> <TEAM_UID>
mainsequence time-index-table remove_from_view <TIME_INDEX_META_TABLE_UID> <USER_UID>
mainsequence time-index-table remove_from_edit <TIME_INDEX_META_TABLE_UID> <USER_UID>
mainsequence time-index-table remove_team_from_view <TIME_INDEX_META_TABLE_UID> <TEAM_UID>
mainsequence time-index-table remove_team_from_edit <TIME_INDEX_META_TABLE_UID> <TEAM_UID>
mainsequence time-index-table delete <TIME_INDEX_META_TABLE_UID>
mainsequence time-index-table delete <TIME_INDEX_META_TABLE_UID> --full-delete-selected
mainsequence time-index-table delete <TIME_INDEX_META_TABLE_UID> --full-delete-selected --override-protection

# 1) List and create
mainsequence code-repository list
mainsequence code-repository add-label <CODE_REPOSITORY_UID> --label rates --label research
mainsequence code-repository remove-label <CODE_REPOSITORY_UID> --label legacy
mainsequence code-repository can_view <CODE_REPOSITORY_UID>
mainsequence code-repository can_edit <CODE_REPOSITORY_UID>
mainsequence code-repository add_to_view <CODE_REPOSITORY_UID> <USER_UID>
mainsequence code-repository add_to_edit <CODE_REPOSITORY_UID> <USER_UID>
mainsequence code-repository add_team_to_view <CODE_REPOSITORY_UID> <TEAM_UID>
mainsequence code-repository add_team_to_edit <CODE_REPOSITORY_UID> <TEAM_UID>
mainsequence code-repository remove_from_view <CODE_REPOSITORY_UID> <USER_UID>
mainsequence code-repository remove_from_edit <CODE_REPOSITORY_UID> <USER_UID>
mainsequence code-repository remove_team_from_view <CODE_REPOSITORY_UID> <TEAM_UID>
mainsequence code-repository remove_team_from_edit <CODE_REPOSITORY_UID> <TEAM_UID>
mainsequence code-repository images list
mainsequence code-repository images list <CODE_REPOSITORY_UID>
mainsequence code-repository images list --show-filters
mainsequence code-repository images list --filter code_repository_commit_hash__in=4a1b2c3d,5e6f7a8b
mainsequence code-repository create tutorial-project
mainsequence code-repository create tutorial-project --default-base-image-uid <base_image_uid> --github-org-uid <github_org_uid>
mainsequence code-repository images create
mainsequence code-repository images create <CODE_REPOSITORY_UID>
mainsequence code-repository images create <CODE_REPOSITORY_UID> 4a1b2c3d
mainsequence code-repository images create <CODE_REPOSITORY_UID> --timeout 600 --poll-interval 15
mainsequence code-repository jobs list
mainsequence code-repository jobs runs list <JOB_UID>
mainsequence code-repository jobs runs logs <JOB_RUN_UID>
mainsequence code-repository jobs runs logs <JOB_RUN_UID> --max-wait-seconds 900
mainsequence code-repository jobs run <JOB_UID>
mainsequence code-repository jobs run <JOB_UID> --arg demo-from-cli
mainsequence code-repository jobs run <JOB_UID> -- --name demo-from-cli
mainsequence code-repository jobs create --name daily-run --execution-path scripts/test.py --related-image-uid <IMAGE_UID>
mainsequence code-repository jobs create --name promoted-run --execution-path scripts/test.py --automatic-deployment
mainsequence code-repository time-index-table-updates list
mainsequence code-repository time-index-table-updates list <CODE_REPOSITORY_UID>
mainsequence code-repository resources list
mainsequence code-repository resources list --show-filters
mainsequence code-repository resources list --filter resource_type=dashboard
mainsequence code-repository resources list --filter resource_type=fastapi
mainsequence code-repository resources create_fastapi
mainsequence code-repository resources create_fastapi <CODE_REPOSITORY_UID>
mainsequence code-repository resources delete_fastapi <RELEASE_UID>
mainsequence code-repository resources delete_fastapi <RELEASE_UID> --yes
mainsequence code-repository validate-name "Rates Platform"

# 2) Set up locally
mainsequence code-repository set-up-locally <CODE_REPOSITORY_UID>
mainsequence code-repository refresh-token

# 3) Environment setup
mainsequence code-repository build-local-venv
mainsequence code-repository build-local-venv --path .
mainsequence code-repository build-local-venv --path . --recreate
mainsequence code-repository freeze-env --path .
mainsequence code-repository update AGENTS.md
mainsequence code-repository update AGENTS.md --path .
mainsequence code-repository update-agent-skills
mainsequence code-repository update-agent-skills --path .

# 4) Day-to-day sync
mainsequence code-repository sync "Update environment"
mainsequence code-repository sync --path . -m "Update environment"
mainsequence code-repository sync --path . -m "Preview environment" --dry-run

# 5) Docker/devcontainer
mainsequence code-repository build-docker-env --path .

# 6) SDK maintenance
mainsequence code-repository sdk-status --path .
mainsequence code-repository update-sdk --path .
```

During `set-up-locally`, the CLI registers a new or inaccessible deploy key through
`/api/v1/code-repositories/{code_repository_uid}/add-deploy-key/` and verifies repository access with the forced
identity before cloning. Registration or access failure stops setup. Repository branch selection
only chooses the branch to clone; it is not deploy-key ownership.

The key filename is `~/.ssh/mainsequence-<repository-slug>-<first-16-sha256>`, with SHA-256 applied
to normalized `host[:non-default-port]/repository/path`. Equivalent SCP and `ssh://` origins share
one identity; same-basename repositories do not. Basename-only legacy keys are neither modified nor
used as a compatibility fallback.

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
  - `mainsequence code-repository images list` always scopes by the selected project.
  - `mainsequence code-repository resources list` always scopes by project and upstream remote `repo_commit_sha`.
  - `mainsequence code-repository jobs runs list` always scopes by `job__uid`.
- If a command's backing model does not expose filter metadata, `--show-filters` will tell you that no additional model filters are available.
- `mainsequence constants list` exposes filters from `Constant.FILTERSET_FIELDS`, currently `name` and `name__in`.
- `mainsequence secrets list` exposes filters from `Secret.FILTERSET_FIELDS`, currently `name` and `name__in`.

## Settings

```bash
mainsequence settings show
mainsequence settings set-base ~/mainsequence
mainsequence settings set-backend <backend-url>
mainsequence settings reset
mainsequence settings refresh
```

## Skills

```bash
mainsequence skills list
mainsequence skills list --json
mainsequence skills path
mainsequence skills path sdk_code_repository_execution
mainsequence skills path maintenance/code_repository_maintenance
mainsequence skills path data_publishing/meta_tables
mainsequence skills path meta_tables
mainsequence skills path meta_tables --json
```

### Updating project agent skills

`mainsequence code-repository update-agent-skills --path <PROJECT>` performs one
dual-source update:

1. it resolves SDK-owned execution skills from the target project's installed
   `agent_scaffold/skills` and records that installed SDK version;
2. it uses the already-configured platform JWT to initialize `/mcp`, discover
   the server-owned platform catalog with `resources/list`, reads the ontology
   first, and retrieves the skills declared by `ontology.skill_resources` with
   `resources/read`;
3. it validates one complete manifest revision, generic URI/name/path/front-
   matter rules, every content hash, and the SDK/platform destination ownership
   map; and
4. it stages the combined result before replacing only
   `.agents/skills/mainsequence/`.

The command does not cache or package platform resources in the SDK. It
requires the backend for the platform lane. The ontology is read and hashed as
part of the platform manifest identity and its `skill_resources` array is the
authoritative skill index. The SDK does not pin concrete platform skill names
or MCP list order. A valid additive platform skill is accepted without an SDK
catalog change, while missing, undeclared, duplicate, unsafe, or internally
inconsistent platform skill resources are rejected. Unrelated MCP resources
are ignored and not read. Only validated platform skill resources are
materialized under `.agents/skills/mainsequence/` in deterministic name/URI
order.

If authentication, transport, unsupported manifest schema, catalog validation,
staging, or final replacement fails, the command exits non-zero and preserves
the previous managed tree and sentinel. It never changes project-owned skills
outside `.agents/skills/mainsequence/`, and it is not run implicitly when an
agent starts.

Use `--json` for the machine-readable result. Existing top-level compatibility
fields remain, while `sdk`, `platform`, and each `updated[].owner` identify the
two independent sources:

```json
{
  "project": "/project",
  "library_name": "mainsequence",
  "namespace": "mainsequence",
  "pinned_version": "5.0.0",
  "sdk": {
    "library_name": "mainsequence",
    "version": "5.0.0",
    "skills_path": "/project/.venv/lib/pythonX.Y/site-packages/agent_scaffold/skills"
  },
  "platform": {
    "source_url": "https://platform.example/mcp",
    "manifest_version": 2,
    "manifest_sha256": "<sha256>",
    "ontology_uri": "mainsequence://platform/ontology",
    "ontology_sha256": "<sha256>",
    "resources": [
      {
        "name": "ontology",
        "uri": "mainsequence://platform/ontology",
        "path": "ontology/platform.json",
        "content_sha256": "<sha256>"
      },
      {
        "name": "a2a_communication",
        "uri": "mainsequence://platform/skills/a2a-communication",
        "path": "skills/agents/a2a_communication/SKILL.md",
        "content_sha256": "<sha256>"
      },
      {
        "name": "project_design",
        "uri": "mainsequence://platform/skills/project-design",
        "path": "skills/platform/project_design/SKILL.md",
        "content_sha256": "<sha256>"
      },
      {
        "name": "project_to_agent",
        "uri": "mainsequence://platform/skills/code-repository-to-agent",
        "path": "skills/agents/project_to_agent/SKILL.md",
        "content_sha256": "<sha256>"
      }
    ],
    "skills": [
      {
        "name": "a2a_communication",
        "uri": "mainsequence://platform/skills/a2a-communication",
        "path": "agents/a2a_communication/SKILL.md",
        "content_sha256": "<sha256>"
      },
      {
        "name": "project_design",
        "uri": "mainsequence://platform/skills/project-design",
        "path": "platform/project_design/SKILL.md",
        "content_sha256": "<sha256>"
      },
      {
        "name": "project_to_agent",
        "uri": "mainsequence://platform/skills/code-repository-to-agent",
        "path": "agents/project_to_agent/SKILL.md",
        "content_sha256": "<sha256>"
      }
    ]
  },
  "updated": [
    {
      "name": "sdk_code_repository_execution",
      "owner": "sdk"
    },
    {
      "name": "maintenance",
      "owner": "sdk"
    },
    {
      "name": "a2a_communication",
      "owner": "platform"
    },
    {
      "name": "project_design",
      "owner": "platform"
    },
    {
      "name": "project_to_agent",
      "owner": "platform"
    }
  ]
}
```

The schema-2 `PINNED_FROM.txt` retains the schema-1 compatibility fields
(`library_name`, `namespace`, `pinned_version`, `skills_path`,
`copied_at_utc`, and `command`) and adds `installed_at_utc`, the `sdk_*`
fields, `platform_source_url`, `platform_retrieved_at_utc`, platform
manifest/ontology identity, `platform_resource_count`,
`platform_skill_count`, and one `platform_resource.<name>.*` group for the
ontology and each installed platform skill.

## Troubleshooting

- Run `mainsequence doctor` to check config, auth visibility, and tool availability.
- If a command says not logged in, run `mainsequence login` again.
- `mainsequence login` persists tokens for later CLI runs. Use `--export` only when you explicitly want shell-managed auth variables instead.
- `mainsequence skills list` lists installed scaffold skills from the current CLI installation by recursively discovering `SKILL.md` files under the installed `agent_scaffold` bundle.
- `mainsequence skills path` with no argument prints the installed `agent_scaffold/skills` directory for the current CLI installation.
- `mainsequence skills path <skill_name>` prints the installed `SKILL.md` path for one scaffold skill from the current CLI installation. It accepts full relative skill names such as `data_publishing/meta_tables` and unique leaf names such as `meta_tables`.
- `mainsequence user` shows the authenticated MainSequence account through `User.get_authenticated_user_details()`.
- in standalone authenticated CLI or script code that is not request-bound, prefer `User.get_authenticated_user_details()` over `User.get_logged_user()`. `User.get_logged_user()` is for SDK-bound identity contexts such as Streamlit or code that explicitly binds `_CURRENT_AUTH_HEADERS`; FastAPI handlers use the platform-populated `request.state.user` instead.
- `mainsequence code-repository search "<QUERY>"` searches visible projects through the SDK client `Project.quick_search()` path and returns `uid` and `code_repository_name` for matching rows.
- `mainsequence code-repository search` requires at least 3 query characters. The backend matches `code_repository_name` by substring and also matches an exact public project UID.
- `mainsequence organization teams list` lists teams through the SDK client `Team.filter()` path.
- `mainsequence organization teams create`, `edit`, and `delete` use the SDK client `Team.create()`, `Team.patch()`, and `Team.delete()` paths.
- `mainsequence organization teams can_view` and `can_edit` inspect team access through the SDK `Team.can_view()` and `Team.can_edit()` paths.
- `mainsequence organization teams add_to_view`, `add_to_edit`, `remove_from_view`, and `remove_from_edit` mutate explicit user access on teams through the SDK `Team` permission-action paths.
- `mainsequence agent list` and `search` require `--environment-uid` to scope discovery to one Organization Environment. This is read context and does not assign an environment to an Agent.
- `mainsequence agent list`, `detail`, `create`, and `delete` use the SDK client `mainsequence.client.agent_runtime_models.Agent` paths.
- `mainsequence agent session list` and `detail` use the SDK client `mainsequence.client.agent_runtime_models.AgentSession` path.
- `mainsequence agent session list --agent-uid <AGENT_UID>` lists sessions for one agent directly.
- `mainsequence agent session get_or_create <AGENT_UID> --session-uid <SESSION_UID>` resolves one existing session through `POST /api/v1/agents/{agent_uid}/sessions/get-or-create-session/`.
- `mainsequence agent session get_or_create <AGENT_UID> --handle-unique-id <HANDLE>` gets or creates a reusable session handle through `POST /api/v1/agents/{agent_uid}/sessions/get-or-create-session/`.
- `mainsequence agent session get_or_create` sends exactly one lookup key: either `session_uid` or `handle_unique_id`. Creation options such as `--name`, `--parent-session-uid`, `--llm-provider`, `--llm-model`, and `--llm-thinking` are valid only with `--handle-unique-id`.
- Agent session list, detail, and get-or-create responses expose the backend-owned, read-only `runtime_capabilities` version map. Callers may inspect advertised capabilities but must not send or override them.
- In runtime A2A allocation, `--parent-session-uid` proves the immediate calling Agent. The backend, not the CLI, copies the parent session's User owner into the child session and handle. That User owns provider credentials across the chain; provider credentials are never forwarded in A2A content.
- `mainsequence agent session a2a send <SESSION_UID> --message "..."` resolves runtime access internally, sends a standard A2A message, and always returns the standard A2A JSON response.
- `mainsequence agent session a2a send <SESSION_UID> --message "..." --strict-dictionary` requests a strict JSON dictionary using the standard A2A output contract.
- `mainsequence agent session a2a send <SESSION_UID> --message "..." --message-id <MESSAGE_ID>` preserves A2A request identity across a caller-approved retry. Direct Message sends are not durably replay-safe and must not be retried automatically after an ambiguous timeout. If a send fails after the CLI generated an id, the CLI prints the id to reuse if the caller elects to retry.
- `mainsequence agent can_view` and `can_edit` inspect agent sharing through the SDK `ShareableObjectMixin` access-state paths on `Agent`.
- `mainsequence agent add_to_view`, `add_to_edit`, `remove_from_view`, and `remove_from_edit` mutate explicit user access on agents through the SDK `ShareableObjectMixin` permission-action paths.
- `mainsequence agent add_team_to_view`, `add_team_to_edit`, `remove_team_from_view`, and `remove_team_from_edit` mutate explicit team access on agents through the SDK `ShareableObjectMixin` team-action paths.
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
- `mainsequence time-index-table list` lists time-index tables through the SDK client `TimeIndexMetaTable.filter()` path.
- `mainsequence time-index-table list --show-filters` prints the filters exposed by `TimeIndexMetaTable.FILTERSET_FIELDS` and the expected value shapes from `FILTER_VALUE_NORMALIZERS`.
- `mainsequence time-index-table list --filter namespace=...` is the first-class CLI form for narrowing time-index tables by storage namespace.
- `mainsequence time-index-table list --data-source-uid <DATA_SOURCE_UID>` is the first-class shortcut for the canonical `data_source__uid` filter.
- `mainsequence time-index-table list` and `mainsequence meta-table list` derive the required Organization Environment scope from the process-frozen, Git-resolved CodeRepositoryBranch. They do not accept an Environment selector; an unregistered branch fails only when this table context is required.
- `mainsequence time-index-table search` is the public semantic discovery command for time-index tables and MetaTable metadata. It uses `TimeIndexMetaTable.description_search()` against `/api/v1/time-index-meta-tables/description-search/?q=<text>`.
- `mainsequence time-index-table search --data-source-uid <DATA_SOURCE_UID>` narrows semantic discovery results by data source.
- `mainsequence time-index-table search --trigram-k 200 --embed-k 200 --w-trgm 0.65 --w-emb 0.35` tunes description-search ranking.
- `mainsequence time-index-table list --filter KEY=VALUE` and `mainsequence time-index-table list --show-filters` are the structured filtering path. Do not treat list filters as semantic discovery.
- `mainsequence time-index-table search --mode column` uses `TimeIndexMetaTable.column_search()` for schema or column-name lookup. Do not use it as the default dataset discovery path.
- `mainsequence time-index-table detail` fetches one storage through `TimeIndexMetaTable.get()` and renders its configuration in the terminal, including the backend-derived `storage_layout` and `physical_index_plan` when the source table configuration exposes them.
- `mainsequence time-index-table run_query` executes `TimeIndexMetaTable.run_query()` against one storage uid and prints the backend query envelope.
- `mainsequence meta-table run_query` executes `MetaTable.run_query()` against one MetaTable uid and prints the backend query envelope. The SDK sends raw SQL as a JSON string body, not as `{ "sql": ... }`.
- `mainsequence time-index-table refresh-search-index` calls the SDK instance method `TimeIndexMetaTable.refresh_table_search_index()` for one storage and prints the backend response in the terminal.
- `mainsequence time-index-table add-label` and `remove-label` mutate `TimeIndexMetaTable` labels through the SDK `LabelableObjectMixin` path. Labels are organizational metadata only and do not affect runtime behavior or functionality.
- `mainsequence code-repository search "<QUERY>"` is the first-class CLI command for finding existing projects before creation or local setup. Use it for fuzzy discovery, then use `mainsequence code-repository validate-name "<PROJECT_NAME>"` for the exact create-time availability check.
- `mainsequence code-repository validate-name "<PROJECT_NAME>"` validates a candidate project name through the SDK client `Project.validate_name()` path, prints normalized names and suggestions, and exits non-zero when the name is unavailable.
- `mainsequence code-repository update AGENTS.md` is project-scoped. It resolves the target project first, then reads `AGENTS.md` from the running CLI's installed `agent_scaffold` bundle. This command does not require the target project's `.venv`. If the target file is missing, it creates it from that installed bundle. If an existing `AGENTS.md` has no Main Sequence managed marker, the command replaces the whole file. If the managed marker exists, the command updates only that managed block.
- `mainsequence code-repository update-agent-skills` is project-scoped and dual-source. In one invocation it resolves SDK-owned execution skills from the target project's installed `agent_scaffold/skills/` bundle, uses the existing platform JWT to initialize `/mcp`, discovers the server-owned resource catalog through paginated `resources/list`, reads the ontology and its dynamically declared `skill_resources` through `resources/read`, validates the complete platform manifest revision and every generic resource/content rule, rejects SDK/platform destination collisions, stages the deterministically ordered combined tree, and replaces only `.agents/skills/mainsequence/`. It writes one schema-2 `.agents/skills/mainsequence/PINNED_FROM.txt` containing the installed SDK version/source path and the independent platform manifest version/hash, ontology hash, resource URIs, resource paths, and content hashes. A failed update preserves the previous managed tree and sentinel. It does not copy bundle-root files such as `AGENTS.md`, does not package platform content in the SDK, and does not modify project-owned skills outside `.agents/skills/mainsequence/`.
- `mainsequence time-index-table can_view` lists users returned by the SDK `ShareableObjectMixin.can_view()` path for `TimeIndexMetaTable`.
- `mainsequence time-index-table can_edit` lists users returned by the SDK `ShareableObjectMixin.can_edit()` path for `TimeIndexMetaTable`.
- `mainsequence time-index-table add_to_view`, `add_to_edit`, `remove_from_view`, and `remove_from_edit` mutate time-index-table user sharing through the SDK `ShareableObjectMixin` paths and render the resulting permission state in the terminal.
- `mainsequence time-index-table add_team_to_view`, `add_team_to_edit`, `remove_team_from_view`, and `remove_team_from_edit` mutate time-index-table team sharing through the SDK `ShareableObjectMixin` team-action paths.
- `mainsequence time-index-table delete` executes the SDK client `TimeIndexMetaTable.delete()` path and exposes the same delete flags as the client: `full_delete_selected`, `full_delete_downstream_tables`, `delete_with_no_table`, and `override_protection`.
- `mainsequence time-index-table delete` always requires typed verification before the delete call is sent.
- `mainsequence code-repository images list` lists project images using the SDK client `CodeRepositoryImage.filter()` path.
- `CodeRepositoryImage` responses include backend metadata such as `creation_date` and the required boolean `build_error` build-status flag.
- All list commands share the same `--filter KEY=VALUE` and `--show-filters` pattern. Commands that already enforce scoping filters reject overriding those keys.
- `mainsequence code-repository images create` only accepts pushed commits for `code_repository_commit_hash`. If omitted, it lists commits from the current branch upstream (or remote refs as fallback), shows which commits already have image ids, and waits until `is_ready=true` by polling every 30 seconds for up to 5 minutes by default.
- `mainsequence code-repository jobs list` lists project jobs through the SDK client `Job.filter()` path.
- `mainsequence code-repository jobs list` shows a human-readable schedule summary from `task_schedule`.
- `mainsequence code-repository time-index-table-updates list` lists persisted table
  updates through `CodeRepositoryBranch.get_time_index_table_updates()`.
- `mainsequence code-repository add-label` and `remove-label` mutate `Project` labels through the SDK `LabelableObjectMixin` path. Labels are organizational metadata only and do not affect runtime behavior or functionality.
- `mainsequence code-repository can_view` lists users returned by the SDK `ShareableObjectMixin.users_can_view()` path for `Project`.
- `mainsequence code-repository can_edit` lists users returned by the SDK `ShareableObjectMixin.users_can_edit()` path for `Project`.
- `mainsequence code-repository add_to_view`, `add_to_edit`, `remove_from_view`, and `remove_from_edit` mutate project user sharing through the SDK `ShareableObjectMixin` paths and render the resulting permission state in the terminal.
- `mainsequence code-repository add_team_to_view`, `add_team_to_edit`, `remove_team_from_view`, and `remove_team_from_edit` mutate project team sharing through the SDK `ShareableObjectMixin` team-action paths.
- `mainsequence code-repository resources list` lists project resources through the SDK client `CodeRepositoryResource.filter()` path and always applies `repo_commit_sha` from the current upstream branch head.
- `mainsequence code-repository current` reports the logical Project UID, current named Git branch, exact commit, resolved CodeRepositoryBranch UID, and branch-resolution status. Local and deployed code resolve the same Git worktree context; `.env` and runtime environment variables do not supply Project or branch identity.
- `mainsequence code-repository sync` is the canonical local release workflow. Its preflight maps the canonical Git repository, attached branch, and exact HEAD commit to CodeRepositoryBranch and rejects detached or unregistered checkouts. With `--dry-run`, it uses `uv version --bump patch --dry-run`, requests the backend-owned tag for that future version, rejects an invalid or existing local tag, prints the complete plan, and returns before SSH key generation, private remote access, dependency changes, or Git mutations. A normal run establishes the forced SSH identity, rejects the exact tag if it already exists on `origin`, applies and verifies the patch version, runs `uv lock`, runs `uv sync`, exports locked production requirements, commits, creates the returned annotated tag, and atomically pushes the explicit branch and tag refs with `--follow-tags`. The backend returns a stable tag on `main` and a branch-qualified tag on every other branch. Backend repository reconciliation is triggered independently by the GitHub branch-push webhook; there is no client post-commit callback.
- `mainsequence code-repository jobs runs list` lists job-run history through the SDK client `JobRun.filter(job__uid=job_uid)` exact-filter path. Multi-job callers can use `job__uid__in` with a list.
- `mainsequence code-repository jobs runs logs` fetches canonical owner-scoped logs through `JobRun.get_logs()`, follows opaque pagination cursors, polls JobRun status separately every 30 seconds while the run is `PENDING` or `RUNNING`, and stops after 10 minutes unless you override `--max-wait-seconds` or disable it with `--max-wait-seconds 0`.
- `mainsequence code-repository jobs runs resource-usage` fetches aggregate CPU, memory, and disk usage through `JobRun.get_resource_usage()`.
- `mainsequence code-repository resources logs` and `resource-usage` inspect runtime-backed ResourceReleases without exposing service, revision, or pod identities.
- `mainsequence agent logs` and `resource-usage` inspect Agent-owned runtime telemetry; `agent logs --agent-session-uid` narrows logs to an authorized session.
- `mainsequence agent session logs` is fixed to the AgentSession in the command path and does not accept a session override.
- `mainsequence code-repository jobs run` triggers a manual run through the SDK client `Job.run_job()` path.
- `mainsequence code-repository jobs run --arg ...` appends per-run args to the saved job entrypoint; it does not replace the saved `execution_path` or `app_name`.
- `mainsequence code-repository jobs run -- --name demo-from-cli` is the preferred form when an appended arg itself starts with `-`.
- `mainsequence code-repository jobs create` creates jobs through the SDK client `Job.create()` path. A manually pinned Job requires one exact ready `--related-image-uid`. An automatically deployed Job uses `--automatic-deployment` and must omit the image UID; the backend derives and prepares the exact initial image from the CodeRepositoryBranch's persisted synchronized commit. Use `--automatic-redeployment-tag-regex` to restrict later qualifying tags. The command expects `execution_path` relative to the content root, for example `scripts/test.py`, builds interval or crontab schedules interactively when requested, and defaults compute settings to `cpu_request=0.25`, `memory_request=0.5`, `spot=false`, `max_runtime_seconds=86400` when omitted.
- `mainsequence code-repository jobs list` reports each job's exact image, commit, readiness, automatic-deployment state, and effective tag policy.
- `mainsequence code-repository jobs runs list` reports the immutable runtime image UID, digest, and commit snapshot used by each run.
- Repository-managed Jobs and ResourceReleases use backend-owned declarations
  under `.mainsequence/workflows/`. The removed `schedule_batch_jobs` command
  and `scheduled_jobs.yaml` format are not compatibility surfaces. Retrieve the
  current CodeRepositoryBranch workflow template, validate the file through the
  backend, commit it, and inspect the repository-event result after push.
