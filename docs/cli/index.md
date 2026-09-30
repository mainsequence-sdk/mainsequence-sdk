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
mainsequence login --access-token "$TOKEN" --refresh-token "$REFRESH" --backend http://127.0.0.1:80 --code-repositories-base mainsequence-dev
mainsequence logout
```

Backend/base-folder overrides passed to `login` are terminal-session only. They do not rewrite the persisted CLI settings for other terminals.
When no backend is provided, `mainsequence login` targets the currently configured backend shown by `mainsequence doctor`. An explicit `--backend` takes precedence; the standard production backend is used only when no other backend is configured.

By default, `mainsequence login` persists the session in the operating system
credential store, one record per backend: the login Keychain on macOS, Secret
Service on Linux, and Credential Manager on Windows. A session saved from one
CodeRepository is read from every other one on the machine. If no credential
store is available, the CLI keeps the session only in the process environment
and reports that persistence was unavailable; it does not write new plaintext
token files.

A CodeRepository `.env` holds the backend endpoint and no credential. See
[ADR 0037](../adr/0037-machine-session-in-the-os-credential-store.md).

You only need `--export` if you explicitly want shell-managed environment variables.
`--export` cannot be combined with `--mcp`.

### Renewing the session

```bash
mainsequence refresh-token
```

`mainsequence refresh-token` renews the saved session and says whether it works.
It takes no path, because the session is one record per backend on the machine
and no CodeRepository holds a copy. It renews the access token from the refresh
token, or from the runtime credential of a platform runtime, saves it, and
reports the backend, the user and the session's expiry. It prints no token
value, asks nothing and opens no browser. `--json` prints the session report of
`mainsequence auth status` plus `removed_env_entries`.

When the directory it runs in has a `.env` with an access token, a refresh
token or a runtime credential that an earlier version or another tool left
there, the command removes those entries and names them. A tool that loads that
file would otherwise use them instead of the saved session. Nothing else in the
file changes.

It exits `0` when the session was renewed, `1` when there is no session, the
credential store could not be read, or the backend refused the session, and `3`
when the machine has no credential store and the environment carries no
credentials. After `1`, run `mainsequence login`.

### Handing the session to another local tool

```bash
mainsequence auth token --json
mainsequence auth status
mainsequence auth status --check --json
```

`mainsequence auth token` prints a short-lived access token for the session this
process would use: credentials set in the environment, otherwise the saved
session. It renews the token first when it would expire within a minute. It
never prints the refresh token, asks nothing and opens no browser.

With `--json` the output is one object:

| Field | Meaning |
| --- | --- |
| `endpoint` | The backend the token is for |
| `access_token` | The token to send as `Authorization: Bearer ...` |
| `token_type` | Always `Bearer` |
| `expires_at` | Expiry in epoch seconds, or `null` when the token carries none |

Without `--json` the access token alone is printed. A caller keeps the token in
memory until shortly before `expires_at` and runs the command again after that,
or after a `401`.

| Exit code | Meaning |
| --- | --- |
| `0` | A token was printed |
| `1` | There is no session, the credential store could not be read, or the backend did not renew the session. Run `mainsequence login` |
| `3` | This machine has no credential store and the environment carries no credentials |

`mainsequence auth status` reports whether a usable session exists, for which
backend and user, where it is stored, whether its credentials came from the
environment or from the saved session, and when it expires. It prints no token
value. Without `--check` the answer comes from the tokens' own expiry and needs
no network; `--check` also asks the backend. It exits `0` when a usable session
exists and `1` when it does not.

A credential store that cannot be read looks like a machine that is not logged
in. `auth status` therefore reports the reason in `store_error` (`null` when the
store was read), and `mainsequence doctor` shows it as well. On macOS the usual
reason is a locked Keychain, or a Keychain entry that belongs to another
program, which `mainsequence login` replaces.

`mainsequence login --mcp` is for a coding agent that already has an
authenticated Main Sequence MCP connection. The CLI creates PKCE state and a
challenge, asks the configured backend to create a short-lived handoff, and
prints the exact `auth.cli_authorize` tool invocation. The backend returns the
callback URI; the CLI does not create a localhost callback for this flow.
After the MCP tool authorizes the handoff, the backend returns the normal
tracked JWT pair directly to the waiting CLI, which persists it in the same
credential storage used by browser login. Tokens never pass through the MCP
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
mainsequence agent list --json
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
mainsequence user
mainsequence settings show
mainsequence sdk latest
```

## CodeRepository Commands

```bash
mainsequence code-repository --help
```

Most frequently used flows:

Agents are created by Django when a CodeRepository branch reconciles a
`harness_agent` workflow declaration with a valid indexed
`.agents/agent_card.json`. The SDK does not create an Agent directly; the card's
name and description become the Agent's display identity. `agent list` supports
UID and exact `name` filters, plus broad text search; it does not support an
`agent_type` filter. Names need not be unique across branches.

```bash
# Agents
mainsequence agent list --filter name=SentinelExecutor
mainsequence agent search "data research copilot"
mainsequence agent detail e0e75693-4110-464c-93e0-82c7fd9c9a23
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
mainsequence code-repository create tutorial-repository
mainsequence code-repository create tutorial-repository --default-base-image-uid <base_image_uid> --github-org-uid <github_org_uid>
mainsequence code-repository jobs list
mainsequence code-repository jobs runs list <JOB_UID>
mainsequence code-repository jobs runs logs <JOB_RUN_UID>
mainsequence code-repository jobs runs logs <JOB_RUN_UID> --max-wait-seconds 900
mainsequence code-repository jobs run <JOB_UID>
mainsequence code-repository jobs run <JOB_UID> --arg demo-from-cli
mainsequence code-repository jobs run <JOB_UID> -- --name demo-from-cli
mainsequence code-repository resources list
mainsequence code-repository resources list --show-filters
mainsequence code-repository resources list --filter resource_type=fastapi
mainsequence code-repository resources delete_fastapi <RELEASE_UID>
mainsequence code-repository resources delete_fastapi <RELEASE_UID> --yes
mainsequence code-repository validate-name "Rates Platform"

# 2) Set up locally
mainsequence code-repository set-up-locally <CODE_REPOSITORY_UID>

# 3) Environment setup
mainsequence code-repository build-local-venv
mainsequence code-repository build-local-venv --path .
mainsequence code-repository build-local-venv --path . --recreate
mainsequence code-repository freeze-env --path .
# exports the locked runtime closure; development dependencies are excluded
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

`set-up-locally` writes `.env` with the backend endpoint and no credential.
There is no per-checkout token command: the session belongs to the machine, and
`mainsequence refresh-token` renews it from any directory.

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
  - `mainsequence code-repository images list` always scopes by the selected code repository.
  - `mainsequence code-repository resources list` always scopes by CodeRepositoryBranch and upstream remote `repo_commit_sha`.
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
```

### Updating CodeRepository agent skills

`mainsequence code-repository update-agent-skills --path <CODE_REPOSITORY>` performs one
dual-source update:

1. it resolves SDK-owned execution skills from the target code repository's installed
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
the previous managed tree and sentinel. It never changes repository-owned skills
outside `.agents/skills/mainsequence/`, and it is not run implicitly when an
agent starts.

Use `--json` for the machine-readable result. Existing top-level compatibility
fields remain, while `sdk`, `platform`, and each `updated[].owner` identify the
two independent sources:

```json
{
  "code_repository": "/code-repository",
  "library_name": "mainsequence",
  "namespace": "mainsequence",
  "pinned_version": "5.0.0",
  "sdk": {
    "library_name": "mainsequence",
    "version": "5.0.0",
    "skills_path": "/code-repository/.venv/lib/pythonX.Y/site-packages/agent_scaffold/skills"
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
        "name": "code_repository_design",
        "uri": "mainsequence://platform/skills/code-repository-design",
        "path": "skills/platform/code_repository_design/SKILL.md",
        "content_sha256": "<sha256>"
      },
      {
        "name": "code_repository_to_agent",
        "uri": "mainsequence://platform/skills/code-repository-to-agent",
        "path": "skills/agents/code_repository_to_agent/SKILL.md",
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
        "name": "code_repository_design",
        "uri": "mainsequence://platform/skills/code-repository-design",
        "path": "platform/code_repository_design/SKILL.md",
        "content_sha256": "<sha256>"
      },
      {
        "name": "code_repository_to_agent",
        "uri": "mainsequence://platform/skills/code-repository-to-agent",
        "path": "agents/code_repository_to_agent/SKILL.md",
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
      "name": "code_repository_design",
      "owner": "platform"
    },
    {
      "name": "code_repository_to_agent",
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
