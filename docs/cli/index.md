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
reason is a session that another version of the CLI saved, which is not read
and which `mainsequence login` replaces, or a locked Keychain.

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
mainsequence code-repository update AGENTS.md
mainsequence code-repository update AGENTS.md --path .
mainsequence code-repository update-agent-skills
mainsequence code-repository update-agent-skills --path .

# 4) After changing dependencies: uv lock, uv sync, export requirements.txt
mainsequence code-repository sync
mainsequence code-repository sync --path .
# then commit and push uv.lock and requirements.txt with git

# 5) Docker/devcontainer
mainsequence code-repository build-docker-env --path .

# 6) SDK maintenance
mainsequence code-repository sdk-status --path .
mainsequence code-repository update-sdk --path .
```

`sync` changes local files only. In the project root (`--path`, default the
current directory) it runs `uv lock`, `uv sync`, and the locked runtime export
to `requirements.txt` (development dependencies are excluded). It makes no
request to the platform, creates no SSH or deploy key, changes no version, and
runs no Git command. Commit and push with Git as usual. Whether a push deploys
is set by `tag_regex` in `.mainsequence/workflows/*.yaml`: when it is omitted,
every push deploys; when it is a regular expression, a matching tag deploys the
commit it points at, whether that is the branch's latest commit or an older
commit on the branch. Release tags come from the
repository's own CI; see
[Deploy on every push or on release tags](../knowledge/infrastructure/scheduling_jobs.md#deploy-on-every-push-or-on-release-tags).

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

### Updating SDK-owned skills

`mainsequence code-repository update-agent-skills --path <CODE_REPOSITORY>`
copies only the target checkout's installed `agent_scaffold/skills` bundle.
It requires neither a signed-in session nor a reachable backend. The SDK owns
the entire `.agents/skills/mainsequence/` namespace: every file and folder not
shipped by that installed version is removed. This is a replacement, not a
merge with old skills or platform content.

The copy is staged and atomic, and its `PINNED_FROM.txt` records the installed
library's `pinned_version` and source path. Source checkouts, overlapping paths,
and destinations escaping the checkout are protected. Failed replacement
restores the previous namespace. Other libraries' skill folders are untouched.

After an SDK upgrade, refresh both SDK-owned skills and the managed Main
Sequence instructions when the user requests those updates:

```bash
uv run --offline mainsequence code-repository update-agent-skills --path .
uv run --offline mainsequence code-repository update AGENTS.md --path .
```

The first command deletes retired skills, including the old table workflow
folders. The second updates the Main Sequence managed block in `AGENTS.md`;
text outside that block is preserved. Without managed markers, the existing
`update AGENTS.md` command replaces the file from the template.

`update-sdk` upgrades the dependency and reports a missing or mismatched skill
pin. It does not silently modify skills or `AGENTS.md`. Use the refreshed
checkout's CLI, not an older global CLI installation. Supplying
`--code-repository-uid` requests a platform identity assertion and may require
backend access; omit it for a local copy.

Use `--json` to report `sdk`, `pinned_version`, `destination_root`,
`sentinel_path`, and the installed `updated` entries.

### Updating platform-owned skills

Platform skills have a separate namespace and authenticated command:

```bash
mainsequence code-repository update-platform-skills --path .
```

This discovers the backend's catalog through authenticated MCP resources,
validates the ontology declarations, manifest identity, URI/name/path/front
matter, and content hashes, then atomically replaces only
`.agents/skills/mainsequence_platform/`. The SDK-owned `mainsequence`
namespace and other libraries are untouched. The platform sentinel records the
catalog's original source, retrieval time, and manifest/resource hashes.

Existing mixed installations are not retained in the SDK namespace. Refresh
SDK skills to remove those entries, then explicitly refresh platform skills
into their own namespace. A platform refresh failure does not block local SDK
skill copying.

The backend owns catalog membership; SDK constants do not pin concrete skill
names or list order. Missing, duplicate, undeclared, unsafe, or inconsistent
resources fail before installation. Platform content is never vendored in the
SDK. The `--json` result reports `platform`, the destination, and the installed
entries.



## Troubleshooting
