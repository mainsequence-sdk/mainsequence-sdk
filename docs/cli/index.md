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

Every request the CLI sends to the backend, from login, logout and session
renewal to the session check of each authenticated command, has the SDK's
default request budget unless the command sets its own: 5 seconds to connect,
120 seconds to read, 125 seconds in total. A backend that does not answer in
time ends the command with an error, for example
`The backend at <url> could not be reached (ReadTimeout).`, instead of leaving
it waiting.

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

A logout that revokes the login session also ends, within at most 60 seconds, the
[application tokens](../knowledge/infrastructure/resource_releases.md#who-can-use-a-release)
obtained with that session.

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
mainsequence access policies
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

`agent session a2a send --timeout N` is one total budget of `N` seconds for the
send: waiting for the Agent runtime to become ready, the request and the
Agent's response. Without it the send's budget is `(5, 900)`, 905 seconds in
total. A runtime that is still starting or waking when the budget can no
longer fit the platform's next retry ends the command with an error, and
nothing is sent. Resolving a handle first is a separate request with the same
budget. The value can only shorten a wait; it cannot extend the platform's
limits. In particular, the platform answers a direct Message (the default
`--response-kind message`) within its Message limit, 90 seconds; when the Agent
does not answer in time it stops the turn and the command fails with an error
that says so, without retrying. Send longer work with `--response-kind task`
when the Agent advertises it.

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
mainsequence agent access list e0e75693-4110-464c-93e0-82c7fd9c9a23
mainsequence agent access grant e0e75693-4110-464c-93e0-82c7fd9c9a23 user --user <USER_UID>
mainsequence agent access revoke e0e75693-4110-464c-93e0-82c7fd9c9a23 <BINDING_UID>
mainsequence agent access explain e0e75693-4110-464c-93e0-82c7fd9c9a23 agents.use
mainsequence agent delete e0e75693-4110-464c-93e0-82c7fd9c9a23
mainsequence constants list
mainsequence constants list --show-filters
mainsequence constants create APP__MODE production
mainsequence constants create ASSETS__MASTER '{"dataset":"bloomberg"}'
mainsequence constants access list <CONSTANT_UID>
mainsequence constants access grant <CONSTANT_UID> reader --user <USER_UID>
mainsequence constants access revoke <CONSTANT_UID> <BINDING_UID>
mainsequence constants access explain <CONSTANT_UID> constants.update
mainsequence constants delete <CONSTANT_UID>
mainsequence secrets list
mainsequence secrets list --show-filters
mainsequence secrets create API_KEY super-secret-value
mainsequence secrets access list <SECRET_UID>
mainsequence secrets access grant <SECRET_UID> value_reader --user <USER_UID>
mainsequence secrets access revoke <SECRET_UID> <BINDING_UID>
mainsequence secrets access explain <SECRET_UID> secrets.read_value
mainsequence secrets delete <SECRET_UID>
mainsequence code-repository search tutorial
mainsequence organization github-organizations
mainsequence organization teams list
mainsequence organization teams list --show-filters
mainsequence organization teams create Research --description "Model validation"
mainsequence organization teams edit <TEAM_UID> --name "Research Core" --inactive
mainsequence organization teams members list <TEAM_UID>
mainsequence organization teams members add <TEAM_UID> <USER_UID>
mainsequence organization teams members remove <TEAM_UID> <USER_UID>
mainsequence organization teams leave <TEAM_UID>
mainsequence organization teams access list <TEAM_UID>
mainsequence organization teams access grant <TEAM_UID> viewer --user <USER_UID>
mainsequence organization teams access revoke <TEAM_UID> <BINDING_UID>
mainsequence organization teams access explain <TEAM_UID> organization-teams.update
mainsequence organization teams delete <TEAM_UID>

# 1) List and create
mainsequence code-repository list
mainsequence code-repository add-label <CODE_REPOSITORY_UID> --label rates --label research
mainsequence code-repository remove-label <CODE_REPOSITORY_UID> --label legacy
mainsequence code-repository access list <CODE_REPOSITORY_UID>
mainsequence code-repository access grant <CODE_REPOSITORY_UID> reader --user <USER_UID>
mainsequence code-repository access revoke <CODE_REPOSITORY_UID> <BINDING_UID>
mainsequence code-repository access explain <CODE_REPOSITORY_UID> code-repositories.update
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
mainsequence code-repository jobs access list <JOB_UID>
mainsequence code-repository jobs access grant <JOB_UID> runner --user <USER_UID>
mainsequence code-repository jobs access revoke <JOB_UID> <BINDING_UID>
mainsequence code-repository jobs access explain <JOB_UID> jobs.run
mainsequence code-repository resources list
mainsequence code-repository resources list --show-filters
mainsequence code-repository resources list --filter resource_type=fastapi
mainsequence code-repository resources delete_fastapi <RELEASE_UID>
mainsequence code-repository resources delete_fastapi <RELEASE_UID> --yes
mainsequence code-repository resources access list <RELEASE_UID>
mainsequence code-repository resources access grant <RELEASE_UID> user --user <USER_UID>
mainsequence code-repository resources access revoke <RELEASE_UID> <BINDING_UID>
mainsequence code-repository resources access explain <RELEASE_UID> resource-releases.invoke
mainsequence code-repository validate-name "Rates Platform"

# 2) Set up locally
mainsequence code-repository set-up-locally <CODE_REPOSITORY_UID>

# 3) Environment setup
mainsequence code-repository build-local-venv
mainsequence code-repository build-local-venv --path .
mainsequence code-repository build-local-venv --path . --recreate
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

The committed `requirements.txt` decides the repository's dependency image. The
platform builds it once for each content of the file and reuses it for every
commit until the file changes, and deployed code runs from the repository's
files at `/workspace`; the repository's own package is not installed. See
[Dependencies and the deployed image](../knowledge/cli.md#dependencies-and-the-deployed-image).

`set-up-locally` writes `.env` with the backend endpoint and no credential.
There is no per-checkout token command: the session belongs to the machine, and
`mainsequence refresh-token` renews it from any directory.

During `set-up-locally`, the CLI registers a new or inaccessible deploy key through
`/api/v1/code-repositories/{code_repository_uid}/add-deploy-key/` and verifies repository access with the forced
identity before cloning. Registration or access failure stops setup. Repository branch selection
only chooses the branch to clone; it is not deploy-key ownership. Adding the deploy key needs the
repository's `add_deploy_key` action: repository `editor` or `owner`, or `editor` of one of its
branches, held directly, through a team or through a Project.

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

## Access Policies

```bash
mainsequence access policies
mainsequence access policies --type secrets
mainsequence access policies show secrets.reader
mainsequence access policies show <POLICY_UID>
mainsequence access policies --type secrets --json
```

`mainsequence access policies` lists the policies the platform defines: each
policy's id, label, who it can be given to and summary. `--type` limits the
list to one resource type, for example `secrets`. `show` prints one policy,
given by its id or its UID: what a holder can do, what can happen because of
it, what it does not include and its actions. Both commands read the access
catalog, which every signed-in user can read, and accept `--json` and
`--timeout`; neither changes who has access. See
[Access Policy Catalog](../knowledge/infrastructure/access_catalog.md).

## Teams

```bash
mainsequence organization teams create Research --description "Model validation"
mainsequence organization teams members list <TEAM_UID>
mainsequence organization teams members add <TEAM_UID> <USER_UID>
mainsequence organization teams members remove <TEAM_UID> <USER_UID>
mainsequence organization teams leave <TEAM_UID>
mainsequence organization teams access list <TEAM_UID>
mainsequence organization teams access grant <TEAM_UID> membership_manager --user <USER_UID>
mainsequence organization teams access grant <TEAM_UID> viewer --team <OTHER_TEAM_UID>
mainsequence organization teams access revoke <TEAM_UID> <BINDING_UID>
mainsequence organization teams access explain <TEAM_UID> organization-teams.update
mainsequence organization teams access explain <TEAM_UID> organization-teams.add_member --user <USER_UID>
```

A team's membership is who is in it; who controls it is the policies on it.
Any person can create a team with `create`, and becomes its owner and its first
member.

- `members list` prints each member's UID, kind (`person` or `workload`),
  email and name, people first. `members add` adds a person or a workload
  identity; adding a member again changes nothing. `members remove` removes a
  member. `leave` ends your own membership; a policy you hold on the team, such
  as `owner`, stays.
- `access list` prints the policies attached to the team, each with its
  binding UID, policy id, principal and source, and the access the platform
  computes: Organization admins hold every action, members hold
  `organization-teams.member`, and each holder of the full-directory team's
  `viewer` policy, named in the table, holds `viewer`. It needs the team's
  `read_access` action.
- `access grant` gives a policy, by name such as `viewer` or by id such as
  `organization-teams.viewer`, to a person or workload identity with `--user`
  or to a team with `--team`; pass exactly one. It needs the team's
  `manage_access` action and every action of the policy.
  `mainsequence access policies --type organization-teams` lists the policies.
- `access revoke` detaches a binding by the UID `access list` shows. Removing
  your own binding is always allowed.
- `access explain` says whether you, or the User given with `--user`, may
  perform an action, every path that gives it, and the policies that contain
  it.

Every command accepts `--json` and `--timeout`. With `--json`, `members remove`,
`leave` and `access revoke` print what they did, for example
`{"team_uid": "...", "binding_uid": "...", "detached": true}`. A refused
command prints the platform's message and the policies that would allow it.
See [Who Has Access to an Object](../knowledge/infrastructure/object_access.md)
and [Users and Access](../knowledge/infrastructure/users_and_access.md#teams).

## Constants

```bash
mainsequence constants access list <CONSTANT_UID>
mainsequence constants access grant <CONSTANT_UID> reader --user <USER_UID>
mainsequence constants access grant <CONSTANT_UID> editor --team <TEAM_UID>
mainsequence constants access revoke <CONSTANT_UID> <BINDING_UID>
mainsequence constants access explain <CONSTANT_UID> constants.update
mainsequence constants access explain <CONSTANT_UID> constants.read --user <USER_UID>
```

Who can use a constant is decided by its policies: `reader` sees it and its
value; `editor` also changes its name and value, deletes it and sees who has
access; `owner` also decides who has access. The person or workload that
creates a constant holds `owner`.

- `access list` prints the policies attached to the constant, each with its
  binding UID, policy id, principal and source, and the access the platform
  computes: Organization admins hold every action, and when a workload created
  the constant, its managers hold the creator's rights. It needs the constant's
  `read_access` action, which `editor` and `owner` hold.
- `access grant` gives a policy, by name such as `reader` or by id such as
  `constants.reader`, to a person or workload identity with `--user` or to a
  team with `--team`; pass exactly one. It needs the constant's
  `manage_access` action, which `owner` holds, and every action of the policy.
  `mainsequence access policies --type constants` lists the policies.
- `access revoke` detaches a binding by the UID `access list` shows. Removing
  your own binding is always allowed.
- `access explain` says whether you, or the User given with `--user`, may
  perform an action, every path that gives it, and the policies that contain
  it.

These commands address the constant by its UID alone: unlike `constants list`,
`create` and `delete`, they need no registered branch or Environment. Every
command accepts `--json` and `--timeout`; with `--json`, `access revoke` prints
`{"constant_uid": "...", "binding_uid": "...", "detached": true}`. A constant
you cannot see is reported as not found, and a refused command prints the
platform's message and the policies that would allow it. See
[Constants and Secrets](../knowledge/infrastructure/constants_and_secrets.md#who-can-use-a-constant)
and [Who Has Access to an Object](../knowledge/infrastructure/object_access.md).

## Secrets

```bash
mainsequence secrets access list <SECRET_UID>
mainsequence secrets access grant <SECRET_UID> value_reader --user <USER_UID>
mainsequence secrets access grant <SECRET_UID> editor --team <TEAM_UID>
mainsequence secrets access revoke <SECRET_UID> <BINDING_UID>
mainsequence secrets access explain <SECRET_UID> secrets.read_value
mainsequence secrets access explain <SECRET_UID> secrets.update_value --user <USER_UID>
```

Who can use a secret is decided by its policies: `reader` sees it, never its
value; `value_reader` also reads its current value; `editor` also reads earlier
values, sets a new value, deletes it and sees who has access; `owner` also
decides who has access. The person or workload that creates a secret holds
`owner`. A secret the platform manages, such as a hosted database password,
cannot get a new value or be deleted.

- `access list` prints the policies attached to the secret, each with its
  binding UID, policy id, principal and source, and the access the platform
  computes: Organization admins hold every action, and when a workload created
  the secret, its managers hold the creator's rights. It needs the secret's
  `read_access` action, which `editor` and `owner` hold.
- `access grant` gives a policy, by name such as `value_reader` or by id such
  as `secrets.value_reader`, to a person or workload identity with `--user` or
  to a team with `--team`; pass exactly one. A workload that uses the secret
  needs `value_reader`. It needs the secret's `manage_access` action, which
  `owner` holds, and every action of the policy.
  `mainsequence access policies --type secrets` lists the policies.
- `access revoke` detaches a binding by the UID `access list` shows. Removing
  your own binding is always allowed.
- `access explain` says whether you, or the User given with `--user`, may
  perform an action, every path that gives it, and the policies that contain
  it.

These commands address the secret by its UID alone: unlike `secrets list`,
`create` and `delete`, they need no registered branch or Environment. Every
command accepts `--json` and `--timeout`; with `--json`, `access revoke` prints
`{"secret_uid": "...", "binding_uid": "...", "detached": true}`. A secret you
cannot see is reported as not found, and a refused command prints the
platform's message and the policies that would allow it. `secrets delete` reads
the secret, value included, for its preview and then deletes it, so it needs
`editor` or `owner`; its refusals name the policies the same way. See
[Constants and Secrets](../knowledge/infrastructure/constants_and_secrets.md#who-can-use-a-secret)
and [Who Has Access to an Object](../knowledge/infrastructure/object_access.md).

## Code repositories

```bash
mainsequence code-repository access list <CODE_REPOSITORY_UID>
mainsequence code-repository access grant <CODE_REPOSITORY_UID> reader --user <USER_UID>
mainsequence code-repository access grant <CODE_REPOSITORY_UID> editor --team <TEAM_UID>
mainsequence code-repository access revoke <CODE_REPOSITORY_UID> <BINDING_UID>
mainsequence code-repository access explain <CODE_REPOSITORY_UID> code-repositories.update
mainsequence code-repository access explain <CODE_REPOSITORY_UID> code-repositories.read --user <USER_UID>
```

Who can use a code repository is decided by its policies: `reader` sees it,
its GitHub link and issues, and every branch in it; `editor` also renames or
archives it, adds deploy keys, creates branches, edits every branch and sees
who has access; `owner` also decides who has access. The repository's creator
holds `owner`. Deleting a repository is in no policy: only Organization admins
delete one. A repository's policies pass down to every branch of it.

A policy on one of the repository's branches, held directly, through a team or
through a Project, also reaches the repository without reaching its other
branches: a branch `reader` finds and sees the repository, and a branch
`editor` or `owner` also adds deploy keys to develop locally. These two
computed policies, `code-repositories.branch_reader` and
`code-repositories.branch_editor`, appear in `access list` as computed access
and cannot be granted, and they apply only while the person can work in the
branch's environment.

- `access list` prints the policies attached to the repository, each with its
  binding UID, policy id, principal and source, and the access the platform
  computes, such as the Organization admins' every action. It needs the
  repository's `read_access` action, which `editor` and `owner` hold.
- `access grant` gives a policy, by name such as `reader` or by id such as
  `code-repositories.reader`, to a person or workload identity with `--user`
  or to a team with `--team`; pass exactly one. A person needs the platform's
  `user` product role to hold one. It needs the repository's `manage_access`
  action, which `owner` holds, and every action of the policy.
  `mainsequence access policies --type code-repositories` lists the policies.
- `access revoke` detaches a binding by the UID `access list` shows. Removing
  your own binding is always allowed.
- `access explain` says whether you, or the User given with `--user`, may
  perform an action, every path that gives it, and the policies that contain
  it. For `code-repositories.delete` it names none.

These commands address the repository by its UID alone: they need no
registered branch or Environment. Every command accepts `--json` and
`--timeout`; with `--json`, `access revoke` prints
`{"code_repository_uid": "...", "binding_uid": "...", "detached": true}`. A
repository you cannot see is reported as not found, and a refused command
prints the platform's message and the policies that would allow it. The
platform refuses every code repository route, these included, to a person
without its `user` product role, and the command prints that refusal's
message. The CLI has no access commands for branches and Projects; use their
access methods. See
[Code Repositories, Branches and Projects](../knowledge/infrastructure/code_repositories.md)
and [Who Has Access to an Object](../knowledge/infrastructure/object_access.md).

## Agents

```bash
mainsequence agent access list <AGENT_UID>
mainsequence agent access grant <AGENT_UID> user --user <USER_UID>
mainsequence agent access grant <AGENT_UID> viewer --team <TEAM_UID>
mainsequence agent access revoke <AGENT_UID> <BINDING_UID>
mainsequence agent access explain <AGENT_UID> agents.use
mainsequence agent access explain <AGENT_UID> agents.read_tasks --user <USER_UID>
```

Who can use an Agent is decided by its policies: `viewer` talks to it, gives
it Tasks, sees all its Tasks and reads your own log lines; `user` talks to it
and gives it Tasks, seeing only the Tasks you asked for; `operator` also stops
any Task or session run; `editor` also configures the Agent and its runtime,
deletes it, manages what its workload can reach and sees who has access;
`owner` also decides who has access. The Agent's policies also decide its
runtime release. Turning on acting for the requester is in no policy: only
Organization admins do it.

- `access list` prints the policies attached to the Agent, each with its
  binding UID, policy id, principal and source, and the access the platform
  computes, such as the Organization admins' every action. It needs the
  Agent's `read_access` action, which `editor` and `owner` hold.
- `access grant` gives a policy, by name such as `user` or by id such as
  `agents.user`, to a person or workload identity with `--user` or to a team
  with `--team`; pass exactly one. It needs the Agent's `manage_access` action,
  which `owner` holds, and every action of the policy.
  `mainsequence access policies --type agents` lists the policies.
- `access revoke` detaches a binding by the UID `access list` shows. Removing
  your own binding is always allowed.
- `access explain` says whether you, or the User given with `--user`, may
  perform an action, every path that gives it, and the policies that contain
  it. For `agents.set_acts_for_requester` it names none.

These commands address the Agent by its UID alone: they need no registered
branch or Environment. Every command accepts `--json` and `--timeout`; with
`--json`, `access revoke` prints
`{"agent_uid": "...", "binding_uid": "...", "detached": true}`. An Agent you
cannot see is reported as not found, and a refused command prints the
platform's message and the policies that would allow it. See
[Agents](../knowledge/infrastructure/agents.md) and
[Who Has Access to an Object](../knowledge/infrastructure/object_access.md).

## Resource releases

```bash
mainsequence code-repository resources access list <RELEASE_UID>
mainsequence code-repository resources access grant <RELEASE_UID> user --user <USER_UID>
mainsequence code-repository resources access grant <RELEASE_UID> viewer --team <TEAM_UID>
mainsequence code-repository resources access revoke <RELEASE_UID> <BINDING_UID>
mainsequence code-repository resources access explain <RELEASE_UID> resource-releases.invoke
mainsequence code-repository resources access explain <RELEASE_UID> resource-releases.read_logs --user <USER_UID>
```

Who can use a release is decided by its policies: `viewer` sees its
configuration, revisions and logs and calls it, its API, site or MCP endpoint;
`user` sees it and calls it, without its logs; `editor` also changes its
configuration, activates static deployments, deletes it, manages what its
workload can reach and sees who has access; `owner` also decides who has
access. A release also takes the policies of the branch that declares it. A
release that backs an Agent follows the Agent and takes no policy of its own.

- `access list` prints the policies attached to the release and the access the
  platform computes, including what the release takes from its branch or its
  Agent. It needs the release's `read_access` action, which `editor` and
  `owner` hold.
- `access grant` gives a policy, by name such as `user` or by id such as
  `resource-releases.user`, to a person or workload identity with `--user` or
  to a team with `--team`; pass exactly one. It needs the release's
  `manage_access` action, which `owner` holds, and every action of the policy.
  On a release that backs an Agent it prints the platform's refusal: give
  access on the Agent with `mainsequence agent access grant`.
  `mainsequence access policies --type resource-releases` lists the policies.
- `access revoke` detaches a binding by the UID `access list` shows. Removing
  your own binding is always allowed.
- `access explain` says whether you, or the User given with `--user`, may
  perform an action, every path that gives it, and the policies that contain
  it. For `resource-releases.set_acts_for_requester` it names none.

These commands address the release by its UID alone: they need no registered
branch or Environment. Every command accepts `--json` and `--timeout`; with
`--json`, `access revoke` prints
`{"resource_release_uid": "...", "binding_uid": "...", "detached": true}`. A
release you cannot see is reported as not found, and a refused command prints
the platform's message and the policies that would allow it. See
[Who can use a release](../knowledge/infrastructure/resource_releases.md#who-can-use-a-release)
and [Who Has Access to an Object](../knowledge/infrastructure/object_access.md).

## Jobs

```bash
mainsequence code-repository jobs access list <JOB_UID>
mainsequence code-repository jobs access grant <JOB_UID> runner --user <USER_UID>
mainsequence code-repository jobs access grant <JOB_UID> viewer --team <TEAM_UID>
mainsequence code-repository jobs access revoke <JOB_UID> <BINDING_UID>
mainsequence code-repository jobs access explain <JOB_UID> jobs.run
mainsequence code-repository jobs access explain <JOB_UID> jobs.read_runs --user <USER_UID>
```

Who can use a Job is decided by its policies: `viewer` sees the Job, its
schedule, runs, logs and outputs; `runner` also runs it and stops its runs;
`editor` also changes and deletes it, manages what its workload can reach and
sees who has access; `owner` also decides who has access. `jobs run` needs
`runner` or more and `jobs update` `editor` or `owner`. A standalone Job also
takes the policies of its branch, and a release's backing Job those of its
release; a backing Job is changed, run and deleted with its release.

- `access list` prints the policies attached to the Job and the access the
  platform computes, including what the Job takes from its branch or its
  release. It needs the Job's `read_access` action, which `editor` and `owner`
  hold.
- `access grant` gives a policy, by name such as `runner` or by id such as
  `jobs.runner`, to a person or workload identity with `--user` or to a team
  with `--team`; pass exactly one. It needs the Job's `manage_access` action,
  which `owner` holds, and every action of the policy.
  `mainsequence access policies --type jobs` lists the policies.
- `access revoke` detaches a binding by the UID `access list` shows. Removing
  your own binding is always allowed.
- `access explain` says whether you, or the User given with `--user`, may
  perform an action, every path that gives it, and the policies that contain
  it. On a backing Job it shows `jobs.update`, `jobs.run` and `jobs.delete` as
  blocked by a platform rule.

These commands address the Job by its UID alone: they need no registered
branch or Environment. Every command accepts `--json` and `--timeout`; with
`--json`, `access revoke` prints
`{"job_uid": "...", "binding_uid": "...", "detached": true}`. A Job you cannot
see is reported as not found, and a refused command prints the platform's
message and the policies that would allow it. See
[Who can use a Job](../knowledge/infrastructure/scheduling_jobs.md#who-can-use-a-job)
and [Who Has Access to an Object](../knowledge/infrastructure/object_access.md).

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

SDK guidance and domain instructions ship together inside the skills bundle.
To explicitly refresh that bundle from the installed package:

```bash
uv run --offline mainsequence code-repository update-agent-skills --path .
```

The command deletes retired SDK skills, including the old table workflow
folders, and updates the sentinel. It never creates, copies, updates, or removes
the repository's `AGENTS.md`, including files with older managed blocks. The
SDK no longer ships an `AGENTS.md` template or a command for editing that file.

`update-sdk` upgrades the dependency, then runs the newly installed SDK's skill
updater in a fresh process against the same repository. It reports success only
after the skills and sentinel are refreshed. If refreshing fails, it exits with
an error and a retry command; the installed SDK remains upgraded. Direct `uv`
upgrades require the explicit skill refresh above. Use the refreshed
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
Each MCP request has the SDK's default request budget: 5 seconds to connect,
120 seconds to read, 125 seconds in total. A request the platform does not
answer in time ends the command with an error, and nothing is installed.

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
