# MainSequence CLI

This CLI mirrors key functionality from the MainSequence VS Code extension:
- Login / logout
- CodeRepository list + setup locally
- Signed terminal support
- Dependency sync (`uv lock`, `uv sync`, `requirements.txt` export)
- Docker environment build + devcontainer config
- Current code repository detection
- SDK version status + update
- Diagnostics (`doctor`)

## Installation

Install the `mainsequence-sdk` package (whatever your internal process is).

## Configuration

The CLI stores non-secret configuration in a platform-specific directory:

- **Windows:** `%APPDATA%\\MainSequenceCLI`
- **macOS:** `~/Library/Application Support/MainSequenceCLI`
- **Linux:** `~/.config/mainsequence`

The session is backend-scoped and persisted in the operating system credential
store when one is available: the login Keychain on macOS, Secret Service on
Linux, and Credential Manager on Windows. The CLI does not create a plaintext
token-file fallback, and it does not write a credential into a CodeRepository
`.env`. The SDK reads the saved session when it is imported, so a login made
from one CodeRepository serves every other one on the machine.

### Environment overrides

- `MAINSEQUENCE_ENDPOINT` overrides the configured backend URL.
- `MAINSEQUENCE_ACCESS_TOKEN` and `MAINSEQUENCE_REFRESH_TOKEN` can be used to provide JWT auth for the current process.

Credentials already set in the process environment win over the saved session.
When that pair is rejected, the SDK does not fall back to the saved session,
because the pair may belong to another user or backend; the error says that the
rejected credentials came from the environment. `mainsequence auth status`
shows which of the two a process is using.

### Other local tools

A tool that does not read the credential store itself asks the CLI for a
short-lived access token:

```bash
mainsequence auth token --json
```

The command never prints the refresh token. Its output and exit codes are
described in the [CLI reference](../cli/index.md#handing-the-session-to-another-local-tool).

`mainsequence login`, including `mainsequence login --mcp`, uses this resolved
configured backend unless `--backend` is supplied explicitly. The backend shown
by `mainsequence doctor` is therefore the backend that receives an implicit
login handoff.

For the full authentication model, including runtime credential auth and request-bound auth, see [Authentication](infrastructure/auth.md).

When a backend-launched process has
`MAINSEQUENCE_AUTH_MODE=runtime_credential`, `mainsequence login` exchanges the
injected runtime credential instead of opening browser login. This mode is not
a user-settable branch or runtime selector. Use `mainsequence login --export`
if that process's current shell needs the exchanged
`MAINSEQUENCE_ACCESS_TOKEN`.

When a coding agent already has an authenticated Main Sequence MCP connection,
`mainsequence login --mcp` creates a backend-owned PKCE handoff and prints the
exact `auth.cli_authorize` call. The backend supplies the callback URI and
returns the tracked JWT pair directly to the waiting CLI after approval; the
MCP tool never returns credentials. Runtime-credential mode continues to use
ordinary `mainsequence login`; `--mcp` is rejected in that mode and cannot be
combined with `--export`.

`mainsequence logout` now performs a hard CLI logout when the session came from browser-based CLI login and a refresh token is available. It revokes the tracked CLI login session server-side through `/auth/cli/revoke/`, falls back to JWT logout on older backends that do not implement that endpoint, and otherwise clears only local CLI auth state.

`mainsequence code-repository set-up-locally` writes the backend endpoint into
the CodeRepository `.env` and no credential, in every auth mode. A
backend-launched runtime credential process already has its credential in its
own environment; nothing of it is copied into the checkout. It never writes a
CodeRepositoryBranch UID, repository branch, Organization Environment UID, or another
caller-selected deployed runtime context.

`mainsequence refresh-token` renews the saved session. It is a top-level command
without a path, because the session belongs to the machine and not to a
checkout. When the directory it runs in has a `.env` with an access token, a
refresh token or a runtime credential that an earlier version or another tool
left there, it removes those entries, names them, and changes nothing else in
the file.

Local setup registers a new or inaccessible deploy key against the logical CodeRepository at
`/api/v1/code-repositories/{code_repository_uid}/add-deploy-key/` and verifies repository access with that forced
identity before cloning. The selected CodeRepositoryBranch is only the Git branch to clone and is not the
owner of repository credentials.

Repository keys use the cross-CLI filename
`~/.ssh/mainsequence-<repository-slug>-<first-16-sha256>` derived from the normalized
`host[:non-default-port]/repository/path`. Equivalent SCP and `ssh://` origins select the same key,
while repositories that only share a basename do not. Legacy basename-only files are left
untouched and are not used as a fallback.

## Quickstart

```bash


mainsequence login

mainsequence code-repository list
mainsequence code-repository set-up-locally <CODE_REPOSITORY_UID>
mainsequence code-repository open-signed-terminal <CODE_REPOSITORY_UID>

# CodeRepository operations
mainsequence code-repository add-label <CODE_REPOSITORY_UID> --label rates --label research

# After changing dependencies
mainsequence code-repository sync --path .
# runs uv lock, uv sync and the locked runtime export to requirements.txt (dev group excluded);
# no backend request and no Git command: commit and push the changed files yourself

# Docker environment build
mainsequence code-repository build-docker-env --path .
# builds via docker buildx and writes .devcontainer/devcontainer.json

# Current code repository status
mainsequence code-repository current --debug --json
# reports logical CodeRepository UID, current Git branch, resolved CodeRepositoryBranch UID,
# and branch resolution status

# SDK status and update
mainsequence code-repository sdk-status --path .
mainsequence code-repository update-sdk --path .

# Diagnostics
mainsequence doctor
```

## Commit, push and release tags

Publish changes with Git as usual: commit, then push. The platform deploys from
the push according to the repository's `.mainsequence/workflows/*.yaml`.

`mainsequence code-repository sync` is only for dependency changes. In the
project root (`--path`, default the current directory) it runs `uv lock`,
`uv sync`, and `uv export --locked --no-dev --no-hashes` into
`requirements.txt`. It sends no request to the platform, creates no SSH or
deploy key, changes no version, and runs no `git add`, `commit`, `tag` or
`push`. Review `uv.lock` and `requirements.txt`, then commit and push them with
the change.

Whether a push deploys is decided by `tag_regex` in the workflow file. When it
is omitted, every push deploys. When it is a regular expression, a matching tag
deploys the commit it points at, whether that is the branch's latest commit or
an older commit on the branch. The Main
Sequence platform does not create tag names; release tags come from the
repository's own CI. See
[Deploy on every push or on release tags](infrastructure/scheduling_jobs.md#deploy-on-every-push-or-on-release-tags)
for an example release workflow.

---

## Notes on packaging

Because these changes introduce new modules, ensure package discovery includes `mainsequence/cli/*.py` in your build config.

## Labels

Several CLI object groups expose `add-label` and `remove-label`.

Those commands mutate organizational metadata only. Labels are useful for grouping and discovery, but they do not change runtime behavior or functionality.
