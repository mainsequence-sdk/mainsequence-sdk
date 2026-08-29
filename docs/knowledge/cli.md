# MainSequence CLI

This CLI mirrors key functionality from the MainSequence VS Code extension:
- Login / logout
- CodeRepository list + setup locally
- Signed terminal support
- Compile environment (`uv export`)
- Sync code-repository workflow (bump, lock/sync/export, git commit/push)
- Docker environment build + devcontainer config
- Current code repository detection
- SDK version status + update
- Diagnostics (`doctor`)

## Installation

Install the `mainsequence-sdk` package (whatever your internal process is).

## Configuration

The CLI stores config and tokens in a platform-specific directory:

- **Windows:** `%APPDATA%\\MainSequenceCLI`
- **macOS:** `~/Library/Application Support/MainSequenceCLI`
- **Linux:** `~/.config/mainsequence`

### Environment overrides

- `MAINSEQUENCE_ENDPOINT` overrides the configured backend URL.
- `MAINSEQUENCE_ACCESS_TOKEN` and `MAINSEQUENCE_REFRESH_TOKEN` can be used to provide JWT auth for the current process.

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

`mainsequence code-repository set-up-locally` and `mainsequence code-repository refresh-token`
are auth-mode aware. In a backend-launched runtime credential process they
preserve the injected auth mode, credential id/secret, and an exchanged
`MAINSEQUENCE_ACCESS_TOKEN` in the CodeRepository `.env`; they do not require or write
`MAINSEQUENCE_REFRESH_TOKEN`. Both commands preserve unrelated `.env` entries
and do not carry obsolete `MAINSEQUENCE_TOKEN` or
`MAIN_SEQUENCE_PROJECT_ID` entries into the rendered file. They never write a
CodeRepositoryBranch UID, repository branch, Organization Environment UID, or another
caller-selected deployed runtime context.

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
mainsequence time-index-table add-label <TIME_INDEX_META_TABLE_UID> --label curated

# Compile environment
mainsequence code-repository freeze-env --path .
# writes requirements.txt using uv export

# Sync CodeRepository (commit + push workflow)
mainsequence code-repository sync -m "Update deps" --path .
# first maps the Git repository, attached branch, and exact commit to CodeRepositoryBranch;
# detached, unregistered, or mismatched CodeRepository assertions fail before mutations
# then requests the backend-owned branch tag and runs lock/sync/export/commit/push

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

---

## Notes on packaging

Because these changes introduce new modules, ensure package discovery includes `mainsequence/cli/*.py` in your build config.

## Labels

Several CLI object groups expose `add-label` and `remove-label`.

Those commands mutate organizational metadata only. Labels are useful for grouping and discovery, but they do not change runtime behavior or functionality.
