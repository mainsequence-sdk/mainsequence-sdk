# MainSequence CLI

This CLI mirrors key functionality from the MainSequence VS Code extension:
- Login / logout
- Project list + setup locally
- Signed terminal support
- Compile environment (`uv export`)
- Sync project workflow (bump, lock/sync/export, git commit/push)
- Docker environment build + devcontainer config
- Current project detection
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

For the full authentication model, including runtime credential auth and request-bound auth, see [Authentication](infrastructure/auth.md).

When `MAINSEQUENCE_AUTH_MODE=runtime_credential`, `mainsequence login` exchanges the configured runtime credential instead of opening browser login. Use `mainsequence login --export` if the current shell needs the exchanged `MAINSEQUENCE_ACCESS_TOKEN`.

When a coding agent already has an authenticated Main Sequence MCP connection,
`mainsequence login --mcp` creates a backend-owned PKCE handoff and prints the
exact `auth.cli_authorize` call. The backend supplies the callback URI and
returns the tracked JWT pair directly to the waiting CLI after approval; the
MCP tool never returns credentials. Runtime-credential mode continues to use
ordinary `mainsequence login`; `--mcp` is rejected in that mode and cannot be
combined with `--export`.

`mainsequence logout` now performs a hard CLI logout when the session came from browser-based CLI login and a refresh token is available. It revokes the tracked CLI login session server-side through `/auth/cli/revoke/`, falls back to JWT logout on older backends that do not implement that endpoint, and otherwise clears only local CLI auth state.

`mainsequence project set-up-locally` and `mainsequence project refresh_token` are auth-mode aware. In runtime credential mode they write `MAINSEQUENCE_AUTH_MODE=runtime_credential`, the runtime credential id/secret, and an exchanged `MAINSEQUENCE_ACCESS_TOKEN` into the project `.env`; they do not require or write `MAINSEQUENCE_REFRESH_TOKEN`. Both commands preserve unrelated `.env` entries and do not carry obsolete `MAINSEQUENCE_TOKEN` or `MAIN_SEQUENCE_PROJECT_ID` entries into the rendered file.

## Quickstart

```bash


mainsequence login

mainsequence project list
mainsequence project set-up-locally <PROJECT_UID>
mainsequence project open-signed-terminal <PROJECT_UID>

# Project operations
mainsequence project add-label <PROJECT_UID> --label rates --label research
mainsequence data-node add-label <DATA_NODE_STORAGE_UID> --label curated

# Compile environment
mainsequence project freeze-env --path .
# writes requirements.txt using uv export

# Sync project (commit + push workflow)
mainsequence project sync -m "Update deps" --path .
# first resolves the current named Git branch under MAIN_SEQUENCE_PROJECT_UID;
# detached or unregistered branches fail before local mutations
# then requests the backend-owned branch tag and runs lock/sync/export/commit/push

# Docker environment build
mainsequence project build-docker-env --path .
# builds via docker buildx and writes .devcontainer/devcontainer.json

# Current project status
mainsequence project current --debug --json
# reports logical Project UID, current Git branch, resolved ProjectBranch UID,
# and branch resolution status

# SDK status and update
mainsequence project sdk-status --path .
mainsequence project update-sdk --path .

# Diagnostics
mainsequence doctor
```

---

## Notes on packaging

Because these changes introduce new modules, ensure package discovery includes `mainsequence/cli/*.py` in your build config.

## Labels

Several CLI object groups expose `add-label` and `remove-label`.

Those commands mutate organizational metadata only. Labels are useful for grouping and discovery, but they do not change runtime behavior or functionality.
