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

- `MAIN_SEQUENCE_BACKEND_URL` overrides the configured backend URL.
- `MAINSEQUENCE_ACCESS_TOKEN` and `MAINSEQUENCE_REFRESH_TOKEN` can be used to provide JWT auth for the current process.

For the full authentication model, including runtime credential auth and request-bound auth, see [Authentication](infrastructure/auth.md).

When `MAINSEQUENCE_AUTH_MODE=runtime_credential`, `mainsequence login` exchanges the configured runtime credential instead of opening browser login. Use `mainsequence login --export` if the current shell needs the exchanged `MAINSEQUENCE_ACCESS_TOKEN`.

## Quickstart

```bash


mainsequence login

mainsequence project list
mainsequence project set-up-locally 123
mainsequence project open-signed-terminal 123

# Project operations
mainsequence project add-label 123 --label rates --label research
mainsequence data-node add-label 42 --label curated
mainsequence simple_table add-label 41 --label reference-data
mainsequence cc workspace add-label 7 --label trading

# Compile environment
mainsequence project freeze-env --path .
# writes requirements.txt using uv export

# Sync project (commit + push workflow)
mainsequence project sync -m "Update deps" --path .
# runs: uv version bump, uv lock/sync, export requirements, git commit + push

# Docker environment build
mainsequence project build-docker-env --path .
# builds via docker buildx and writes .devcontainer/devcontainer.json

# Current project status
mainsequence project current --debug

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
