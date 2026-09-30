# Main Sequence CLI

The SDK CLI manages authentication, backend settings, and local CodeRepository development workflows. Its current commands are:

```text
mainsequence login [BACKEND] [CODE_REPOSITORIES_BASE] [--backend URL] [--no-open | --mcp | --access-token TOKEN --refresh-token TOKEN]
mainsequence logout
mainsequence settings show
mainsequence settings set-backend URL
mainsequence settings reset
mainsequence version
mainsequence doctor [--check-connection]
mainsequence code-repository set-up-locally CODE_REPOSITORY_UID [--branch BRANCH]
mainsequence code-repository refresh-token [--path PATH]
mainsequence code-repository build-local-venv [--path PATH] [--recreate]
mainsequence code-repository freeze-env [--path PATH]
mainsequence code-repository update-sdk [--path PATH] [--dry-run]
mainsequence code-repository update AGENTS.md [--path PATH]
mainsequence code-repository update-agent-skills [--path PATH]
mainsequence code-repository open-signed-terminal [--path PATH]
mainsequence code-repository sync MESSAGE [--path PATH] [--dry-run]
```

`login` opens a browser by default and persists the returned JWT pair in the CLI auth store. `--no-open` prints the authorization URL. `--mcp` waits for an existing authenticated MCP principal to approve the printed `auth.cli_authorize` handoff. Runtime credential mode exchanges the backend-injected credential for an access token. `logout` attempts backend revocation when a refresh token exists and clears local tokens.

`doctor` reports the configured endpoint, auth visibility, and token-store location. Add `--check-connection` for a five-second backend probe. It does not reveal token values.

The CodeRepository commands cover cloning and local auth provisioning, Python
environment creation/export, SDK dependency updates, managed scaffold updates,
repository-specific SSH access, and Git version/commit/tag/push synchronization.
`sync --dry-run` runs read-only preflight and does not create or register an SSH
key. `update-agent-skills` installs SDK-owned and backend-advertised platform
skills into `.agents/skills/mainsequence` with a provenance sentinel.

The previous MetaTables, table-update, migrations, Docker/deployment, and agent
workflow commands were removed. See [the migration guide](../migrations/metatables-sdk-removal.md).


## Development context

Login does not require a registered Git branch or an Environment. A branch without
an Environment supports source discovery and unscoped SDK operations. Constants,
Secrets, Buckets, and Artifacts require the registered branch's Environment and
raise when it is missing. There is no Environment override or fallback. See
[Git source and Environment context](../knowledge/infrastructure/context.md).

Login preserves `--code-repositories-base` / `--base-folder` and `--export` /
`--export-env`; logout preserves both export spellings. Login exports the existing
`MAINSEQUENCE_AUTH_MODE` with its tokens. SDK import bootstraps missing endpoint and
credentials from the checkout/CLI configuration without replacing explicit values.

Commands operating on an existing checkout use the actual Git worktree and the
single process-frozen CodeRepository context. They do not accept an Environment
selector or runtime branch override. `set-up-locally --branch` only selects the
branch to clone before that checkout exists.
