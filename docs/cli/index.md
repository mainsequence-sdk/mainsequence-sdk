# Main Sequence CLI

The SDK CLI manages authentication and backend endpoint settings. Its current commands are:

```text
mainsequence login [BACKEND] [CODE_REPOSITORIES_BASE] [--backend URL] [--no-open | --mcp | --access-token TOKEN --refresh-token TOKEN]
mainsequence logout
mainsequence settings show
mainsequence settings set-backend URL
mainsequence settings reset
mainsequence version
mainsequence doctor [--check-connection]
```

`login` opens a browser by default and persists the returned JWT pair in the CLI auth store. `--no-open` prints the authorization URL. `--mcp` waits for an existing authenticated MCP principal to approve the printed `auth.cli_authorize` handoff. Runtime credential mode exchanges the backend-injected credential for an access token. `logout` attempts backend revocation when a refresh token exists and clears local tokens.

`doctor` reports the configured endpoint, auth visibility, and token-store location. Add `--check-connection` for a five-second backend probe. It does not reveal token values.

The previous MetaTables, table-update, migrations, CodeRepository deployment, Docker/SSH, agent workflow, and bundled-skill commands were removed. See [the migration guide](../migrations/metatables-sdk-removal.md).


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
