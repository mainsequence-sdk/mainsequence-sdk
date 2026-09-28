# Main Sequence CLI

The SDK CLI manages authentication and backend endpoint settings. Its current commands are:

```text
mainsequence login [--backend URL] [--no-open | --mcp | --access-token TOKEN --refresh-token TOKEN]
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

Login does not require a registered Git branch. To use Environment-owned SDK
resources from an unregistered development branch, configure the typed Python
`DevelopmentEnvironmentSelection` before the first scoped operation. This
process-local selection is separate from CLI login; no CLI flag or persisted
Environment setting is added. See [Git source and Environment context](../knowledge/infrastructure/context.md).
