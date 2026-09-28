# CLI usage

The SDK CLI now handles login, logout, backend settings, version, and connection diagnostics. See the [CLI reference](../cli/index.md) for the current command list and the [MetaTables removal guide](../migrations/metatables-sdk-removal.md) for retired commands.


## Development context

Login does not require a registered Git branch. To use Environment-owned SDK
resources from an unregistered development branch, configure the typed Python
`DevelopmentEnvironmentSelection` before the first scoped operation. This
process-local selection is separate from CLI login; no CLI flag or persisted
Environment setting is added. See [Git source and Environment context](infrastructure/context.md).
