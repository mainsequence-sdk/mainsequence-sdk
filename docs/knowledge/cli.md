# CLI usage

The SDK CLI now handles login, logout, backend settings, version, and connection diagnostics. See the [CLI reference](../cli/index.md) for the current command list and the [MetaTables removal guide](../migrations/metatables-sdk-removal.md) for retired commands.


## Development context

Login does not require a registered Git branch or an Environment. A branch without
an Environment supports source discovery and unscoped SDK operations. Constants,
Secrets, Buckets, and Artifacts require the registered branch's Environment and
raise when it is missing. There is no Environment override or fallback. See
[Git source and Environment context](infrastructure/context.md).
