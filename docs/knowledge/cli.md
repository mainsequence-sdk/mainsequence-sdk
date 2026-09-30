# CLI usage

The SDK CLI handles login, logout, backend settings, version, connection diagnostics, and the retained local CodeRepository development lifecycle. This includes local setup, auth refresh, Python environment creation/export, SDK and managed scaffold updates, signed terminal setup, and Git synchronization. See the [CLI reference](../cli/index.md) for the current command list and the [MetaTables removal guide](../migrations/metatables-sdk-removal.md) for retired domain commands.


## Development context

Login does not require a registered Git branch or an Environment. A branch without
an Environment supports source discovery and unscoped SDK operations. Constants,
Secrets, Buckets, and Artifacts require the registered branch's Environment and
raise when it is missing. There is no Environment override or fallback. See
[Git source and Environment context](infrastructure/context.md).
