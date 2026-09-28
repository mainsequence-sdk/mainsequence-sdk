# Independent Git source and Environment context

ADR 0035 separates local source discovery from optional platform registration and
adds authorized development Environment selection. It is implemented on
`metatables_removal`; consumers need this local SDK checkout until a release
containing these interfaces is published.

| Existing use | Migration |
| --- | --- |
| Read Git facts through `get_code_repository_context()` | Use `get_git_source_context()` to avoid authentication and directory lookup. Keep the original function when platform branch metadata is needed. |
| Environment resources from a registered branch | Existing calls keep their branch default; platform resource requests enforce access in the backend. |
| Environment resources on an unregistered branch | Configure `DevelopmentEnvironmentSelection` before the first scoped operation. The resource clients and transport are unchanged. |
| Catch missing Environment as `CodeRepositoryBranchContextRequiredError` | Catch `CodeRepositoryEnvironmentContextRequiredError` explicitly, or their common `CodeRepositoryContextError` base. The Environment error is no longer a branch-error subclass. |
| Retry a failed directory lookup by restarting | A restart still works; `retry_failed_context_resolution()` also clears failed optional lookups while preserving source and any established scope. |
| Change Git branch or account in a long-lived developer process | Stop existing work, call `reset_development_context()`, and configure scope again, or start a fresh process. |

Configuration is through Python, not a new environment variable or CLI flag.
Per-resource Environment overrides remain unsupported. A selection conflicting
with a registered branch fails; runtime scope cannot be overridden. Branch-owned
operations retain their real branch prerequisites.

Platform-context and Environment resolution perform an authenticated identity request on each call
and a public Environment detail lookup on first resolution. This detects account
changes and verifies scope; offline consumers needing only source facts should
use `get_git_source_context()` instead.

See [Git source and Environment context](../knowledge/infrastructure/context.md)
for examples, failure behavior, reset semantics, and runtime constraints. The
SDK package boundary remains defined by [ADR 0034](../adr/0034-remove-metatables-from-sdk.md).
