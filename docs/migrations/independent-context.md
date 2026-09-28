# Independent Git source and optional platform context

ADR 0035 adds independent local source discovery and defers missing Environment
errors until a scoped operation. It preserves branch-derived Environment semantics
and existing SDK authentication.

| Existing use | Behavior |
| --- | --- |
| Read local Git facts | Use `get_git_source_context()` without authentication or platform lookup. |
| Read platform branch metadata | Keep `get_code_repository_context()`. Missing registration or Environment is a valid result. |
| Environment resources from a registered branch | Existing calls use that branch's Environment and normal backend authorization. |
| Environment resources from a branch without an Environment | The operation raises `CodeRepositoryEnvironmentContextRequiredError`. No override or fallback is available. |
| Branch-owned operation on an unregistered branch | The existing registered-branch prerequisite still applies. |
| Change checkout or branch registration | Start a fresh process, as with the existing process snapshot. |

Missing Environment errors retain the existing
`CodeRepositoryBranchContextRequiredError` / `CodeRepositoryContextError` hierarchy. Catch `CodeRepositoryEnvironmentContextRequiredError`
when handling that specific prerequisite.

CLI credential bootstrap, credential provider selection, and authentication remain
unchanged. Context lookup adds no `/users/me/` revalidation or Organization ownership
preflight. [The context guide](../knowledge/infrastructure/context.md) describes
the operation boundaries and retained runtime target checks.
