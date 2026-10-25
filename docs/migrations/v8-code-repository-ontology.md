# SDK 8.0 CodeRepository Ontology Cutover

Main Sequence SDK 8.0 requires the matching canonical backend. The repository
domain consists of `CodeRepository`, `CodeRepositoryBranch`, and
`GitHubRepositoryBinding`; no former model, route, field, filter, CLI group,
structured-output key, runtime discriminator, import, or alias is accepted.

## Required Consumer Changes

1. Deploy the canonical backend and SDK as one coordinated release.
2. Use `CodeRepository` for the governed logical repository.
3. Use `CodeRepositoryBranch` for every exact branch execution context.
4. Use `GitHubRepositoryBinding` only for GitHub provider integration state.
5. Use `code_repository_uid` and `code_repository_branch_uid` explicitly; they
   are different identities and are never interchangeable.
6. Use the `code-repository` CLI group and `--code-repositories-base` option.
7. Update structured-output consumers to read the canonical repository fields.

New local setup uses
`<base>/<organization>/projects/<checkout>`. Existing checkouts are
ordinary Git repositories and may be moved deliberately; the SDK does not
search superseded managed-directory conventions.

The migration scaffold generates `CodeRepositoryAlembicVersion`. Existing
authored migration registries continue to use the class name recorded in their
own immutable history, while regenerated examples and new scaffolds use the
canonical name.

After a direct package-manager upgrade, refresh the SDK-owned skills and their
instructions. Platform skills have a separate authenticated update:

```bash
mainsequence code-repository update-agent-skills --path .
mainsequence code-repository update-platform-skills --path .
```

Repository-owned `AGENTS.md` files are preserved. The SDK distributes its
instructions inside the skills bundle.

## Source Context Verification

The SDK resolves source context once per process from the canonical Git remote,
attached branch, and exact HEAD commit. The backend maps that source to one
`CodeRepository` and one `CodeRepositoryBranch`. An unregistered local branch
can still be used for ordinary local development; operations requiring a
registered branch, its Organization Environment, or its MetaTables DataSource
fail closed.

Callers do not select branch or Environment identity through compatibility
environment variables. Verify the cutover from the checkout:

```bash
mainsequence code-repository current --debug --json
```

The result must contain a resolved `code_repository_uid` and
`code_repository_branch_uid` before branch-owned operations are invoked.

## Unrelated Vocabulary

Python packaging `[project]`, `pyproject.toml`, and
`UV_PROJECT_ENVIRONMENT` are external standards and are unchanged.
