# 7.x To 8.0: CodeRepository Ontology Hard Cut

Main Sequence SDK 8.0 completes the coordinated repository-domain cutover.
The governed codebase aggregate is `CodeRepository`, one exact branch context
is `CodeRepositoryBranch`, and its GitHub provider record is
`GitHubRepositoryBinding`.

This is a breaking cut. SDK 8 must run with the matching canonical backend.
The removed model names, routes, fields, filters, CLI group names, structured
output keys, and runtime discriminators are not available as aliases.

## What Changed

| Removed repository-domain name | SDK 8 name |
| --- | --- |
| `Project` | `CodeRepository` |
| `ProjectBranch` | `CodeRepositoryBranch` |
| `GitRepository` | `GitHubRepositoryBinding` |
| `ProjectImage` | `CodeRepositoryImage` |
| `ProjectResource` | `CodeRepositoryResource` |
| `project_uid` | `code_repository_uid` |
| `project_branch_uid` | `code_repository_branch_uid` |
| `project` CLI group | `code-repository` CLI group |
| `--projects-base` | `--code-repositories-base` |

New local setup uses
`<base>/<organization>/code-repositories/<checkout>`. Existing checkouts are
ordinary Git repositories and may be moved deliberately; the SDK does not
silently search the retired managed directory name.

The migration scaffold now generates `CodeRepositoryAlembicVersion` by
default. Existing authored migration registries keep working under whatever
class name their repository defines, but regenerated examples and new
scaffolds use the canonical name.

The platform-owned agent conversion skill is installed at
`.agents/skills/mainsequence/code_repository_to_agent/SKILL.md`. Refresh the
managed scaffold after upgrading:

```bash
mainsequence code-repository update-agent-skills --path .
mainsequence code-repository update AGENTS.md --path .
```

## Source Context

The SDK resolves source context once per process from the canonical Git remote,
attached branch, and exact HEAD commit. The backend maps that source to
`CodeRepository` and `CodeRepositoryBranch`. An unregistered local branch can
still be used for ordinary local development; only operations requiring a
registered branch, its Organization Environment, or its MetaTables DataSource
fail.

Callers do not supply branch or Environment identity. Retired
`MAIN_SEQUENCE_PROJECT_*` environment names are removed from rendered `.env`
files and ignored as source-selection inputs.

## Migration Checklist

1. Deploy the matching canonical backend and SDK as one coordinated release.
2. Replace removed Python imports, type names, fields, filters, and routes with
   the CodeRepository equivalents.
3. Replace CLI invocations and automation using `project` or
   `--projects-base` with `code-repository` and
   `--code-repositories-base`.
4. Update consumers of structured CLI output to read `code_repository` and
   canonical CodeRepository fields.
5. Refresh managed skills and `AGENTS.md` from the installed SDK.
6. Run `mainsequence code-repository current --debug --json` from the checkout
   and verify a resolved `code_repository_uid` and
   `code_repository_branch_uid` before branch-owned operations.
7. Run the repository's tests and live branch-owned smoke checks.

## Vocabulary That Did Not Change

Project Blueprint, the backend-owned `project-design` skill, Blueprint
`project`, and Blueprint `project_to_agent` remain product-intent vocabulary.
Python packaging `[project]`, `pyproject.toml`, and
`UV_PROJECT_ENVIRONMENT` are external standards and are also unchanged.
