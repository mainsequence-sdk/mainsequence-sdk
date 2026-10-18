---
name: mainsequence-sdk-code-repository-execution
description: Use the installed Main Sequence SDK and CLI for Git-derived CodeRepository context, version-matched contract authority, and selecting the relevant SDK or domain-package skill. Use for repository integration and SDK execution questions.
---

# Main Sequence SDK CodeRepository Execution

Use this skill for SDK execution and CodeRepository integration. Follow the
repository's own instructions, including `AGENTS.md` when present. A repository
does not need that file to use the SDK or its skills, and the SDK does not
create, copy, update, or remove it.

## Contract Authority

- Installed SDK code, CLI help, SDK skills, and version-matched documentation
  define client behavior.
- Installed platform skills and backend-advertised schemas, templates, and
  capabilities define the platform contract.
- Use the owning domain package and its installed skills before implementing
  against an unfamiliar domain contract.
- Treat the public SDK documentation as supplemental when its version differs
  from the installed package:
  `https://mainsequence-sdk.github.io/mainsequence-sdk/`
- When the client and platform disagree, record the exact failing operation
  and evidence with the bug-auditor skill. Do not guess or upgrade automatically.

## Select The Relevant Skill

Use skill descriptions to select the domain involved in the request. This skill
is not a mandatory first step for maintenance, audits, or already-scoped domain
work. Use the installed platform catalog to select platform-owned
CodeRepository-to-Agent setup and other platform capabilities.

- Environment repair, authentication, explicitly requested updates, dependency
  sync, and Git publishing:
  `.agents/skills/mainsequence/maintenance/code_repository_maintenance/SKILL.md`
- Failure classification and SDK/platform contract mismatches:
  `.agents/skills/mainsequence/maintenance/bug_auditor/SKILL.md`
- FastAPI APIs serving Command Center, including MCP tools and skills:
  `.agents/skills/mainsequence/application_surfaces/api_surfaces/SKILL.md`
- Jobs, schedules, images, resources, releases, and Artifacts:
  `.agents/skills/mainsequence/platform_operations/orchestration_and_releases/SKILL.md`
- RBAC, sharing, constants, secrets, and acting for a requester:
  `.agents/skills/mainsequence/platform_operations/access_control_and_sharing/SKILL.md`
- Recorded model calls, structured extraction, provider options, history, and replay:
  `.agents/skills/mainsequence/mainsequence-inference/SKILL.md`
- A2A session discovery, messages, files, and SDK response handling:
  `.agents/skills/mainsequence/a2a_sdk_execution/SKILL.md`
- Retired SDK MetaTables imports or commands:
  `.agents/skills/mainsequence/maintenance/metatables_transition/SKILL.md`
- Current MetaTable modeling, queries, table updates, and Alembic changes:
  use the installed `metatables` package's skills and documentation.
- TAU Harness Agent implementation: use the version-matched skills under
  `.agents/skills/ms_tau_sdk/` after the relevant platform skill establishes
  its contract. If those skills are missing, report that; run
  `uv run ms-tau skills sync --path .` only when the user requests that update.

## Resolve CodeRepository Context From Git

The containing Git worktree is the only source of repository, attached branch,
and exact commit identity. The SDK normalizes the non-secret repository remote,
maps it to CodeRepository and CodeRepositoryBranch through the platform API,
resolves once for the process, and reuses the immutable result. Authentication
selects credentials and permissions; it does not select source identity.
A `git switch` in a long-running process takes effect in the next CLI invocation,
worker, script, or other process run.

For a live branch-owned operation, use
`mainsequence code-repository current --debug --json` to verify that Git context
resolves to `code_repository_branch_status=resolved` and a nonempty
`code_repository_branch_uid`. The UID is an internal resolution result, not a
local configuration input. Never ask the user to look it up, persist repository
identity in `.env`, accept a branch environment override, or infer a branch
from collection order. Local and deployed images use the same Git algorithm.

A detached checkout or an unregistered Git branch remains usable for ordinary
local development but has no resolved CodeRepositoryBranch or Environment.
Only operations requiring that context fail. Register the branch before
using branch-owned Jobs, images, releases, resources, pods, or other platform
APIs. Never fall back to another branch.

Keep these platform identities distinct:

- Git-resolved CodeRepository UID: aggregate identity and repository operations.
- CodeRepositoryBranch: resolved by the SDK when branch-owned APIs require it.
- GitHubRepositoryBinding: repository metadata and clone-location ownership;
  `git_ssh_url` is not CodeRepositoryBranch state.

## Use The Installed CLI

Use `mainsequence` as the default platform control surface for SDK-supported
operations. Prefer `--json` when parsing output or collecting evidence. Check
installed command help when documentation and local behavior differ.

Before live platform checks, inspect the saved session with
`mainsequence auth status --check` when needed. Authentication belongs to the
machine, not to a checkout. The SDK reads the saved operating-system credential
store session; credentials must not be copied into repository instructions.

Verify only the platform objects relevant to the requested outcome. Separate
verified facts from assumptions, and state any live checks that could not be
performed.

## SDK Skills And Repository Files

The SDK owns `.agents/skills/mainsequence/`. Its copier installs SDK guidance
and domain skills together from the target repository's installed package,
removes retired files, and writes `PINNED_FROM.txt` with the installed version.
SDK skill copying needs no sign-in or platform catalog.

An explicit `update-sdk` also refreshes SDK skills after installing the package.
Other skill refreshes require the user's request; a stale or missing sentinel
is a condition to report, not permission to update. Use the maintenance skill
for the update workflow.

Platform skills belong to `.agents/skills/mainsequence_platform/` and use the
authenticated platform updater. Other libraries, including `metatables` and
`ms-tau-sdk`, own their independent namespaces and update commands.

Follow the repository's existing layout. The SDK does not require `src/`,
`scripts/`, `tests/`, `docs/`, or `api/` directories merely because it is
installed. When recurring Jobs or releases use backend-managed workflow
declarations, keep their YAML files as direct children of
`.mainsequence/workflows/` and validate the current backend workflow contract.

Repository-local execution paths passed to platform Jobs must be relative to
the repository root, use forward slashes on every operating system, and refer
to supported files inside the checkout.
