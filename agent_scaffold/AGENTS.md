# AGENTS.md

You are a dual-mandate agent. Follow the repository-specific instructions in this file and the
relevant skills, while also keeping in mind that application surfaces, data, and implementation
operate within the Main Sequence platform and must follow Main Sequence platform instructions.

## Repository-Specific Instructions

[ HERE SHOULD BE THE REPOSITORY-SPECIFIC ACTIONS, RULES, CONTEXT, AND LOCAL NOTES. DO NOT REMOVE
THIS LINE UNLESS YOU REPLACE IT WITH REAL REPOSITORY-SPECIFIC CONTENT. ]

Do not remove the `<!-- mainsequence-agent-scaffold:start schema=1 source=agent_scaffold -->`
or `<!-- mainsequence-agent-scaffold:end -->` markers. `mainsequence code-repository update AGENTS.md`
uses them to update only the Main Sequence section below.


<!-- mainsequence-agent-scaffold:start schema=1 source=agent_scaffold -->
## Main Sequence Instructions

Use this managed section to select the correct Main Sequence skill. Detailed
procedures belong in those skills and their referenced documentation.

## Authority

- Repository-specific instructions above this block define local intent and
  repository conventions.
- The installed SDK code, CLI help, SDK-owned skills, and version-matched SDK
  documentation define client behavior.
- Installed platform-owned skills and backend-advertised schemas, templates,
  and capabilities define the current platform contract.
- The public documentation site is supplemental when its SDK version differs
  from the installed package:
  `https://mainsequence-sdk.github.io/mainsequence-sdk/`
- If the installed client and platform contract disagree, do not guess or
  update automatically. Use the bug-auditor skill to record the evidence and
  identify the owning side.

## Updates Are Explicit

Do not run any of these commands unless the user explicitly requests that
specific update:

- `mainsequence code-repository update-sdk --path .`
- `mainsequence code-repository update-agent-skills --path .`
- `mainsequence code-repository update-platform-skills --path .`
- `mainsequence code-repository update AGENTS.md --path .`
- `uv run ms-tau skills sync --path .`

A missing or mismatched `.agents/skills/mainsequence/PINNED_FROM.txt` is state
to report, not permission to mutate the repository. The same rule applies to
`.agents/skills/ms_tau_sdk/PINNED_FROM.txt`. When a Main Sequence update is
requested, use the `code_repository_maintenance` skill. The `mainsequence` CLI
never owns or updates the `ms_tau_sdk` namespace.

The SDK owns all of `.agents/skills/mainsequence/`. Its skill copy is local,
requires no sign-in, and deletes everything absent from the installed SDK
bundle. Platform skills are installed separately under
`.agents/skills/mainsequence_platform/` by the authenticated platform update.

## Route By Task

- Product architecture, platform ontology, and CodeRepository Blueprint work:
  use the matching platform-owned design skill declared by the installed
  platform catalog. Do not assume its filesystem path.
- CodeRepository context, local SDK execution, repository structure, and
  implementation routing:
  `.agents/skills/mainsequence/sdk_code_repository_execution/SKILL.md`
- Local environment repair, authentication, explicitly requested updates,
  dependency sync after a dependency change, and publishing with Git:
  `.agents/skills/mainsequence/maintenance/code_repository_maintenance/SKILL.md`
- Blocker analysis, failure classification, and SDK/platform contract mismatches:
  `.agents/skills/mainsequence/maintenance/bug_auditor/SKILL.md`
- MetaTable modeling, queries, external registration, table updates, Alembic
  schema changes, and data discovery: use the skills the `metatables` package
  installs under `.agents/skills/metatables/` and its documentation.
- Code that still imports `mainsequence.meta_tables` or
  `mainsequence.client.metatables`, or calls the retired `mainsequence` table
  and migration commands:
  `.agents/skills/mainsequence/maintenance/metatables_transition/SKILL.md`
- FastAPI APIs serving the Command Center frontend:
  `.agents/skills/mainsequence/application_surfaces/api_surfaces/SKILL.md`
- Jobs, schedules, images, resources, releases, and Artifacts:
  `.agents/skills/mainsequence/platform_operations/orchestration_and_releases/SKILL.md`
- RBAC, sharing, constants, secrets, and access verification:
  `.agents/skills/mainsequence/platform_operations/access_control_and_sharing/SKILL.md`
- Direct recorded model calls, structured extraction, thinking/provider options,
  inference history and idempotent replay:
  `.agents/skills/mainsequence/mainsequence-inference/SKILL.md`
- A2A session discovery, messages, files, and SDK response handling:
  `.agents/skills/mainsequence/a2a_sdk_execution/SKILL.md`
- Turning a CodeRepository into a platform coding agent or selecting other
  platform-owned capabilities: use the matching skill declared by the installed
  platform catalog. Do not hardcode a platform-owned skill path.
- Implementing or debugging a TAU-based Harness Agent: after the platform skill
  establishes the platform contract, use the version-matched skills under
  `.agents/skills/ms_tau_sdk/`. If they are absent and the user requested the
  update, run `uv run ms-tau skills sync --path .`; do not reconstruct those
  SDK instructions from platform documentation.

## Core Working Rules

- Read the relevant skill before acting on a Main Sequence domain.
- Use the current Git checkout as repository, branch, and commit identity. Do
  not ask users to inject CodeRepositoryBranch or Environment identity.
- Use the `mainsequence` CLI as the default platform control surface. Prefer
  `--json` when output will be parsed or used as evidence.
- Use the owning domain package before designing against an unfamiliar domain contract.
- Keep reusable business logic under `src/` and keep API, job, and other
  integration layers thin.
- Verify only the platform objects relevant to the requested outcome.
- Separate verified facts from assumptions and report blockers with the exact
  failing operation.

## Completion

Before implementation, identify the requested end state and the evidence that
will prove it. Do not claim completion until the relevant code or documentation
checks pass and any required platform state has been verified. If live
verification is unavailable, state what remains unverified.
<!-- mainsequence-agent-scaffold:end -->
