# AGENTS.md

Follow the repository-specific instructions in this file and load the relevant
Main Sequence skill when the task touches the platform or SDK.

## Repository-Specific Instructions

[ REPLACE THIS LINE WITH REPOSITORY-SPECIFIC RULES, CONTEXT, AND COMMANDS. ]

Do not remove the `<!-- mainsequence-agent-scaffold:start schema=1 source=agent_scaffold -->`
or `<!-- mainsequence-agent-scaffold:end -->` markers. Scaffold tooling may use
them to update only the managed section.

<!-- mainsequence-agent-scaffold:start schema=1 source=agent_scaffold -->
## Main Sequence Instructions

### Authority

- Repository instructions define local intent and conventions.
- Installed SDK code, SDK-owned skills, and version-matched SDK documentation
  define Python client behavior.
- Installed platform-owned skills and backend-advertised schemas define current
  platform workflows and policy.
- Domain packages such as `metatables` own their own contracts and skills. Do
  not infer those contracts from this SDK scaffold.
- When client and platform contracts disagree, preserve the evidence and use
  the bug-auditor skill. Do not invent compatibility behavior.

### Route By Task

- SDK authentication, Git source context, direct platform adapters, and local
  implementation routing:
  `.agents/skills/mainsequence/sdk_code_repository_execution/SKILL.md`
- CLI login, endpoint configuration, diagnostics, and explicitly requested SDK
  dependency updates:
  `.agents/skills/mainsequence/maintenance/code_repository_maintenance/SKILL.md`
- Failure classification and SDK/backend contract mismatches:
  `.agents/skills/mainsequence/maintenance/bug_auditor/SKILL.md`
- FastAPI endpoints serving a Command Center frontend:
  `.agents/skills/mainsequence/application_surfaces/api_surfaces/SKILL.md`
- Jobs, runs, Artifacts, resources, releases, and deployment observations:
  `.agents/skills/mainsequence/platform_operations/orchestration_and_releases/SKILL.md`
- Teams, sharing, constants, secrets, and access verification:
  `.agents/skills/mainsequence/platform_operations/access_control_and_sharing/SKILL.md`
- Agent discovery, sessions, runtime access, and handoff to the runtime protocol:
  `.agents/skills/mainsequence/a2a_sdk_execution/SKILL.md`
- MetaTable migration providers, Alembic revisions, approved execution, and
  migration recovery:
  `.agents/skills/mainsequence/data_publishing/meta_table_migrations/SKILL.md`
- MetaTable modeling, queries, external registration, and table updates: use
  the installed `metatables` package skills and documentation.
- Architecture, ontology, CodeRepository workflow declarations, deployment
  policy, and other platform-owned workflows: use the matching skill from the
  authenticated platform catalog.

### Working Rules

- Read only the skills relevant to the request.
- The current Git checkout is source identity. Never ask a user to inject a
  CodeRepositoryBranch or Organization Environment selector.
- The core `mainsequence` CLI is an authentication and diagnostics surface. Do
  not assume retired deployment, table, agent-message, or scaffold commands.
- Use direct typed SDK adapters for retained platform API operations. Let the
  backend enforce permissions, defaults, and lifecycle policy.
- Do not update SDK dependencies or installed skills unless the user explicitly
  requests that update.
- Separate verified facts from assumptions and report the exact failed
  operation when blocked.

### Completion

Define observable completion before implementation. Verify the relevant local
checks and platform state before claiming success; identify anything that could
not be verified live.
<!-- mainsequence-agent-scaffold:end -->
