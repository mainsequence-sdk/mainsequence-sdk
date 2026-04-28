# AGENTS.md

This is the bundled Main Sequence agent scaffold bootstrap.

Project-root `AGENTS.md` files without this managed marker are legacy/unmanaged.
The CLI replaces unmarked files with the current bootstrap. Once the marker is
present, the CLI updates only the managed block delimited below.

<!-- mainsequence-agent-scaffold:start schema=1 source=agent_scaffold -->
## Main Sequence Agent Scaffold

This block is managed by `mainsequence project update AGENTS.md`.

For Main Sequence work in this repository:

1. Start with `.agents/skills/project_builder/SKILL.md`.
2. Route domain work to the relevant skill under `.agents/skills/`.
3. Use `.agents/skills/maintenance/local_journal/SKILL.md` after material changes.

Refresh reusable scaffold instructions with:

`mainsequence project update_agent_skills --path .`
<!-- mainsequence-agent-scaffold:end -->

The durable Main Sequence behavior lives in the scaffold skills under
`agent_scaffold/skills/` and should be refreshed into projects with
`mainsequence project update_agent_skills --path .`.
