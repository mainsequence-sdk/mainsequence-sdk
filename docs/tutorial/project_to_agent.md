# Part 6: Turn Your Project Into an Agent

## Quick Summary

In this tutorial, you will:

- understand what it means to turn a Main Sequence project into an agent-facing project
- prepare the local repository files that describe agent behavior
- connect project skills to real project capabilities instead of vague prompts
- create the metadata an agent surface needs in order to be packaged and released
- understand how this fits the same project/image/release model used elsewhere in Main Sequence

This should be the last tutorial step because it builds on everything that came before.

By this point, your project already knows how to publish data, expose APIs, schedule work, and
package reusable application surfaces. Turning the project into an agent is not a separate
unrelated idea. It is the next application surface on top of the same repository.

The practical outcome is that one project can become a coding-oriented assistant for its own
domain. That helps you compose agentic workflows where projects interact through explicit project
surfaces inside their own controlled repository and runtime environments, instead of sharing one
large unstructured prompt or one shared runtime.

## 1. What "Turn Your Project Into an Agent" Means

In Main Sequence terms, an agent-capable project is still just a project.

It still has:

- code in the repository
- documented project behavior
- a CLI or other local execution surface
- project resources and releases

What changes is that you also give the project an agent-facing layer that explains:

- what the project is supposed to do
- which tasks it can handle
- which local skills it should use
- which repository files define its behavior

That is what makes the project usable as a coding assistant instead of only as a codebase or a
collection of scripts.

## 2. Keep The Scope Honest

Do not invent capabilities.

When you describe a project as an agent, the agent description must match the repository that
actually exists. If the repository does not expose a capability through code, CLI commands,
documented skills, or confirmed user requirements, do not claim that the agent can do it.

This matters because agent projects become operational interfaces. If the description is looser
than the repository, users will ask the agent to do work it cannot actually perform.

## 3. The Required Repository Pieces

### `AGENTS.md`

The repository should contain an `AGENTS.md` file with project-specific instructions. This is the
top-level operating manual for the agent.

At minimum, it should explain:

- what this project is for
- what kinds of tasks the agent should and should not handle
- which local rules or constraints matter for this repository

Use a section named:

```text
## Project-Specific Instructions
```

That section is where you replace generic scaffolding with the real intent of the project.

If the file is missing or the managed Main Sequence block is outdated, refresh it with:

```bash
mainsequence project update AGENTS.md --path .
```
Important: do not remove the general Main Sequence instructions from `AGENTS.md`, because they
help both you and the agent understand how to work within the Main Sequence platform.

### `.agents/skills/`

The repository should contain local skills under:

```text
.agents/skills/
```

These skills are not decoration. They are the mechanism that tells the agent how to perform
project work in a repeatable way.

The important rule is simple:

- non-Main-Sequence skills should map to real project capabilities
- in practice, they should usually support or expose the project CLI and documented workflows
- skills should be CLI-based. This means that if you want the agent to update a data node, run a
  job, or do anything else, you should create a CLI under `src/cli/...` and have the skill
  definition point to that CLI

Main Sequence scaffold skills can be refreshed with:

```bash
mainsequence project update_agent_skills --path .
```

### Project CLI Capabilities

The project should have a real control surface that the agent can rely on.

The preferred pattern is a documented project CLI, often under a module such as `src/cli`, though
the exact location can vary by project. The important thing is not the folder name. The important
thing is that the project exposes concrete commands the agent can execute or reason about.

That is what turns the agent from a free-form chat wrapper into a reliable operator for the
project.

### `.agents/agent_card.json`

The repository should also contain:

```text
.agents/agent_card.json
```

This is the structured description of the agent surface. Think of it as the machine-readable
contract for the agent.

The agent card should:

- use the project name and project id in the agent name
- match the agent-specific description from `AGENTS.md`
- use the same version as `pyproject.toml`
- list all project skills under `.agents/skills/`, excluding `.agents/skills/mainsequence`
- avoid invented tags unless they have been explicitly confirmed

## 4. Why Skills Matter More Than Prompts

The agentic layer should not be a generic system prompt that says "be helpful."

The better pattern is:

1. keep real business logic in repository code
2. expose repeatable operations through the project CLI or clearly documented modules
3. attach skills that explain when and how the agent should use those project capabilities

That structure gives you a project agent that is:

- easier to audit
- easier to extend
- less likely to hallucinate unsupported workflows
- better aligned with the actual repository

In other words, the agent should sit on top of the project. It should not replace the project.

## 5. Recommended Conversion Workflow

Use the following sequence when you want to turn an existing project into an agent-oriented one.

1. Confirm the project already has real capabilities worth exposing.
   Good candidates are project CLIs, data workflows, APIs, dashboards, orchestration helpers, or
   repository-specific maintenance operations.
2. Create or refresh `AGENTS.md`.
   Replace generic text in `## Project-Specific Instructions` with the real local rules and the
   real purpose of the project.
3. Add or refresh `.agents/skills/`.
   Keep project-specific skills focused on the tasks this repository can truly perform.
4. Make sure the skills match real code paths.
   If a skill claims to publish data, inspect the `DataNode` path. If it claims to manage
   dashboards, tie it to real dashboard code or real CLI flows.
5. Create `.agents/agent_card.json`.
   Keep the name, description, version, and skill list aligned with the repository state.
6. Treat the agent as a project surface.
   Once the repository is ready, you package and release it through the same Main Sequence project
   lifecycle used for other application surfaces.

## 6. How This Fits The Main Sequence Deployment Model

Agents fit the same deployment model you already saw for APIs and dashboards, but they have an
extra layer in their construction.


1. sync the repository
2. build or select a project image

From this step onward, the flow is only available in the GUI. In comparison with regular resource
releases, where at this point we use the image as the latest project state to create the release,
here we need to create another image on top of the project that includes the coding-agent
capabilities.

For this, in the GUI, go to `mainsequence-ai -> Project Agents`, select the project and the base
image, and build a new agent image. That image will also appear in the project details. After the
image is ready, come back to this screen and deploy the agent.

Important: once a deployment is done, an agent will be created with the project runtime. After
that, you can change the agent and agent-session configuration in `mainsequence-ai Agents`.

With this, you are ready to start interacting with agents directly in the Main Sequence platform
or as part of other agentic workflows.


This continuity is important.

Main Sequence does not require you to create one disconnected "agent project" somewhere else. The
agent is another project surface built from the same repository, with the same commit, the same
image discipline, and the same explicit release model.

## 7. How Projects Interact In Agentic Workflows

The safest way to think about project-to-project agentic work is through explicit contracts.

This is not only about code structure. Runtime boundaries matter too.

Each project has its own repository, runtime, configuration, release lifecycle, and operational
surface. That separation is important because agentic workflows often trigger real operations:
reading published data, calling APIs, running jobs, querying application-facing tables, or using
released dashboards and other exposed project surfaces.

When those operations rely on explicit contracts, they can be reached from the Main Sequence
platform in a controlled way. That gives other projects, dashboards, and agents a stable way to
interact with the project without needing private implementation details or direct access to the
whole repository.

An important operational rule is to keep coding agents on small resource profiles whenever
possible. A project agent should be able to execute work inside its session, but it should behave
mainly as a quick operator and orchestrator:

- handle short-running CLI tasks directly
- inspect state, make decisions, and coordinate the next action
- delegate heavier or longer-running work to project jobs

In practice, that means expensive computation, large backfills, long pipelines, and other
resource-heavy tasks should usually be pushed into jobs instead of being executed inline by the
agent session.

One project can expose:

- published data through `DataNode`s
- operational rows through `MetaTable`
- application APIs through FastAPI
- released dashboards
- released agent resources

Another project, dashboard, or agent can then consume those surfaces through the contracts the
project has chosen to expose.

That is much safer than treating "agentic" as permission to bypass project boundaries. Each
project keeps its own code ownership, runtime configuration, release lifecycle, and operational
rules. The interaction happens through declared resources, stable interfaces, and documented
behavior instead of ad hoc access to internal code paths.

Minimal interaction pattern:

```text
+-------------------+        A2A / explicit contract        +-------------------+
| Project Agent A   |  --------------------------------->  | Project Agent B   |
+-------------------+                                       +-------------------+
         |                                                           |
         | CLI only                                                   | CLI only
         v                                                           v
+-------------------+                                       +-------------------+
| project CLI       |                                       | project CLI       |
+-------------------+                                       +-------------------+
         |                                                           |
         | quick operation                                            | heavy operation
         v                                                           v
+-------------------+                                       +-------------------+
| direct execution  |                                       | run Job           |
| in agent session  |                                       | / orchestrate Job |
+-------------------+                                       +-------------------+
```

## 8. A Good Final Check

Before you describe a project as an agent, verify the following:

- `AGENTS.md` exists and its `## Project-Specific Instructions` section matches the real project
- `.agents/skills/` exists and the project-specific skills match real repository capabilities
- the project exposes a clear CLI or another concrete local control surface
- `.agents/agent_card.json` exists and matches the project name, description, version, and skills
- the project documentation does not promise tasks the repository cannot actually perform

If those checks pass, you are no longer describing a vague future agent. You are describing a
real project surface that can participate in agentic workflows with clear boundaries.

## 9. Further Reading

- [Part 3.2 — Create Your First API](create_your_first_api.md)
- [Part 4 — Orchestration](scheduling_jobs.md)
- [CLI Overview](../cli/index.md)
- [Command Center Overview](../knowledge/command_center/index.md)
