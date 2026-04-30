---
name: mainsequence-project-to-agent
description: Use this skill when the task is about adding agentic capabilities to a Main Sequence project.
---

Use this skill to plan or implement the changes required to add agentic capabilities to a Main Sequence project repository.

This skill must only be used for adding agentic capabilities to an existing project. For general project creation, project structure, or project-building tasks that are not specifically about agentic capabilities, use the Project Builder skill instead.

## This Skill Can Do

- Suggest a folder structure that better supports agentic capabilities.
- Define a refactor plan that prepares the project for the required Main Sequence agentic capabilities.
- Create the required files and folders for agentic capabilities.
- Verify that the project has the required local agent configuration files.
- Verify that the project has skills that are aligned with its CLI capabilities.

## This Skill Must Not Claim

- That it knows how to build the entire project unless the work is directly related to adding agentic capabilities.
- That it can interact with the Main Sequence platform.
- That it can add, change, or validate platform behavior outside the local project repository.
- That it can invent project capabilities, agent skills, tags, endpoints, or CLI behavior that are not supported by the existing project files or confirmed by the user.

## Route Adjacent Work

For general project-building tasks, use:

```text
.agents/skills/project_builder/SKILL.md
```

## Instructions

When the user asks for a plan, produce a plan only.

Unless the user explicitly asks for a plan only, inspect the repository and implement the changes needed to prepare the project for agentic capabilities.

Do not infer unsupported functionality. Base all checks and generated files on the existing project files, including AGENTS.md, project documentation, CLI code, .agents/skills, and pyproject.toml. If required information is missing, make a clear suggestion to the user instead of inventing it.

## Required Checks

1. Verify that AGENTS.md contains a project-specific instructions section named:

`##Project-Specific Instruction`

2. The content of this section must match the project intention and documentation. If the section is missing or does not match the project, suggest the required update to the user.

Verify that the repository contains skills inside:

`.agents/skills`

3. Exclude any skills or files inside:

`.agents/skills/mainsequence`

4. Verify that the project has CLI capabilities.

Prefer CLI implementations located in a project CLI module, such as:

src/cli

or another documented CLI location.

If the CLI location is not clear, ask the user whether CLI capabilities exist and where they are implemented.

5. Verify that all non-Main Sequence skills in .agents/skills are related to using or supporting the project CLI.

Exclude files inside:

`.agents/skills/mainsequence`

6. Verify that an agent card exists at:

.agents/agent_card.json

If it does not exist, create it.

If it already exists, verify that it complies with the criteria below.

## Agent Card Criteria

The file must be located at:

.agents/agent_card.json

The agent card must satisfy the following criteria:

1. The name must match the project name and project ID.
2. The description must align with the agent-specific description in AGENTS.md.
3. The version must match the project version in pyproject.toml.
4. The skills list must include all skills found in .agents/skills, excluding skills inside .agents/skills/mainsequence.
5. The card must use the template below as its structural base.
6. Do not add skill tags unless they have been confirmed by the user. If tags are required by the structure, use an empty array until the user confirms the tags.
Agent Card Template

Use this template as the base for .agents/agent_card.json:

```json
{
  "name": "YOUR_AGENT_NAME",
  "description": "Describe what this agent does.",
  "version": "1.0.0",

  "supportedInterfaces": [
    {
      "url": "http://localhost:8010",
      "protocolBinding": "HTTP+JSON",
      "protocolVersion": "1.0"
    }
  ],

  "provider": {
    "organization": "YOUR_ORG_OR_WORKSPACE_NAME",
    "url": "https://example.com"
  },

  "documentationUrl": "https://example.com/docs/YOUR_AGENT_NAME",

  "capabilities": {
    "streaming": false,
    "pushNotifications": false,
    "extendedAgentCard": false
  },

  "securitySchemes": {},
  "securityRequirements": [],

  "defaultInputModes": [
    "application/json"
  ],

  "defaultOutputModes": [
    "application/json"
  ],

  "skills": [
    {
      "id": "YOUR_SKILL_ID",
      "name": "Your Skill Name",
      "description": "Describe the task this skill can perform.",
      "tags": [],
      "examples": [
        "Example task request that this agent can handle."
      ],
      "inputModes": [
        "application/json"
      ],
      "outputModes": [
        "application/json"
      ]
    }
  ]
}
```
## Expected Output

When applying this skill, provide one of the following:

A plan for adding agentic capabilities, if the user asked for a plan.
A summary of repository checks performed.
A list of missing or invalid agentic capability requirements.
The files and folders created or modified.
The final .agents/agent_card.json content, when created or updated.
## Guardrails
Do not modify unrelated project-building files unless the change is required for agentic capabilities.
Do not add skills that are not supported by the project documentation, CLI, or user confirmation.
Do not add tags to skills unless the user confirms them.
Do not include skills from .agents/skills/mainsequence in the generated agent card.
Do not claim that the project can interact with the Main Sequence platform.
Do not claim that the project can perform actions that are not represented in its local files, CLI, documentation, or confirmed user requirements.