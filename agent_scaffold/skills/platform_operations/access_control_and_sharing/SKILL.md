---
name: mainsequence-access-control-and-sharing
description: Use this skill when the task is about RBAC, resource sharing, or access verification in a Main Sequence project. This skill owns organization and team access concepts, view and edit semantics, choosing the correct shareable resource boundary, and access checks across projects, DataNodeStorage, constants, secrets, buckets, artifacts, and releases. It does not own job scheduling, DataNode producer logic, or API route design.
---

# Main Sequence Access Control And Sharing

## Overview

Use this skill when the task is about who can view, edit, maintain, or administer a resource in a Main Sequence project.

This skill is for:

- RBAC reasoning
- organization, user, role, and team access concepts
- direct sharing vs team sharing
- `view` vs `edit`
- deciding which resource is the real sharing boundary
- choosing between `Constant` and `Secret`
- access verification for shared resources

## This Skill Can Do

- explain the Main Sequence access model in operational terms
- decide whether a resource should be shared directly to a user or to a team
- decide whether a user needs `view` or `edit`
- identify the correct shareable object boundary:
  - `Project`
  - `DataNodeStorage`
  - `Constant`
  - `Secret`
  - `Bucket`
  - `Artifact`
  - `ResourceRelease`
- explain that sharing a DataNode usually means sharing its `DataNodeStorage`
- choose whether configuration belongs in a `Constant` or a `Secret`
- review CLI sharing flows for existing resources
- verify access assumptions before claiming a workflow is shareable

## This Skill Must Not Claim

This skill must not claim ownership of:

- job scheduling or image pinning
- DataNode producer implementation
- SimpleTable schema design
- FastAPI route design
- Streamlit dashboard implementation
- workspace document structure

## Route Adjacent Work

- jobs, schedules, images, project resources, releases, and Artifacts as operational workflows:
  `.agents/skills/mainsequence/platform_operations/orchestration_and_releases/SKILL.md`
- DataNodes:
  `.agents/skills/mainsequence/data_publishing/data_nodes/SKILL.md`
- SimpleTables:
  `.agents/skills/mainsequence/data_publishing/simple_tables/SKILL.md`
- APIs and FastAPI:
  `.agents/skills/mainsequence/application_surfaces/api_surfaces/SKILL.md`
- Streamlit dashboards:
  `.agents/skills/mainsequence/dashboards/streamlit/SKILL.md`
- Command Center workspaces:
  `.agents/skills/mainsequence/command_center/workspace_builder/SKILL.md`

## Read First

1. `docs/tutorial/role_based_access_control.md`
2. `docs/knowledge/infrastructure/users_and_access.md`
3. `docs/knowledge/infrastructure/constants_and_secrets.md`

If the task is specifically about a resource type, also read the corresponding knowledge or tutorial page for that resource.

## Inputs This Skill Needs

Before changing access or advising on sharing, collect or infer:

- the exact resource type being shared
- whether the actor is:
  - a user
  - a team
- whether the access should be:
  - `view`
  - `edit`
- whether the goal is:
  - consumption
  - maintenance
  - administration
- whether the resource contains sensitive configuration
- whether the task is about the resource itself or about the workflow that uses it

If the resource boundary or intended access level is unclear, stop before changing permissions.

## Required Decisions

For every non-trivial access task, decide:

1. What is the real shareable object?
2. Is this direct user access or team-based access?
3. Does the user need `view` or `edit`?
4. Is this configuration non-sensitive or sensitive?
5. Is the task really an access problem, or is it actually an orchestration or implementation problem?
6. Is the task creating a `Constant` or `Secret` by name, and does that name already exist?

## Build Rules

### 1. Share the real resource boundary

Do not speak loosely about sharing "the code" when the operational boundary is a platform object.

Examples:

- sharing a DataNode usually means sharing the `DataNodeStorage`
- sharing a deployed experience usually means sharing the `ResourceRelease`
- sharing runtime configuration means sharing the `Constant` or `Secret`

### 2. `view` is for consumers, `edit` is for maintainers

Use the simplest rule unless the task requires something more specific:

- `view` for people who need to read, inspect, or consume
- `edit` for people who need to maintain, update, or administer

Do not grant `edit` when `view` is enough.

### 3. Team sharing is for repeated access patterns

Prefer team sharing when the same access needs to be reused across several people or resources.

Prefer direct sharing when:

- the access is one-off
- the access is personal
- creating or reusing a team would add unnecessary complexity

### 4. Team membership is not team administration

Do not claim that a user can manage a team just because they inherit access through that team.

Keep these separate:

- inherited access to shared resources
- administration of the team itself

### 5. `Constant` vs `Secret` is a security boundary

Use `Constant` for non-sensitive runtime values.

Use `Secret` for:

- API keys
- passwords
- bearer tokens
- credentials
- anything that would create an incident if exposed

Do not downgrade a secret into a constant for convenience.

### 6. `Constant` and `Secret` names are unique configuration identities

Treat `Constant.name` and `Secret.name` as unique organization-level configuration keys.

For creation or sync tasks:

- do not assume a create is idempotent by itself
- first resolve whether the object already exists by name
- prefer `get(name=...)` when you expect exactly one object
- use `filter(name__in=[...])` when reconciling several keys
- only create missing names

If the task is phrased as "ensure this constant/secret exists", search first and make the workflow idempotent.

Current CLI note:

- there is no dedicated public `constants get/detail` command
- there is no dedicated public `secrets get/detail` command
- the current CLI workaround is name-filtered list
- use:
  - `mainsequence constants list --filter name=MODEL__DEFAULT_WINDOW`
  - `mainsequence secrets list --filter name=POLYGON_API_KEY`

### 7. Access assumptions must be verified

If the task claims a resource is shareable, readable, or maintainable by another actor, verify that path explicitly with the relevant CLI or client workflow.

Do not claim access based only on naming, role titles, or intuition.

## Review Rules

When reviewing an access-control task, look for:

- sharing the wrong resource boundary
- granting `edit` when `view` is sufficient
- using direct user grants where a team should be used
- using a team when the access is clearly one-off
- treating team membership as team administration
- storing sensitive data in a `Constant`
- creating a `Constant` or `Secret` blindly without resolving whether the name already exists
- weak or unverified claims about who can access a resource
- confusion between access policy and deployment workflow

## Validation Checklist

Do not claim success until you have checked:

- the resource boundary is correct
- the grant target is intentional:
  - user
  - team
- the access level is intentional:
  - `view`
  - `edit`
- the task is using `Constant` vs `Secret` correctly
- any `Constant` or `Secret` creation path first resolved existence by name when idempotency matters
- the access claim was verified against the actual resource path
- the task did not confuse sharing policy with orchestration or producer logic

## This Skill Must Stop And Escalate When

- the real shareable resource is unclear
- the actor should probably be a team, but team structure is unknown
- the task mixes access policy with job scheduling or release mechanics
- the task asks for sensitive data to be stored in a `Constant`
- the task assumes cross-organization sharing without explicit documentation
- the request requires a policy decision the user has not made

Do not guess through security boundaries.
