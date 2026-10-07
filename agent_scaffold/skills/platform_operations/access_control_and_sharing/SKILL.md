---
name: mainsequence-access-control-and-sharing
description: Use this skill when the task is about RBAC, resource sharing, or access verification in a Main Sequence CodeRepository. This skill owns organization and team access concepts, view and edit semantics, choosing the correct shareable resource boundary, access checks across CodeRepositories, constants, secrets, buckets, artifacts, and releases, and whether an application uses its own grants or acts for its requester (`acts_for_requester`). It does not own job scheduling, domain-package data behavior, or API route design.
---

# Main Sequence Access Control And Sharing

## Overview

Use this skill when the task is about who can view, edit, maintain, or administer a resource in a Main Sequence CodeRepository.

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
  - `CodeRepository`
  - `Constant`
  - `Secret`
  - `Bucket`
  - `Artifact`
  - `ResourceRelease`
- choose whether configuration belongs in a `Constant` or a `Secret`
- review CLI sharing flows for existing resources
- verify access assumptions before claiming a workflow is shareable
- decide whether an application uses its own grants or acts for its
  requester, and explain what acting for the requester allows

## This Skill Must Not Claim

This skill must not claim ownership of:

- job scheduling or image pinning
- domain-package data production and schema design
- FastAPI route design
- application UI design or implementation

## Route Adjacent Work

- jobs, schedules, images, code repository resources, releases, and Artifacts as operational workflows:
  `.agents/skills/mainsequence/platform_operations/orchestration_and_releases/SKILL.md`
- MetaTables and table updates: use the installed `metatables` package skills
  and documentation
- Command Center-serving FastAPI providers:
  `.agents/skills/mainsequence/application_surfaces/api_surfaces/SKILL.md`
This skill only reasons about access to deployed resources such as `ResourceRelease`.

## Read First

1. `AGENTS.md`
2. <https://mainsequence-sdk.github.io/mainsequence-sdk/knowledge/infrastructure/users_and_access/>
3. <https://mainsequence-sdk.github.io/mainsequence-sdk/knowledge/infrastructure/constants_and_secrets/>

The pages above are the authoritative reference for access control and sharing.
If the task is specifically about a resource type, also read that resource's own
page from the documentation root at <https://mainsequence-sdk.github.io/mainsequence-sdk/>.

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

Treat `Constant.name` and `Secret.name` as unique Environment-level
configuration keys. CodeRepository-facing SDK and CLI operations derive the
Environment from the process-frozen current Git branch and registered
`CodeRepositoryBranch`. Never ask the user to provide an Environment UID or branch UID.

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

### 7. Public principal identity is UID-only

All SDK and CLI sharing mutations identify users and teams by public UUID:

- `add_to_view(user_uid)` and `add_to_edit(user_uid)`
- `remove_from_view(user_uid)` and `remove_from_edit(user_uid)`
- the corresponding team methods use `team_uid`

Never pass or request a numeric user or team database ID. CLI sharing commands
take `<USER_UID>` or `<TEAM_UID>`, and access-state output is interpreted through
public UID fields.

### 8. Access assumptions must be verified

If the task claims a resource is shareable, readable, or maintainable by another actor, verify that path explicitly with the relevant CLI or client workflow.

Do not claim access based only on naming, role titles, or intuition.

## Application Access: Own Grants Or Acting For Its Requester

This section is the reference for how a deployed application, a FastAPI
release or an Agent, reaches platform data.

### Its own access: grants to its workload User

An application runs as its own workload User (`workload_user_uid` on the
`ResourceRelease`, `Job` or `Agent`). Its own access comes from grants to that
User: share what it needs with that User as with any person, with `view`
unless it must maintain the object. These grants cover every call the
application makes for itself.

### Acting for its requester: `acts_for_requester`

An application can instead be enabled to act for its requester: the person
whose own request it is serving. The platform setting is `acts_for_requester`.

- It is `false` by default. Nothing changes for an application that leaves it
  off.
- Only Organization admins enable it, in one of two ways:
  - after the application exists, with
    `PATCH /api/v1/workload-users/<workload_user_uid>/`;
  - in code, by declaring `acts_for_requester: true` on the resource, a
    `fastapi` or `harness_agent` one, in the repository workflow file:

    ```yaml
    resources:
      - key: api
        kind: fastapi
        spec:
          source_path: api/main.py
        acts_for_requester: true
    ```

    Take the exact field from the branch's workflow template and validate the
    file before committing it. A declaration takes effect only when the person
    who pushed it is an Organization admin. Otherwise that resource fails
    before it deploys.
- An application that should act only for people, for example a data analyst
  Agent, holds no grants of its own. Do not share data with its workload User
  to make it work: everyone who can use the application could then reach that
  data through it.

### What the requester's access covers

The platform applies one rule to every call an Agent makes for its work: with
no delegation, the caller's own ordinary permissions; with a valid delegation,
the person's ordinary permissions, including administrative ones; an invalid or
expired delegation is rejected, never switched to another identity.

- The reads, creates, updates, runs, sharing changes and deletes the person
  may make through supported operations.
- Check the person's permission for the exact object and operation on every
  call. A read grant or team membership alone does not authorize a write.
- Only the person's permissions: never the acting Agent's grants, and never
  the two combined.
- At most 24 hours after the person's request, and only while the work for that
  request runs.
- Checked again on every call, so it stops the moment the person's access ends.
- Platform-authorized Agent-to-Agent delegation keeps the person who started
  the work and the original request time. Each delegated Agent must itself be
  enabled to act for its requester. Delegation cannot restart the time limit,
  select another person or increase their permissions.
- A receiving application cannot forward its inbound assertion to establish
  another requester binding. `reads_as_caller()` still refuses these requests.

The platform decides which operations support requester-bound access;
installing this SDK does not enable one. Treat a refused operation as refused,
never retry it with the application's own grants.

### Never accept a person's UID from a request

The requester comes only from the platform. An application never accepts a
person's UID from a request body, header or query parameter, never parses the
assertion, headers or MCP `_meta` itself, and never lets a caller choose whom
it acts for. A FastAPI application that receives a requester-bound call reads
the requester with `User.get_requester()`, the person's own `uid`, `team_uids`
and `is_organization_admin`, and authorizes reads and writes against that
person's object- and operation-specific permissions. Each handler or tool
decides whether it needs a person. See
`.agents/skills/mainsequence/application_surfaces/api_surfaces/SKILL.md`.

### What people are told

Every client that shows an application with `acts_for_requester` shows the
platform's statement, exactly:

> **This Agent works with your identity.** It can read, create, change, run, share or delete only what your permissions allow through supported operations, only while serving your request, and for at most 24 hours after you ask. It uses your ordinary permissions, including administrative permissions, and access is checked on every call. Your Organization's administrator approved it to work this way.

Do not invent, paraphrase or shorten it. Never present such an Agent as
read-only or claim that it cannot change, share or delete anything.

Explain the risk alongside the statement: a model-driven Agent can be steered
by prompt injection. Its reads and writes can reach as far as the person's
ordinary permissions allow, administrative ones included, such as sharing and
deleting through supported operations, for at most 24 hours after the original
request. Only Organization admins decide which Agents may receive this
authority; the application's per-operation permission checks remain necessary.

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
- grants to an application that should act only for people
- an application that takes a person's UID from a request instead of from the
  platform
- `acts_for_requester` presented as something a non-admin can enable
- a requester-bound write authorized by a read grant, by the Agent's grants, or
  by the Agent's grants combined with the person's
- prompt-injection risk hidden behind a claim that requester access is read-only

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
- an application that acts for its requester takes the person only from the
  platform, and its own grants are intentional
- requester-bound writes enforce the person's permission for the operation,
  and clients show the platform's exact statement and explain the write risk

## This Skill Must Stop And Escalate When

- the real shareable resource is unclear
- the actor should probably be a team, but team structure is unknown
- the task mixes access policy with job scheduling or release mechanics
- the task asks for sensitive data to be stored in a `Constant`
- the task assumes cross-organization sharing without explicit documentation
- the request requires a policy decision the user has not made
- the task needs `acts_for_requester` and no Organization admin has decided to
  enable it

Do not guess through security boundaries.
