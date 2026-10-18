---
name: mainsequence-access-control-and-sharing
description: Use this skill when the task is about RBAC, resource sharing, or access verification in a Main Sequence CodeRepository. This skill owns organization and team access concepts, the policies of each shareable resource, choosing the correct shareable resource boundary, access checks across CodeRepositories, constants, secrets, buckets, artifacts, Agents, releases and Jobs, and whether an application uses its own grants or acts for its requester (`acts_for_requester`). It does not own job scheduling, domain-package data behavior, or API route design.
---

# Main Sequence Access Control And Sharing

## Overview

Use this skill when the task is about who can view, edit, maintain, or administer a resource in a Main Sequence CodeRepository.

This skill is for:

- RBAC reasoning
- organization, user, role, and team access concepts
- direct sharing vs team sharing
- choosing the policy a person, a workload or a team needs
- deciding which resource is the real sharing boundary
- choosing between `Constant` and `Secret`
- access verification for shared resources

## This Skill Can Do

- explain the Main Sequence access model in operational terms
- decide whether a resource should be shared directly to a user or to a team
- decide which policy a user, a workload or a team needs
- identify the correct shareable object boundary:
  - `CodeRepository`
  - `CodeRepositoryBranch`
  - `OrganizationProject`
  - `Constant`
  - `Secret`
  - `Bucket`
  - `Artifact`
  - `Agent`
  - `ResourceRelease`
  - `Job`
- choose whether configuration belongs in a `Constant` or a `Secret`
- review CLI sharing flows for existing resources
- manage a team's members and decide who controls the team, through the
  team's policies
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

1. Repository instructions, including `AGENTS.md` when present.
2. <https://mainsequence-sdk.github.io/mainsequence-sdk/knowledge/infrastructure/users_and_access/>
3. <https://mainsequence-sdk.github.io/mainsequence-sdk/knowledge/infrastructure/constants_and_secrets/>
4. <https://mainsequence-sdk.github.io/mainsequence-sdk/knowledge/infrastructure/object_access/>
   for every shareable object, whose access is decided by policies
5. <https://mainsequence-sdk.github.io/mainsequence-sdk/knowledge/infrastructure/code_repositories/>
   for code repositories, their branches and Projects
6. <https://mainsequence-sdk.github.io/mainsequence-sdk/knowledge/infrastructure/agents/>
   for Agents and the releases that back them
7. <https://mainsequence-sdk.github.io/mainsequence-sdk/knowledge/infrastructure/resource_releases/>,
   <https://mainsequence-sdk.github.io/mainsequence-sdk/knowledge/infrastructure/scheduling_jobs/>
   and <https://mainsequence-sdk.github.io/mainsequence-sdk/knowledge/infrastructure/artifacts/>
   for releases, Jobs, buckets and Artifacts

The pages above are the authoritative reference for access control and sharing.
If the task is specifically about a resource type, also read that resource's own
page from the documentation root at <https://mainsequence-sdk.github.io/mainsequence-sdk/>.

## Inputs This Skill Needs

Before changing access or advising on sharing, collect or infer:

- the exact resource type being shared
- whether the actor is:
  - a user
  - a team
- which policy the access needs:
  - for a `Constant`, which is decided by policies: `reader`, `editor` or `owner`
  - for a `Secret`, which is decided by policies: `reader`, `value_reader`,
    `editor` or `owner`
  - for a `Bucket`, which is decided by policies: `reader`, `contributor`,
    `editor` or `owner`
  - for a `CodeRepository`, a `CodeRepositoryBranch` or an
    `OrganizationProject`, which are decided by policies: `reader`, `editor`
    or `owner`
  - for an `Artifact`, which is decided by policies: `viewer`, `editor` or
    `owner`
  - for an `Agent`, which is decided by policies: `viewer`, `user`,
    `operator`, `editor` or `owner`
  - for a `ResourceRelease`, which is decided by policies: `viewer`, `user`,
    `editor` or `owner`
  - for a `Job`, which is decided by policies: `viewer`, `runner`, `editor` or
    `owner`
  - for a model-provider credential or a custom model provider, which are
    decided by policies: `user`, `editor` or `owner`
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
3. Which policy does the principal need: on a `Constant`, `reader`, `editor` or `owner`; on a
   `Secret`, `reader`, `value_reader`, `editor` or `owner`; on a `Bucket`, `reader`,
   `contributor`, `editor` or `owner`; on an `Artifact`, `viewer`, `editor` or `owner`; on a
   `CodeRepository`, a `CodeRepositoryBranch` or an `OrganizationProject`, `reader`, `editor` or
   `owner`; on an `Agent`, `viewer`, `user`, `operator`, `editor` or `owner`; on a
   `ResourceRelease`, `viewer`, `user`, `editor` or `owner`; on a `Job`, `viewer`, `runner`,
   `editor` or `owner`; on a model-provider credential or a custom model provider, `user`,
   `editor` or `owner`?
4. Is this configuration non-sensitive or sensitive?
5. Is the task really an access problem, or is it actually an orchestration or implementation problem?
6. Is the task creating a `Constant` or `Secret` by name, and does that name already exist?

## Build Rules

### 1. Share the real resource boundary

Do not speak loosely about sharing "the code" when the operational boundary is a platform object.

Examples:

- sharing a deployed experience usually means a policy on the
  `ResourceRelease`, or on the `Agent` when the release backs one
- sharing runtime configuration means sharing the `Constant` or `Secret`

### 2. Consumers get the narrowest policy, maintainers get `editor`

Use the simplest rule unless the task requires something more specific:

- the narrowest reading or using policy, such as `reader`, `viewer` or
  `user`, for people who need to read, inspect, or consume
- `editor` for people who need to maintain or update
- `owner` only for the people who decide who has access

Do not grant `editor` when a reading or using policy is enough. No resource
uses view and edit sharing any more.

A `Constant` is decided by its policies:

- `reader` sees the constant and its value;
- `editor` also changes its name and value, deletes it and sees who has access;
- `owner` also decides who has access.

The person or workload that creates a constant holds `owner`. Give `reader` to
consumers and `editor` to maintainers, with the access methods on `Constant`
(`get_access()`, `attach_policy()`, `detach_policy()`, `explain_access()`) or
`mainsequence constants access list|grant|revoke|explain`. A constant has no
`add_to_view()` or `add_to_edit()`, and its access methods need no Environment.

A `Secret` is decided by its policies too:

- `reader` sees the secret and its name, never its value;
- `value_reader` also reads its current value;
- `editor` also reads earlier values, sets a new value, deletes it and sees who
  has access;
- `owner` also decides who has access.

The person or workload that creates a secret holds `owner`. Give
`value_reader` to the workloads and the few people that need the value, and
`editor` to maintainers; `reader` is for people who configure workloads that
use the secret but must never know it. A workload holding only `reader` cannot
read the value: `Secret.get()` raises `PermissionDeniedError` with
`required_action` `secrets.read_value`. Use the access methods on `Secret` or
`mainsequence secrets access list|grant|revoke|explain`; a secret has no
`add_to_view()` or `add_to_edit()`. A repository workflow file's `access` entry
for a secret names the policy it gives (`policy: reader`, `value_reader`,
`editor` or `owner`). A secret the platform manages, such as a hosted database
password, cannot get a new value or be deleted.

A `Bucket` is decided by its policies too:

- `reader` browses its folders and the Artifacts in it shared with the person;
- `contributor` also uploads Artifacts and creates folders;
- `editor` also renames the bucket, renames and deletes folders, deletes the
  bucket with every Artifact in it and sees who has access;
- `owner` also decides who has access.

The person or workload that creates a bucket holds `owner`, also when an
Artifact upload creates it by name. Give `contributor` to the people and the
workloads that upload into a bucket, and `editor` to maintainers. Uploading
into an existing bucket without `contributor` raises `PermissionDeniedError`
with `required_action` `buckets.add_artifact`. Use the access methods on
`Bucket`; a bucket has no `add_to_view()` or `add_to_edit()`, and the CLI has
no bucket commands. A repository workflow file's `access` entry for a bucket
names the policy it gives (`policy: reader`, `contributor`, `editor` or
`owner`).

A `CodeRepository`, its `CodeRepositoryBranch` objects and an
`OrganizationProject` are decided by their policies too, each with `reader`,
`editor` and `owner`:

- on a repository, `reader` sees it, its GitHub link and issues, and every
  branch in it; `editor` also renames or archives it, adds deploy keys,
  creates branches, edits every branch and sees who has access; `owner` also
  decides who has access. Only Organization admins delete a repository;
- on a branch, `reader` sees the branch, its files, Jobs, runs, logs and
  releases, and calls its releases; `editor` also changes and deletes it,
  edits its Jobs and releases and manages what its workloads can reach;
  `owner` also decides who has access;
- on a Project, a named group of branches in one Environment, `reader` sees
  it and its branches; `editor` also renames it, adds and removes branches and
  deletes it; `owner` also decides who has access.

A repository's creator and a Project's creator hold `owner`; a branch's
creator holds nothing for being its creator. Repository and Project policies
pass down to the branches: `reader` gives branch `reader`, and `editor` and
`owner` give branch `editor`, so share the repository or the Project when
every branch should follow. Using a repository or a branch needs the
platform's `user` product role: without it every route, the access methods
included, raises `PermissionDeniedError` with `code` `missing_product_role`,
also through a Team, and no policy gives it. Use the access methods on
`CodeRepository`, `CodeRepositoryBranch` and `OrganizationProject(uid=...)`,
or `mainsequence code-repository access list|grant|revoke|explain` for a
repository; the CLI has no branch or Project access commands. A repository
workflow file's `access.branch` and `access.branches` entries name the
workload's policy (`branch: reader`, or `policy: reader` on a `branches`
entry).

Model-provider credentials and custom model providers are decided by policies
too, as two types:

- a model-provider credential, one person's sign-in to a built-in model
  provider: `user` selects it and receives it in sessions and workloads,
  including local Tau, where it can be copied; `editor` also refreshes it,
  signs in again, revokes it and sees who has access; `owner`, which the
  credential's person holds, also decides who has access;
- a custom model provider, an inference endpoint with its own authentication
  and models: `user` sends requests through it and its models and receives its
  stored authentication in sessions and workloads, including local Tau, where
  it can be copied; `editor` also changes its endpoint, authentication and
  models, deletes it and sees who has access; `owner`, which its creator holds,
  also decides who has access.

Give `user` to the people and workloads that send requests through a
credential or a custom provider, and only to those you trust with a credential
they can copy; give `editor` to maintainers. The SDK has no model or command
for these records. A repository workflow file's `access` entry for model
providers names the policy it gives (`policy: user`, `editor` or `owner`).

An `Artifact` is decided by its policies too:

- `viewer` sees it and downloads its file;
- `editor` also replaces its file, deletes it and sees who has access;
- `owner` also decides who has access.

Every policy that sees an Artifact also downloads it, and a downloaded copy
stays with the person. Uploading under the name of an Artifact you can see
replaces its file, which needs `editor` or `owner`: with `viewer` alone
`Artifact.upload_file()` raises `PermissionDeniedError` with
`required_action` `artifacts.overwrite`. The Jobs whose runs published an
Artifact give `viewer` on it to everyone who holds a policy on the Job. Use the
access methods on `Artifact`; the CLI has no Artifact commands. A repository
workflow file's `access` entry for an Artifact names the policy it gives, such
as `policy: viewer`.

An `Agent` is decided by its policies:

- `viewer` talks to it, gives it Tasks, sees all its Tasks and reads the
  person's own log lines;
- `user` talks to it and gives it Tasks, seeing only the Tasks the person
  asked for;
- `operator` also stops any Task or session run;
- `editor` also configures it, moves it to a new runtime build, deletes it,
  manages what its workload can reach and sees who has access;
- `owner` also decides who has access.

Give `user` to most people, and `viewer` only to those who should see other
people's Tasks. The Agent works with its own access unless it acts for the
person asking. Its policies also decide its runtime release and that release's
backing Job: give access on the Agent, never on its release, which refuses a
policy of its own with `BadRequestError` `code` `sharing_managed_by_owner`.
Use the access methods on `Agent`, or `mainsequence agent access
list|grant|revoke|explain`. A repository workflow file's `access` entry for an
Agent names the policy it gives, such as `policy: user`.

A `ResourceRelease` and a `Job` are decided by their policies:

- on a release, `viewer` sees its configuration, revisions and logs and calls
  it; `user` calls it without its logs; `editor` also changes its
  configuration, activates static deployments, deletes it and manages what its
  workload can reach; `owner` also decides who has access;
- on a Job, `viewer` sees it, its runs, logs and outputs; `runner` also runs
  it and stops its runs; `editor` also changes and deletes it and manages what
  its workload can reach; `owner` also decides who has access.

A release that backs no Agent and a standalone Job also take the policies of
their branch: branch `reader` gives `viewer`, and `editor` and `owner` give
`editor`. A release's backing Job follows the release and is changed, run and
deleted with it. Give
`user` on a release to the people who only call it, and `runner` on a Job to
the people who run it without changing it. Use the access methods on
`ResourceRelease` and `Job`, or `mainsequence code-repository resources access
list|grant|revoke|explain` and `mainsequence code-repository jobs access
list|grant|revoke|explain`. A repository workflow file's `access` entry for a
release or a Job names the policy it gives, such as `policy: user` on a release
or `policy: runner` on a Job.

The creator of an Agent, a release, a Job or an Artifact holds `owner`, except
the Organization's automation identity, which creates the objects of
repository workflows and holds no policy on them. The access methods of all
four take no Environment; with a workload's runtime credentials they reach
only the objects of that workload's Environment.

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

Membership is who is in the team. Control is the policies on the team, not view
and edit sharing:

- `viewer` sees the team and its members;
- `membership_manager` adds and removes members;
- `editor` renames, describes, activates or deactivates it and sees its policies;
- `owner` does everything, including deleting it and deciding who manages it;
- every member holds `member`: they can see the team and its members, and leave it.

Any person can create a team and becomes its owner and first member. Use
`Team.add_member()`, `remove_member()`, `leave()` and the access methods
(`get_access()`, `attach_policy()`, `detach_policy()`, `explain_access()`), or
`mainsequence organization teams members ...`, `leave` and `access ...`. A
team has no `add_to_view()` or `add_to_edit()`. Before saying who can change a
team, read it with `team.get_access()` or `explain_access()`.

Working in an Organization Environment is a policy too: `member` works in it
and creates Projects, Buckets, Constants, Secrets, model providers and
repositories there; `observer` works in it without creating anything. Only
Organization admins attach them, to people and Teams, with the access methods
on `OrganizationEnvironment`. Admission never shows objects that are not shared
with the person.

An application that checks whether its caller or requester belongs to a team
uses `in_teams()`; the identity carries no team list. See
`.agents/skills/mainsequence/application_surfaces/api_surfaces/SKILL.md`.

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

All SDK and CLI access changes identify people, workload identities and teams
by public UUID:

- `attach_policy(policy, principal_type="user", principal_uid=user_uid)` and
  `attach_policy(policy, principal_type="team", principal_uid=team_uid)`
- `detach_policy(binding_uid)`, with a binding UID that `get_access()` lists

Never pass or request a numeric user or team database ID. CLI access commands
take `--user <USER_UID>` or `--team <TEAM_UID>` and `<BINDING_UID>`, and access
output is interpreted through public UID fields.

### 8. Access assumptions must be verified

If the task claims a resource is shareable, readable, or maintainable by another actor, verify that path explicitly with the relevant CLI or client workflow.

Do not claim access based only on naming, role titles, or intuition.

## Application Access: Own Grants Or Acting For Its Requester

This section is the reference for how a deployed application, a FastAPI
release or an Agent, reaches platform data.

### Its own access: grants to its workload User

An application runs as its own workload User (`workload_user_uid` on the
`ResourceRelease`, `Job` or `Agent`). Its own access comes from grants to that
User: give it the policies it needs as you would a person, a reading or using
policy unless it must maintain the object. A `Constant` it reads needs
`reader`, a `Secret` whose value it reads needs `value_reader`, a `Bucket` it
uploads Artifacts into needs `contributor`, an `Artifact` it reads needs
`viewer`, a release it calls needs `user`, a `Job` it runs needs `runner`, an
`Agent` it gives Tasks to needs `user`, and a model-provider credential or a
custom model provider it sends requests through needs `user`. These grants
cover every call the application makes for itself.

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
the requester with `User.get_requester()`, the person's own `uid` and
`is_organization_admin`, checks the person's teams with `in_teams()`, and
authorizes reads and writes against that
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
- granting `editor` or `owner` when a reading or using policy is sufficient
- a policy on a release that backs an Agent instead of on the Agent
- using direct user grants where a team should be used
- using a team when the access is clearly one-off
- treating team membership as team administration
- team checks that do not use `in_teams()`, that read an `in_teams()` error
  as "not a member", or that reuse an older answer (no `max_age_seconds=0`)
  before a write that must see a removal
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
- the policy is intentional:
  - `reader`, `editor` or `owner` on a `Constant`
  - `reader`, `value_reader`, `editor` or `owner` on a `Secret`
  - `reader`, `contributor`, `editor` or `owner` on a `Bucket`
  - `reader`, `editor` or `owner` on a `CodeRepository`, a
    `CodeRepositoryBranch` or an `OrganizationProject`
  - `viewer`, `editor` or `owner` on an `Artifact`
  - `viewer`, `user`, `operator`, `editor` or `owner` on an `Agent`
  - `viewer`, `user`, `editor` or `owner` on a `ResourceRelease`
  - `viewer`, `runner`, `editor` or `owner` on a `Job`
  - `user`, `editor` or `owner` on a model-provider credential or a custom
    model provider
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
