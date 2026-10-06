# Users and Access

Main Sequence is built for shared work. CodeRepositories, datasets, application surfaces, constants, secrets, artifacts, and releases are rarely useful for only one person.

That means access control is not an optional admin topic. It is part of how teams build and operate on the platform.

This guide explains the access model in plain language, without implementation details.

For notification delivery rules and client usage, see [Notifications](notifications.md).

## Start with the right mental model

There are five concepts to keep separate:

1. `Organization`: the tenant boundary
2. `User`: a person who signs in to the platform, or the workload identity a deployed Job, FastAPI release or Agent runs as
3. `Role`: a broad level of responsibility inside the organization
4. `Team`: a reusable group you can share resources with
5. Resource sharing: the explicit grants that decide who can view or edit a specific resource

Most access questions can be reduced to:

- which organization does this belong to
- who is the user
- does the user have a broad admin role
- was the resource shared directly to the user
- was the resource shared to a team the user belongs to

## How the pieces fit together

```mermaid
graph TD
    Org["Organization"] --> Users["Users"]
    Org --> Teams["Teams"]
    Users -->|membership| Teams
    Users -->|direct access| Resources["CodeRepositories, Constants, Secrets, Buckets, Artifacts, Releases"]
    Teams -->|team access| Resources
```

## Organization

The organization is the main security boundary.

In practice, that means:

- users belong to one organization
- teams belong to one organization
- shared resources are usually visible only inside that organization
- access is designed around collaboration inside the same tenant, not across tenants

This is why the first access question is usually not "what role does this person have?" but "are they even in the same organization?"

## User

A user is the person operating on the platform.

Users are the identities that:

- sign in
- create resources
- run commands through the CLI
- receive direct access to resources
- belong to one or more teams

When you share a resource directly to a user, you are saying:

> this specific person should be able to see or edit this specific thing

That is the most explicit form of access.

## Workload identities

A deployed Job, FastAPI release or Agent runs as its own User: its workload
identity. A workload identity belongs to the Organization like a person, but it
is not a person: other Users see no username, email, join date or name for it.

Any member of the Organization can read a workload identity by its UID, for
example to check the caller of a request an application received:

```python
from mainsequence.client import User

user = User.get_by_uid(user_uid)

user.identity_type          # "workload"
user.is_active
user.job_uid                # the Job it belongs to, or None
user.resource_release_uid   # the release it belongs to, or None
user.agent_uid              # the Agent it belongs to, or None
user.username, user.email   # None for a workload identity
```

For a workload identity the platform sends `uid`, `identity_type`, `is_active`,
`job_uid`, `resource_release_uid` and `agent_uid`. The `User` fields it does not
send are `None`, or empty for lists.

To go the other way, from a workload to its identity, read
`workload_user_uid` on the `Job`, `ResourceRelease` or `Agent`. It is `None`
when the object has no workload identity, for example a static-site release,
a release's backing Job, or an Agent without a runtime release. An Agent and
its runtime release report the same value.

```python
from mainsequence.client import Job, User

job = Job.filter(name__contains="nightly-prices")[0]
user = User.get_by_uid(job.workload_user_uid)  # identity_type == "workload"
```

`User.filter()` lists people only. Ask for workload identities with the
`identity_type` filter; the listing holds the ones you can view:

```python
workloads = User.filter(identity_type="workload")
```

`identity_type` is one of `human`, `service_account`, `deleted_user` and
`workload`. A person's row may not carry it, so test for `"workload"` rather
than for `"human"`.

The sharing methods take a workload identity like any other User. An
application that received a request from a workload can give that workload
access to its own objects, and the platform decides whether the grant is
allowed:

```python
caller = User.get_logged_user()
user = User.get_by_uid(caller.uid)
if user.identity_type == "workload":
    artifact.add_to_view(user)  # any shareable object the application created
```

## Roles

Roles are the broad, organization-level classification of what kind of platform user someone is.

For most practical workflows, the important roles are:

- `Organization Admin`
- `Dev User`
- `Light User`

Use roles to think about broad responsibility:

- who can administer the organization
- who is expected to build and maintain workflows
- who mostly consumes outputs

Do not use roles as your mental model for day-to-day sharing.

Why:

- roles are coarse
- real collaboration usually happens at the resource level
- two users with the same broad role may still need access to very different CodeRepositories, tables, or application surfaces

So the safe rule is:

- roles define broad responsibility
- sharing defines actual resource access

## Teams

Teams are reusable sharing groups inside one organization.

Use a team when the same set of people should repeatedly receive access to the same class of resources.

Examples:

- `Research`
- `Platform`
- `Risk`
- `Execution`
- `Client Reporting`

Teams are useful because they let you share once and reuse that decision many times.

Instead of sharing several application resources and datasets to five people one by one, you can share them to one team and manage membership there.

## What team membership means

If a user belongs to a team, they inherit access for resources that were shared to that team.

That is the core idea.

Membership means:

- the user is part of that team for access purposes
- the user receives inherited access to resources shared to that team

Membership does not automatically mean:

- the user can manage the team
- the user can change who else is in the team
- the user can change the team's sharing rules
- the user can administer every resource the team can see

That distinction matters a lot.

Being inside a team means the user benefits from the team's access. It does not automatically make them an administrator of the team itself.

## Resource sharing

Main Sequence uses explicit sharing for important resources.

That is the practical layer that answers:

- who can read this resource
- who can edit this resource

This model appears across resources such as:

- `CodeRepository`
- `Constant`
- `Secret`
- `Bucket`
- `Artifact`
- `ResourceRelease`

The same pattern shows up again and again:

- some users can view
- some users can edit
- access may come directly or through a team

## Direct access vs team access

There are two common ways a user gets access to a resource.

### Direct access

The resource is shared straight to a user.

Use this when:

- the access is personal
- the resource is unusual or one-off
- you do not want to create or reuse a team for it

### Team access

The resource is shared to a team, and all current team members inherit that access.

Use this when:

- several people need the same access
- access should stay aligned as people join or leave the team
- you want one reusable access boundary instead of many manual grants

## View vs edit

Most of the time, the practical distinction is simple:

- `view`: can consume or inspect the resource
- `edit`: can modify, maintain, or administer the resource

For example:

- on a supported application release, `view` means opening or calling it
- on a constant, `edit` means changing the runtime value

This is the cleanest engineering split:

- readers get `view`
- maintainers get `edit`

## The subtle but important rule about teams

This is the part people most often get wrong:

> a team can be used as a principal for sharing, but being a member of the team does not automatically make you an administrator of the team itself

In other words:

- if a dataset is shared to `Research`, members of `Research` inherit access to that dataset
- that does not automatically mean every member of `Research` can manage the `Research` team

This is why "team membership" and "team administration" should be thought of as separate concerns.

## Practical examples

### Example 1: Share a dataset to a team

If an `Artifact` is shared to `Research` with `view` access:

- current members of `Research` can read the dataset
- future members of `Research` will also inherit that read access
- removing someone from `Research` removes that inherited path

### Example 2: Give one person direct edit access

If one workflow maintainer needs to manage a dataset directly:

- share the `Artifact` to that user with `edit`
- do not widen access for the whole team unless that is actually intended

### Example 3: Team membership is not team administration

If Alice is a member of `Research`:

- Alice inherits access for resources shared to `Research`
- Alice does not automatically become responsible for changing `Research` membership
- Alice does not automatically become responsible for changing `Research` sharing rules

### Example 4: Team-based edit does not mean unlimited control

If `Team A` has `edit` access to an application resource or dataset:

- members of `Team A` can use that edit path on that resource
- that does not automatically mean they can change every access rule everywhere else on the platform

Keep your thinking resource by resource.

## How access fits the platform workflow

The same resource-scoped pattern applies across the platform:

- `Constant` and `Secret` control access to runtime configuration
- `Bucket` and `Artifact` control access to stored files
- `ResourceRelease` controls access to supported deployed experiences such as APIs

That is why RBAC appears early in the Main Sequence workflow. The moment a resource becomes useful to other people, access design becomes part of the engineering work.

## Rules of thumb

- start with the organization boundary first
- use roles for broad responsibility, not detailed sharing
- use teams for repeatable access patterns
- use direct sharing for exceptional or personal cases
- keep `view` and `edit` separate whenever possible
- do not assume team membership means team administration
- think in terms of resource boundaries: CodeRepository, dataset, secret, bucket, artifact, release

For related configuration guidance, see
[Constants and Secrets](./constants_and_secrets.md).


## Identity facts for application authorization

User detail and current-user responses may supply `is_organization_admin`
(a strict boolean) and `active_team_uids` (active Team UUIDs). They are facts
computed by the platform; the SDK does not infer roles from group names or
implement an application's resource grants. Missing fields remain `None`, so
applications requiring these facts must fail closed rather than assume access.

Use `User.get_authenticated_user_details()` for the process user and
`User.get_by_uid(uid)` for an already verified application caller, subject to the
existing platform directory permissions. These are ordinary User reads; they do
not change the process identity or caller assertion contract. Refresh facts at
the application's authorization boundary to apply Team/admin changes.

The caller can be a person or a workload identity; see
[Workload identities](#workload-identities) for what the lookup returns for a
workload.
