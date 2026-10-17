# Who Has Access to an Object

When the platform decides an object's access with policies, the object serves
its own access routes. A policy from the
[Access Policy Catalog](access_catalog.md) is attached to a principal, a
person, a workload identity or a Team, on one object. That attachment is a
**binding**.

Teams, Organization Environments and constants are decided this way. Other
shareable objects, such as secrets, buckets, artifacts, CodeRepositories and
releases, still use view and edit sharing, as
[Users and Access](users_and_access.md#resource-sharing) describes.

Working in an Environment is a policy. `member` works in it, opens what is
shared with the person there, and creates Projects, Buckets, Constants,
Secrets, model providers and repositories in it; `observer` works in it
without creating anything. Both go to people and Teams, never to workload
identities, and only Organization admins attach or detach them, with the same
methods on `OrganizationEnvironment`:

```python
from mainsequence.client import OrganizationEnvironment

environment = OrganizationEnvironment.get_by_uid(environment_uid)
environment.attach_policy("observer", principal_type="team", principal_uid=team_uid)
```

Being admitted to an Environment never shows objects that are not shared with
the person. Creating inside it without the create action, for example a
Constant as an `observer`, raises `PermissionDeniedError` with
`required_action` `organization-environments.create_constant`.

A Constant has three policies: `reader` sees it and its value; `editor` also
changes its name and value, deletes it and sees who has access; `owner` also
decides who has access. The person or workload that creates a Constant holds
`owner`. The policies go to people, workload identities and Teams, with the
same methods on `Constant`, which no longer has view and edit sharing methods:

```python
from mainsequence.client import Constant

constant = Constant.get(name="MODEL__DEFAULT_WINDOW")
constant.attach_policy("reader", principal_type="team", principal_uid=team_uid)
print(constant.get_my_access().allowed_actions)
```

The access methods address a Constant by its UID alone, so they need no
Environment; reading the Constant, as `Constant.get()` does above, uses the
current branch's.

## Read and change access in Python

Every model whose object serves these routes has the same methods, from
`AccessManagedObjectMixin`. `Team` is one:

```python
from mainsequence.client import Team

team = Team.get_by_uid(team_uid)

listing = team.get_access()
for binding in listing.bindings:
    print(binding.uid, binding.policy.id, binding.principal.type, binding.principal.uid)
for row in listing.computed:
    print(row.source, row.policy, row.detail)

mine = team.get_my_access()  # mine.allowed_actions, mine.policies

candidates = team.list_access_candidates("membership_manager", search="ana")
binding = team.attach_policy(
    "membership_manager",
    principal_type="user",
    principal_uid=candidates[0].uid,
)
team.detach_policy(binding.uid)

answer = team.explain_access("organization-teams.update", principal_uid=user_uid)
print(answer.allowed, [path.source for path in answer.paths], answer.granting_policies)
```

| Method | Route | Returns |
| --- | --- | --- |
| `get_access()` | `GET {object}/access/` | `AccessListing`: the object, its bindings and the computed access |
| `get_my_access()` | `GET {object}/access/me/` | `CallerAccess`: your allowed actions and the policies you hold |
| `list_access_candidates(policy, principal_type="user", identity_type=None, search=None)` | `GET {object}/access/candidates/` | `list[AccessPrincipal]`, at most 200 |
| `attach_policy(policy, principal_type=..., principal_uid=...)` | `POST {object}/access/bindings/` | the new `AccessBinding` |
| `detach_policy(binding_uid)` | `DELETE {object}/access/bindings/{binding_uid}/` | `None` |
| `apply_access_changes(attach=[...], detach=[...])` | `POST {object}/access/changes/` | the object's bindings after the changes |
| `explain_access(action, principal_uid=None)` | `GET {object}/access/explain/` | `AccessExplanation` |

`{object}` is the object's detail route, for example `/api/v1/teams/{uid}`.
Every method also takes `timeout`.

- `policy` is a policy name of the object's type, such as `viewer`, or its
  full id, such as `organization-teams.viewer`. `mainsequence access policies
  --type organization-teams` lists them.
- `principal_type` is `user` for a person or a workload identity, and `team`
  for a Team. `principal_uid` is the UID, or an object or a dict that carries
  it as `uid`, such as a `User`, a `Team` or an `AccessPrincipal`.
- `list_access_candidates()` lists people by default. `identity_type="workload"`
  lists workload identities, `principal_type="team"` lists Teams, and `search`
  narrows people by email and Teams by name. A policy that cannot be given to
  that kind of principal returns an empty list.
- `apply_access_changes()` takes `attach` entries as mappings with `policy`,
  `principal_type` and `principal_uid`, and `detach` entries as binding UIDs or
  `AccessBinding` objects. The platform applies every change or none of them.
- `explain_access()` takes an action id of the object's type, such as
  `organization-teams.update`. Without `principal_uid` it explains your own
  access.

## Who may read and change access

- Reading the bindings, and explaining someone else's access, needs the
  object's `read_access` action. Reading your own access and explaining it
  needs only read access to the object.
- Attaching a policy needs the object's `manage_access` action and every
  action of the policy. The managers of a workload identity may also give that
  workload a policy whose actions they hold on the object. Listing candidates
  needs the same.
- Detaching needs `manage_access`. Removing your own binding, and removing the
  binding of a workload identity you manage, are always allowed.
- Anyone who is not an Organization admin gives policies only to people who
  share an active Team with them, to Teams they belong to, and to workload
  identities they can view. A workload identity never gives another workload
  identity access.
- Organization admins hold every action on their Organization's objects. That
  access is computed, not stored: `get_access()` shows it as a `computed` row.

## What the models carry

`AccessListing`:

| Field | Meaning |
| --- | --- |
| `object` | `AccessObjectReference`: the object's `resource_type`, such as `organization-teams`, and `uid` |
| `bindings` | The policies attached to the object, as `AccessBinding` |
| `computed` | `AccessComputedRow` rows: access no binding stores. `source` is `role` for Organization admins, whose `policy` is `None` because they hold every action, or `membership` for Team members, who hold `organization-teams.member`. `detail` says it in words |

`AccessBinding`:

| Field | Meaning |
| --- | --- |
| `uid` | The binding's UID, which `detach_policy()` takes |
| `policy` | `AccessPolicyReference`: the policy's `id` and `label` |
| `principal` | `AccessPrincipal`: who holds it |
| `source` | Why it exists: `explicit` (someone attached it), `creator` (given to the object's creator), `workflow` (declared in a repository workflow file) or `migrated` (converted from an earlier view or edit grant). The source grants nothing by itself |
| `granted_by` | `AccessGrantor`: the `uid` and `email` of the User who attached it, or `None` |
| `workflow_declaration` | `AccessWorkflowDeclaration`: the workflow file's `path` and the declaration's `key`, for a `workflow` binding; otherwise `None` |
| `created_at` | When it was created |

`AccessPrincipal`:

| Field | Meaning |
| --- | --- |
| `type` | `user` for a person or a workload identity, `team` for a Team |
| `uid` | The User's or the Team's UID |
| `name` | A person's name, or their email when they have none; a Team's name; empty for a workload identity |
| `email` | A person's email; `None` for a workload identity and for a Team |
| `identity_type` | A User's identity type, such as `human` or `workload`; `None` for a Team |

`CallerAccess` has `allowed_actions` and `policies`, both lists of ids.

`AccessExplanation`:

| Field | Meaning |
| --- | --- |
| `action` | The action explained |
| `allowed` | Whether the principal may perform it |
| `in_scope` | Whether the object is within the principal's reach at all: their Organization and, for objects that live in an Environment, an Environment they are admitted to |
| `paths` | `AccessExplanationPath` rows, one for each way the action is given. `source` is `role`, `direct` (a binding to the principal), `team` (a binding to a Team they belong to, named by `team_uid`), `membership` (membership of the Team itself), `manager` (managing the workload identity that created the object) or `inherited` (a policy held on a related object, named by `relation` and `parent_policy`). `policy`, `binding_uid` and `detail` say more when they apply |
| `blocked_by_rule` | Whether a platform rule blocks the action on this object for everyone, Organization admins included |
| `granting_policies` | The policies that contain the action: what to ask for |

`source`, `type` and `identity_type` are read tolerantly: a value this SDK
release does not declare is kept as the platform sent it and logged once.
Fields the release does not declare are dropped.

## Errors

- A refused request raises `PermissionDeniedError`. Its `payload` is the
  platform's answer: `detail`, `code` `missing_action`, `required_action`,
  `granting_policies` (the policies that would allow it), `resource_type` and
  `resource_uid`.

  ```python
  from mainsequence.client.exceptions import PermissionDeniedError

  try:
      team.attach_policy("owner", principal_type="user", principal_uid=user_uid)
  except PermissionDeniedError as error:
      print(error.payload["required_action"], error.payload["granting_policies"])
  ```

- A change the platform does not accept raises `BadRequestError`, whose
  `payload` names the reason in `code`. For example, `invalid_principal_uids`
  is someone outside the people you can give access to,
  `principal_kind_not_allowed` a policy that cannot be given to that kind of
  principal, and `policy_not_attachable` a computed policy such as
  `organization-teams.member`.
- An object you cannot read raises `NotFoundError`.
- An empty policy or action, a `principal_type` other than `user` or `team`, an
  `identity_type` other than `human` or `workload`, a UID that is not a UUID,
  and an `apply_access_changes()` call without any change raise `ValueError`
  before anything is sent.

## From the command line

`mainsequence organization teams access list|grant|revoke|explain` reads and
changes a team's policies, and `mainsequence constants access
list|grant|revoke|explain` a constant's; see [Teams](../../cli/index.md#teams)
and [Constants](../../cli/index.md#constants) in the CLI reference.
