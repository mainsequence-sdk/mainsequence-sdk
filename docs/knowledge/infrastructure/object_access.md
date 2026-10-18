# Who Has Access to an Object

When the platform decides an object's access with policies, the object serves
its own access routes. A policy from the
[Access Policy Catalog](access_catalog.md) is attached to a principal, a
person, a workload identity or a Team, on one object. That attachment is a
**binding**.

Teams, Organization Environments, constants, secrets, buckets, code
repositories, their branches, Projects, model-provider credentials and custom
model providers are decided this way. Other shareable objects, such as
artifacts, agents and releases, still use view and edit sharing, as
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

A Secret has four policies: `reader` sees it, never its value; `value_reader`
also reads its current value; `editor` also reads earlier values, sets a new
value, deletes it and sees who has access; `owner` also decides who has access.
The person or workload that creates a Secret holds `owner`. The policies go to
people, workload identities and Teams with the same methods on `Secret`, which
no longer has view and edit sharing methods either, and which need no
Environment:

```python
from mainsequence.client import Secret

secret = Secret.filter(name="POLYGON_API_KEY")[0]
secret.attach_policy("value_reader", principal_type="user", principal_uid=workload_user_uid)
```

Reading a Secret's value without `value_reader`, with `Secret.get()` or
`Secret.get_by_uid()`, raises `PermissionDeniedError` with `required_action`
`secrets.read_value`. See
[Constants and Secrets](constants_and_secrets.md#who-can-use-a-secret).

A Bucket has four policies: `reader` browses its folders and the Artifacts in
it that are shared with the person; `contributor` also uploads Artifacts and
creates folders; `editor` also renames the Bucket, renames and deletes folders,
deletes the Bucket with every Artifact in it, and sees who has access; `owner`
also decides who has access. The person or workload that creates a Bucket
holds `owner`, also when an upload creates it by name. The policies go to
people, workload identities and Teams with the same methods on `Bucket`, which
no longer has view and edit sharing methods either, and which need no
Environment:

```python
from mainsequence.client import Bucket

bucket = Bucket.get(name="vendor_prices")
bucket.attach_policy("contributor", principal_type="user", principal_uid=workload_user_uid)
```

Uploading into an existing Bucket without `contributor`, with
`Artifact.upload_file()`, raises `PermissionDeniedError` with
`required_action` `buckets.add_artifact`. See
[Artifacts](artifacts.md#who-can-use-a-bucket).

A code repository has three policies: `reader` sees it, its GitHub link and
issues, and every branch in it; `editor` also renames or archives it, adds
deploy keys, creates branches, edits every branch and sees who has access;
`owner` also decides who has access. Its creator holds `owner`, and deleting a
repository is in no policy: only Organization admins delete one. A branch has
`reader`, `editor` and `owner` too, and a branch's creator holds nothing for
being its creator. A Project, a named group of branches in one Environment, has
`reader`, `editor` and `owner`, and its creator holds `owner`. A repository's
policies pass down to every branch of it, and a Project's to its branches:
`reader` gives branch `reader`, and `editor` and `owner` give branch `editor`.
`get_access()` on a branch shows those as `inherited` rows. The same methods are
on `CodeRepository`, `CodeRepositoryBranch` and `OrganizationProject`, which
carries only the Project's UID:

```python
from mainsequence.client import CodeRepository, OrganizationProject

repository = CodeRepository.get_by_uid(code_repository_uid)
repository.attach_policy("editor", principal_type="team", principal_uid=team_uid)

project = OrganizationProject(uid=project_uid)
project.attach_policy("reader", principal_type="user", principal_uid=user_uid)
```

Using a repository or a branch needs the platform's `user` product role: a
person without it gets `PermissionDeniedError` with `code`
`missing_product_role` from every route of either, the access methods
included, also when a Team they belong to holds a policy. A repository's
policies go only to people with that role, to workload identities and to
Teams. The access methods of all three take no Environment, and the platform
refuses them to runtime credentials. See
[Code Repositories, Branches and Projects](code_repositories.md).

Model-provider credentials and custom model providers are decided by policies
too, as two types, each with three policies:

- a model-provider credential, one person's sign-in to a built-in model
  provider in one Environment: `user` selects it and receives it in sessions
  and workloads, including local Tau, where it can be copied; `editor` also
  refreshes it, signs in to the provider again, revokes it and sees who has
  access; `owner`, which the credential's person holds, also decides who has
  access;
- a custom model provider, an inference endpoint with its own authentication
  and models: `user` sends requests through it and its models; `editor` also
  changes its endpoint, authentication and models, deletes it and sees who has
  access; `owner`, which its creator holds, also decides who has access.

The policies go to people, workload identities and Teams. The SDK has no model
for these records, so it has no access methods for them; selecting a
credential or a custom provider for a session or an inference request needs its
`use` action, which all three policies include. Existing view grants became
`user` and edit grants `editor`, and a repository workflow file's `access`
entry for model providers gives `user` for `level: view` and `editor` for
`level: edit`.

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
| `apply_access_changes(attach=[...], detach=[...])` | `POST {object}/access/changes/` | the bindings after the changes that the platform lists to you |
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
  It answers with every binding on the object when you hold the object's
  `read_access` action, and otherwise with only your own bindings and those of
  the workload identities you manage.
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
  So is a policy that passes down from a parent object, with who holds it, and
  the creator's rights that the managers of a creating workload identity hold.

## What the models carry

`AccessListing`:

| Field | Meaning |
| --- | --- |
| `object` | `AccessObjectReference`: the object's `resource_type`, such as `organization-teams`, and `uid` |
| `bindings` | The policies attached to the object, as `AccessBinding` |
| `computed` | `AccessComputedRow` rows: access no binding stores, below |

`AccessComputedRow`:

| Field | Meaning |
| --- | --- |
| `source` | `role` (Organization admins), `membership` (a Team's members, who hold `organization-teams.member`), `inherited` (one holder of a policy on a parent object that passes down) or `manager` (the managers of the workload identity that created the object) |
| `detail` | Who receives the access, in words |
| `policy` | The policy id the access amounts to. `None` on a `role` row, because admins hold every action, and on a `manager` row, because the managers hold the creator's rights |
| `principal` | `AccessPrincipal`: who holds the parent policy, on an `inherited` row; `None` on the others |
| `relation` | How the object relates to the parent, on an `inherited` row, for example `full-directory-team`: the full-directory team's `viewer` policy gives `viewer` on every team |
| `parent` | `AccessObjectReference`: the parent object's `resource_type` and `uid`, on an `inherited` row |
| `parent_policy` | The policy id held on the parent, on an `inherited` row |

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
- A code repository or a branch refuses a person without the platform's
  `user` product role with `PermissionDeniedError` too, whose `payload` has
  `code` `missing_product_role` and no `granting_policies`: no policy gives
  that role.
- An object you cannot read raises `NotFoundError`.
- An empty policy or action, a `principal_type` other than `user` or `team`, an
  `identity_type` other than `human` or `workload`, a UID that is not a UUID,
  and an `apply_access_changes()` call without any change raise `ValueError`
  before anything is sent.

## From the command line

`mainsequence organization teams access list|grant|revoke|explain` reads and
changes a team's policies, `mainsequence constants access
list|grant|revoke|explain` a constant's, `mainsequence secrets access
list|grant|revoke|explain` a secret's, and `mainsequence code-repository access
list|grant|revoke|explain` a code repository's; see
[Teams](../../cli/index.md#teams), [Constants](../../cli/index.md#constants),
[Secrets](../../cli/index.md#secrets) and
[Code repositories](../../cli/index.md#code-repositories) in the CLI reference.
The CLI has no access commands for branches and Projects.
