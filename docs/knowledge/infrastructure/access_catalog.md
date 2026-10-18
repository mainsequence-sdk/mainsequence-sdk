# Access Policy Catalog

The platform describes access to shareable objects in a catalog that every
signed-in user can read. For each resource type, such as `secrets` or
`resource-releases`, the catalog lists:

- the **actions**: each thing a principal can do on objects of the type, such
  as `secrets.read`, with its access level;
- the **policies**: named bundles of the type's actions. Each policy has three
  texts written for the people who grant it: what the holder can do (summary),
  what can happen because of it (impact), and what it does not allow;
- the **inheritance rules**: how holding a policy on a parent object gives a
  policy on an object of this type;
- the **guards**: platform rules that block some actions on some objects for
  everyone, Organization admins included.

The catalog describes what each policy allows. It does not say who holds a
policy on which object, and reading it changes nothing. Who holds a policy on
an object is read and changed on the object itself, as
[Who Has Access to an Object](object_access.md) describes; teams, Organization
Environments, constants, secrets, buckets, model-provider credentials and
custom model providers work this way. Sharing other objects with view or edit
access works as
[Users and Access](users_and_access.md) describes.

## Read the catalog in Python

`AccessCatalog` reads the catalog with the session the SDK already uses, like
the other clients:

```python
from mainsequence.client import AccessCatalog

catalog = AccessCatalog()

listing = catalog.list_resource_types()
print(listing.catalog_hash)
for resource_type in listing.resource_types:
    print(resource_type.key, resource_type.label, len(resource_type.policies))

secrets = catalog.get_resource_type("secrets")
policies = catalog.list_policies(resource_type="secrets")
policy = catalog.get_policy(policies[0].uid)
actions = catalog.list_actions(resource_type="secrets")
```

| Method | Route | Returns |
| --- | --- | --- |
| `list_resource_types()` | `GET /api/v1/access/resource-types/` | `AccessResourceTypeListing`: `catalog_hash` and every resource type |
| `get_resource_type(key)` | `GET /api/v1/access/resource-types/{key}/` | `AccessResourceType` |
| `list_policies(resource_type=None)` | `GET /api/v1/access/policies/?resource_type=` | `list[AccessPolicy]` |
| `get_policy(uid)` | `GET /api/v1/access/policies/{uid}/` | `AccessPolicy` |
| `list_actions(resource_type=None)` | `GET /api/v1/access/actions/?resource_type=` | `list[AccessAction]` |

Every method also takes `timeout`, as the other clients do. Without
`resource_type`, `list_policies()` and `list_actions()` return the policies or
actions of every resource type; a key the catalog does not have returns an
empty list.

`get_policy()` takes the policy's public UID. To find a policy by its id, look
among the policies of its resource type:

```python
reader = next(
    policy
    for policy in catalog.list_policies(resource_type="secrets")
    if policy.id == "secrets.reader"
)
```

`catalog_hash` identifies the catalog's content and changes whenever the
catalog changes. A program that keeps a copy of the catalog can compare hashes
to know when to read it again.

## What the models carry

`AccessResourceType`:

| Field | Meaning |
| --- | --- |
| `key` | The resource type key, for example `secrets` |
| `label`, `description` | Its name and what objects of the type are |
| `environment_scoped` | Whether objects of the type live in an Organization Environment, so that access to them also requires being admitted to that Environment |
| `actions` | The type's actions, as `AccessAction`; deprecated actions are left out |
| `policies` | The platform's active policies for the type, as `AccessPolicy` |
| `inheritance` | `AccessInheritanceRule` rows: holding `parent_policy` on an object of `parent_type` gives `child_policy` on the related object of this type, through the relation named in `relation` |
| `guards` | `AccessGuard` rows: each rule's `name`, its `description` and the ids of the `actions` it blocks |

`AccessPolicy`:

| Field | Meaning |
| --- | --- |
| `uid` | The policy's public UID, the key `get_policy()` takes |
| `id` | `<resource type>.<policy>`, for example `secrets.reader` |
| `resource_type`, `name` | The two parts of the id |
| `label` | The policy's name for people |
| `summary` | What a holder of the policy can do |
| `impact` | What can happen because someone holds it |
| `not_included` | What it does not allow |
| `actions` | Ids of the actions it contains, for example `secrets.read` |
| `attachable` | `False` for a policy the platform computes and never attaches, such as the policy Team members hold on their Team |
| `principal_kinds` | Who it can be given to: `people`, `workloads` and `teams`; empty when it is not attachable |
| `stage` | `active`, or `deprecated` for a policy that is being retired |

`AccessAction`:

| Field | Meaning |
| --- | --- |
| `id` | `<resource type>.<action>`, for example `secrets.read` |
| `resource_type`, `key` | The two parts of the id |
| `label`, `description` | Its name and what it lets a principal do |
| `access_level` | `read`, `use`, `write` or `permissions` |
| `is_deprecated` | Whether the action is deprecated |

The access levels say how far an action reaches:

- `read`: see configuration;
- `use`: touch data or run something;
- `write`: change, operate or delete;
- `permissions`: see or decide who has access, or manage a workload's access.

`access_level`, `principal_kinds` and `stage` are read tolerantly: a value this
SDK release does not declare is kept as the platform sent it and logged once,
so it does not fail the call. Fields the release does not declare are dropped.

## Errors

A failed request raises the SDK's API errors:

- `NotFoundError`, which is also a `DoesNotExist`, for a resource type key or a
  policy UID the catalog does not have;
- `AuthenticationError` when there is no valid session;
- `TransportError` when the platform cannot be reached.

`get_resource_type()` with an empty key, `list_policies()` and
`list_actions()` with an empty `resource_type`, and `get_policy()` with a value
that is not a UUID raise `ValueError` before any request is sent.

## From the command line

```bash
mainsequence access policies
mainsequence access policies --type secrets
mainsequence access policies show secrets.reader
mainsequence access policies show <POLICY_UID>
mainsequence access policies --type secrets --json
```

`mainsequence access policies` lists each policy's id, label, who it can be
given to and summary; `--type` limits the list to one resource type.
`mainsequence access policies show` takes a policy id such as `secrets.reader`,
or a policy UID, and prints the policy's summary, impact, what it does not
include and its actions. Both commands accept `--json` and `--timeout`; see the
[CLI reference](../../cli/index.md#access-policies).
