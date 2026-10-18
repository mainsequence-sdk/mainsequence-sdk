# Constants and Secrets

Constants and secrets solve the same broad problem:

> how should a Main Sequence workflow receive configuration at runtime?

They are both small pieces of runtime configuration, but they are not interchangeable.

Use the split below:

- `Constant`: safe operational configuration
- `Secret`: sensitive configuration

If leaking the value would create an incident, it should not be a constant.

## Quick Decision Rule

Use a `Constant` for things like:

- feature flags
- model windows and thresholds
- dataset names
- vendor identifiers
- small JSON mappings
- mode switches such as `production`, `paper`, or `sandbox`

Use a `Secret` for things like:

- API keys
- bearer tokens
- passwords
- webhook signing secrets
- connection credentials
- private vendor access tokens

In practice:

- `MODEL__DEFAULT_WINDOW = 252` is a constant
- `FEATURE__ENABLE_VALIDATION = true` is a constant
- `POLYGON_API_KEY = "..."` is a secret
- `AWS_SECRET_ACCESS_KEY = "..."` is a secret

## Mental Model

Think of constants and secrets as runtime inputs, not workflow outputs.

Jobs, resources, APIs, agents, and notebooks often need configuration that should not live directly inside code. Constants and secrets give you a place to store that configuration in the platform.

A useful mental split is:

- code defines behavior
- constants define non-sensitive runtime settings
- secrets define protected credentials

That keeps the repository cleaner and reduces the amount of environment-specific data hardcoded into scripts and jobs.

Constants and Secrets are owned by an Organization Environment. SDK operations
use the current registered branch's [Environment context](context.md), with
authenticated runtime target checks when deployed. A branch without an
Environment raises only when a scoped operation needs one. Users do not pass an
Environment UID or branch UID.

## Names Are Unique Identities

Treat constant and secret names as Environment-level unique configuration keys.

In practice, this means:

- `MODEL__DEFAULT_WINDOW` should identify one constant, not several variants
- `POLYGON_API_KEY` should identify one secret, not several conflicting rows
- name-based reads such as `Constant.get(name=...)` or `Secret.get(name=...)` assume uniqueness

For idempotent automation or setup flows:

- first resolve whether the constant or secret already exists by name
- use `get(name=...)` when you expect exactly one object
- use `filter(name__in=[...])` when reconciling a batch of keys
- create only the missing names

Do not treat `create(...)` as a substitute for existence checks when the task is "make sure this value exists".

## RBAC and Shareable Resources

Constants and secrets sit inside a broader platform access model.

Main Sequence applies resource-level access control, so the real operational question is not only "what is this value?" but also:

- who can see it
- who can edit it
- which team or CodeRepository boundary it belongs to
- whether it is safe to expose through a deployed release

Constants and secrets are decided by their policies:

- a `Constant`'s policies are `reader`, `editor` and `owner`; see
  [Who can use a constant](#who-can-use-a-constant)
- a `Secret`'s policies are `reader`, `value_reader`, `editor` and `owner`, and
  seeing a secret does not show its value; see
  [Who can use a secret](#who-can-use-a-secret)

`CodeRepository`, `Artifact` and `ResourceRelease` use view and edit sharing
instead; a `Bucket` is decided by its policies too, see
[Who can use a bucket](artifacts.md#who-can-use-a-bucket).

Why these matter:

- `CodeRepository` defines an important collaboration and execution boundary
- `Constant` stores shareable, non-sensitive runtime configuration
- `Secret` stores protected credentials
- `Bucket` holds artifacts and files that may themselves contain sensitive or controlled content
- `Artifact` is the individual stored object that may need controlled visibility
- `ResourceRelease` is the deployment-facing wrapper for supported resources such as APIs

!!! warning "IMPORTANT"
    `Constant` and `Secret` are not the only shareable resources in the SDK.
    `CodeRepository`, `Artifact`, and `ResourceRelease` use view and edit sharing.
    Teams, Organization Environments and Buckets are decided by policies, as `Constant` and `Secret` are; see
    [Who Has Access to an Object](object_access.md).
    This guide focuses on constants and secrets because they are the simplest place to learn the policy pattern.

That is the practical reason this topic matters early:

- constants and secrets teach the access-control model in its simplest form
- the same idea scales later to CodeRepositories, buckets, artifacts, and deployed resources

## Constants

The client model is:

```python
from mainsequence.client import Constant
```

A constant is a small, readable, organization-level configuration value.

The current SDK model exposes:

- `uid`
- `name`
- `value`
- `category`, the prefix of the name before `__`
- `organization_environment_uid` and `organization_environment_name`, the
  Environment that owns it

### What constants are good for

Good fits:

- default windows such as `252`
- tuning parameters such as confidence thresholds
- external dataset names
- reusable small lookup dictionaries
- environment-independent configuration values

Bad fits:

- passwords
- API keys
- secrets copied from other systems
- anything that should not be casually displayed in a UI, notebook, or log

### Naming and categories

Constants are expected to use `UPPER_SNAKE_CASE`.

You can also group constants with a double underscore:

```text
ASSETS__MASTER
MODEL__DEFAULT_WINDOW
FEATURE__ENABLE_VALIDATION
BROKER__DEFAULT_ACCOUNT
```

This does not create a nested object. It is just a naming convention.

Main Sequence treats the prefix before `__` as a human-readable category. For example:

- `ASSETS__MASTER` is displayed under category `ASSETS`
- `MODEL__DEFAULT_WINDOW` is displayed under category `MODEL`

This is useful when you want the constants table to stay readable without inventing a more complex hierarchy.

### Constant examples

Simple scalar value:

```python
Constant.create(
    name="MODEL__DEFAULT_WINDOW",
    value=252,
)
```

Boolean flag:

```python
Constant.create(
    name="FEATURE__ENABLE_VALIDATION",
    value=True,
)
```

Small JSON object:

```python
Constant.create(
    name="BROKER__DEFAULTS",
    value={
        "execution_venue": "paper",
        "account_type": "margin",
    },
)
```

### Reading constants

Read one constant:

```python
from mainsequence.client import Constant

default_window = Constant.get(name="MODEL__DEFAULT_WINDOW").value
```

Read just the value:

```python
default_window = Constant.get_value("MODEL__DEFAULT_WINDOW")
```

Read multiple constants by name:

```python
constants = Constant.filter(
    name__in=[
        "MODEL__DEFAULT_WINDOW",
        "FEATURE__ENABLE_VALIDATION",
    ]
)
```

Current client-side supported filters are:

- `name`
- `name__in`

For idempotent constant setup, prefer resolving by name before create:

```python
existing = {c.name for c in Constant.filter(name__in=["MODEL__DEFAULT_WINDOW"])}
if "MODEL__DEFAULT_WINDOW" not in existing:
    Constant.create(name="MODEL__DEFAULT_WINDOW", value=252)
```

### Who can use a constant

A constant's access is decided by its policies:

- `reader`: see the constant and its value;
- `editor`: also change its name and value, delete it, and see who has access;
- `owner`: also decide who has access.

The person or workload that creates a constant holds `owner`, and Organization
admins hold every action. The policies go to people, workload identities and
Teams. Read and change them with the access methods on `Constant`:

```python
from mainsequence.client import Constant

constant = Constant.get(name="MODEL__DEFAULT_WINDOW")

for binding in constant.get_access().bindings:
    print(binding.uid, binding.policy.id, binding.principal.type, binding.principal.uid)

binding = constant.attach_policy("reader", principal_type="team", principal_uid=team_uid)
constant.detach_policy(binding.uid)

answer = constant.explain_access("constants.update", principal_uid=user_uid)
print(answer.allowed, answer.granting_policies)
```

The access methods address the constant by its UID alone, so they need no
Environment. A refused change raises `PermissionDeniedError`, whose `payload`
names the `required_action` and the `granting_policies`.
`mainsequence access policies --type constants` lists the policies with their
actions, and [Who Has Access to an Object](object_access.md) describes every
method and model.

`Constant` no longer has view and edit sharing methods: `can_view()`,
`can_edit()`, `add_to_view()`, `add_to_edit()`, `remove_from_view()`,
`remove_from_edit()`, their team variants and the `users_can_*` and
`list_users_can_*` aliases are gone. Existing view grants became `reader` and
edit grants `editor`; attach those policies where you used to share.

### When to prefer a constant over code

Use a constant instead of hardcoding when:

- operators may need to change the value without editing source
- the value is reused across several jobs or scripts
- the value is environment or organization specific
- the value is business configuration, not program logic

Examples:

- a rebalance threshold
- a default benchmark name
- a vendor dataset alias
- a list of supported external identifiers

## Secrets

The client model is:

```python
from mainsequence.client import Secret
```

A secret is protected configuration. The platform should treat it differently from normal application settings.

The current SDK model exposes:

- `uid`
- `name`
- `value`
- `organization_environment_uid` and `organization_environment_name`, the
  Environment that owns it

`value` is filled only when the secret is read with `get()` or `get_by_uid()`;
listings and the answers to creating or changing a secret never carry it.

### What secrets are good for

Use a secret for:

- `POLYGON_API_KEY`
- `OPENAI_API_KEY`
- `AWS_SECRET_ACCESS_KEY`
- broker credentials
- database passwords
- signed webhook tokens

If you would be uncomfortable seeing the value in a screenshot, error email, or notebook output, it should be a secret.

### Secret examples

Create a secret:

```python
from mainsequence.client import Secret

Secret.create(
    name="POLYGON_API_KEY",
    value="***",
)
```

Read a secret:

```python
secret = Secret.get(name="POLYGON_API_KEY")
polygon_key = secret.value.get_secret_value() if secret.value is not None else None
```

`Secret.get(name=...)` first resolves the unique Environment-owned metadata row,
then retrieves its value through the UID detail endpoint. `Secret.filter(...)`
always remains metadata-only and never returns secret values. `Secret.value` is a
Pydantic `SecretStr`; unwrap it only where plaintext is required and never log it.
A validation error raised while reading a `Secret` does not include its input, so
it does not show the value either. Reading the value needs the secret's
`value_reader` policy or more; see [Who can use a secret](#who-can-use-a-secret).

Filter secrets by name:

```python
secrets = Secret.filter(name__in=["POLYGON_API_KEY", "OPENAI_API_KEY"])
```

Current client-side supported filters are:

- `name`
- `name__in`

For idempotent secret setup, prefer resolving by name before create:

```python
existing = {s.name for s in Secret.filter(name__in=["POLYGON_API_KEY"])}
if "POLYGON_API_KEY" not in existing:
    Secret.create(name="POLYGON_API_KEY", value="***")
```

### Who can use a secret

A secret's access is decided by its policies:

- `reader`: see the secret, its name and its versions' dates, never its value;
- `value_reader`: also read its current value;
- `editor`: also read earlier values, set a new value, delete it, and see who
  has access;
- `owner`: also decide who has access.

The value needs `value_reader` or more, and earlier values `editor` or more.
`Secret.get()` and `Secret.get_by_uid()` read the current value; without
`value_reader` they raise `PermissionDeniedError`, whose `payload` names the
`required_action`, `secrets.read_value`, and the `granting_policies`.
`Secret.filter()` lists the secrets you can see without their values, so a
`reader` finds a secret by name there. Give a workload that uses a secret
`value_reader`; `reader` is for people who configure workloads that use the
secret but must never know it.

The person or workload that creates a secret holds `owner`, and Organization
admins hold every action. The policies go to people, workload identities and
Teams. A secret the platform manages, such as a hosted database password, cannot
get a new value or be deleted, by anyone: those requests answer `409`. A
repository workflow file's `access` entry for a secret gives its workload
`value_reader` for `level: view` and `editor` for `level: edit`.

Read and change the policies with the access methods on `Secret`:

```python
from mainsequence.client import Secret

secret = Secret.filter(name="POLYGON_API_KEY")[0]

for binding in secret.get_access().bindings:
    print(binding.uid, binding.policy.id, binding.principal.type, binding.principal.uid)

binding = secret.attach_policy(
    "value_reader", principal_type="user", principal_uid=workload_user_uid
)
secret.detach_policy(binding.uid)

answer = secret.explain_access("secrets.read_value", principal_uid=user_uid)
print(answer.allowed, answer.granting_policies)
```

The access methods address the secret by its UID alone, so they need no
Environment. A refused change raises `PermissionDeniedError` too.
`mainsequence access policies --type secrets` lists the policies with their
actions, and [Who Has Access to an Object](object_access.md) describes every
method and model.

`Secret` no longer has view and edit sharing methods: `can_view()`,
`can_edit()`, `add_to_view()`, `add_to_edit()`, `remove_from_view()`,
`remove_from_edit()`, their team variants and the `users_can_*` and
`list_users_can_*` aliases are gone. Existing view grants became
`value_reader`, which reads the current value but not earlier ones, and edit
grants `editor`; attach those policies where you used to share.

### Operational rules for secrets

Secrets need a different operational standard than constants:

- do not commit them to the repository
- do not copy them into constants
- do not print them in logs
- do not paste them into application UIs or notebooks casually
- rotate them when external systems require it

A good working rule is:

- constants may be read frequently and discussed openly
- secrets should be accessed only where they are required

## Human-Readable Examples

### Example 1: Vendor data integration

You are integrating a vendor feed.

Use constants for:

- the vendor dataset name
- a default universe identifier
- a feature flag enabling a fallback path

Use secrets for:

- the vendor API key
- the vendor bearer token

Example split:

- `VENDOR__DATASET = "us_equities_realtime"` as a constant
- `VENDOR__DEFAULT_EXCHANGE = "XNYS"` as a constant
- `VENDOR_API_KEY` as a secret

### Example 2: Model configuration

You have a job that computes signals using a rolling window and a threshold.

Use constants for:

- rolling window
- minimum signal score
- default region

Do not use secrets here unless an external protected service is involved.

Example:

- `MODEL__DEFAULT_WINDOW = 252`
- `MODEL__MIN_SCORE = 0.85`
- `MODEL__REGION = "US"`

### Example 3: External execution venue

You have a workflow that talks to a broker or exchange.

Use constants for:

- default account alias
- execution mode such as `paper` or `live`
- venue configuration labels

Use secrets for:

- API secret
- refresh token
- signing private key

## CLI Usage

The CLI exposes both resources directly.

Constants:

```bash
mainsequence constants list
mainsequence constants create MODEL__DEFAULT_WINDOW 252
mainsequence constants create BROKER__DEFAULTS '{"mode":"paper"}'
mainsequence constants delete <CONSTANT_UID>
```

Secrets:

```bash
mainsequence secrets list
mainsequence secrets create POLYGON_API_KEY your-secret-value
mainsequence secrets delete <SECRET_UID>
```

Important behavior:

- constants display the category derived from the prefix before `__`
- secrets are shown by metadata only in CLI tables and delete previews
- delete commands require typed verification

### CLI access commands for constants

These commands read and change a constant's policies:

```bash
mainsequence constants access list <CONSTANT_UID>
mainsequence constants access grant <CONSTANT_UID> reader --user <USER_UID>
mainsequence constants access grant <CONSTANT_UID> editor --team <TEAM_UID>
mainsequence constants access revoke <CONSTANT_UID> <BINDING_UID>
mainsequence constants access explain <CONSTANT_UID> constants.update
```

Interpretation:

- `<CONSTANT_UID>` is the constant public UID
- `<USER_UID>` is the public UUID of the person or workload identity receiving
  the policy, and `<TEAM_UID>` the team's
- `<BINDING_UID>` is the UID of a binding, which `access list` shows

Unlike the other constants commands, these need no registered branch: the
constant is addressed by its UID alone. The sharing commands `can_view`,
`can_edit`, `add_to_view`, `add_to_edit`, `remove_from_view`,
`remove_from_edit` and their team variants are gone. See
[Constants](../../cli/index.md#constants) in the CLI reference.

### CLI access commands for secrets

These commands read and change a secret's policies:

```bash
mainsequence secrets access list <SECRET_UID>
mainsequence secrets access grant <SECRET_UID> value_reader --user <USER_UID>
mainsequence secrets access grant <SECRET_UID> editor --team <TEAM_UID>
mainsequence secrets access revoke <SECRET_UID> <BINDING_UID>
mainsequence secrets access explain <SECRET_UID> secrets.read_value
```

`<SECRET_UID>` is the secret public UID; `<USER_UID>`, `<TEAM_UID>` and
`<BINDING_UID>` are as for constants. Like the constant access commands, these
need no registered branch. `secrets delete` reads the secret, value included,
before it asks for confirmation, and then deletes it, so it needs `editor` or
`owner`. A refused command prints the platform's message and the policies that
would allow it. The sharing commands `can_view`, `can_edit`, `add_to_view`,
`add_to_edit`, `remove_from_view`, `remove_from_edit` and their team variants
are gone. See [Secrets](../../cli/index.md#secrets) in the CLI reference.

This is the most concrete CLI example of Main Sequence RBAC at the resource level.

## Recommended Practice

A good configuration layout usually looks like this:

- constants contain readable runtime settings
- secrets contain credentials only
- application code reads both, but does not redefine them

A healthy split is:

- store business and runtime configuration in constants
- store credentials and protected tokens in secrets

That gives you:

- better readability
- safer operations
- fewer credentials hardcoded in repository code

## Anti-Patterns

Avoid these:

- storing API keys in constants
- storing large application config blobs in secrets
- hardcoding credentials in source files
- using secrets for ordinary non-sensitive flags
- creating many nearly identical constants when one small JSON object would do

## Final Rule

If the value is safe to read, review, and discuss as part of ordinary repository configuration, use a `Constant`.

If the value must be protected, use a `Secret`.
