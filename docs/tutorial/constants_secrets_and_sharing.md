# Part 2.1: Constants, Secrets, and Sharing

## Quick Summary

In this part, you will:

- separate code from runtime configuration
- learn when to use `Constant` versus `Secret`
- understand the Main Sequence RBAC model at a practical level
- see how access can be controlled at the resource level
- create and share constants from the CLI

DataNodes created in this part: **none**.

Platform resources introduced in this part: **`Constant`**, **`Secret`**, **`Project`**, **`Bucket`**, and **`ResourceRelease`**.

## Why this comes right after your first DataNode

Once you have created your first `DataNode`, the next mistake people usually make is hardcoding every runtime setting directly inside the codebase.

That works for a prototype, but it breaks down quickly:

- values change without changing the logic
- different users need different access levels
- some values are safe to share and some are not
- deployed resources need controlled visibility

This is where constants, secrets, and sharing enter the picture.

## Constants vs secrets

The simplest rule is:

- use a `Constant` for safe operational configuration
- use a `Secret` for protected values

Examples of constants:

- default model windows
- feature flags
- dataset identifiers
- small JSON configuration objects
- mode switches such as `paper` or `production`

Examples of secrets:

- API keys
- passwords
- bearer tokens
- signing credentials
- private vendor access tokens

If you would be uncomfortable seeing the value in a screenshot or notebook output, it should be a secret.

## RBAC in Main Sequence

Main Sequence is not only a place to store data and run code. It is also a platform with resource-level access control.

The practical question is:

> who can see this resource, who can edit it, and who can use it safely in production workflows?

That is the RBAC layer in practice.

The important mental model is that access is not only attached to users. It is attached to platform resources.

Examples of resources where access boundaries matter:

- `Project`
- `Constant`
- `Secret`
- `Bucket`
- `ResourceRelease`

Why each one matters:

- `Project`: controls who can work inside a code and execution boundary
- `Constant`: controls who can read or change shared operational settings
- `Secret`: controls who can use protected credentials
- `Bucket`: controls access to stored files and artifacts
- `ResourceRelease`: controls visibility of deployable outputs such as dashboards and agents

This is important because collaboration in Main Sequence is usually not all-or-nothing. Different people may need different visibility or edit rights depending on the resource.

!!! warning "IMPORTANT"
    In the current SDK and CLI, the clearest direct sharing examples are `Constant` and `Secret`.
    Those objects expose explicit `can_view`, `can_edit`, `add_to_view`, and `add_to_edit` flows.
    Other resources such as projects, buckets, and resource releases still participate in access control, but their governance is typically enforced through the broader project and platform permission model rather than through the exact CLI commands shown below.

## CLI example: create constants

Start with a simple constant:

```bash
mainsequence constants create MODEL__DEFAULT_WINDOW 252
```

Create a small JSON constant:

```bash
mainsequence constants create BROKER__DEFAULTS '{"mode":"paper","account_type":"margin"}'
```

List the constants visible to you:

```bash
mainsequence constants list
```

Inspect supported list filters:

```bash
mainsequence constants list --show-filters
```

Filter to a specific set of names:

```bash
mainsequence constants list --filter name__in=MODEL__DEFAULT_WINDOW,BROKER__DEFAULTS
```

The category shown in the CLI is derived from the prefix before `__`.

For example:

- `MODEL__DEFAULT_WINDOW` appears under category `MODEL`
- `BROKER__DEFAULTS` appears under category `BROKER`

## CLI example: share a constant

Once a constant exists, you can inspect and manage who can see or edit it.

Check who can view constant `42`:

```bash
mainsequence constants can_view 42
```

Check who can edit constant `42`:

```bash
mainsequence constants can_edit 42
```

Grant user `7` view access:

```bash
mainsequence constants add_to_view 42 7
```

Grant user `7` edit access:

```bash
mainsequence constants add_to_edit 42 7
```

Remove those permissions again if needed:

```bash
mainsequence constants remove_from_view 42 7
mainsequence constants remove_from_edit 42 7
```

This is the simplest concrete example of resource-level sharing in the platform.

## Reading constants from code

Once a constant has been created, your code can read it without hardcoding the value:

```python
from mainsequence.client import Constant

default_window = Constant.get_value("MODEL__DEFAULT_WINDOW")
```

That keeps the code stable while still allowing operators to change runtime settings centrally.

## Secrets follow the same pattern, with stricter handling

The CLI also exposes direct secret management:

```bash
mainsequence secrets create POLYGON_API_KEY your-secret-value
mainsequence secrets list
mainsequence secrets can_view 42
mainsequence secrets can_edit 42
```

The operational rule is stricter:

- do not print secret values in logs
- do not copy them into constants
- do not hardcode them in the repository

## Why this matters for the rest of the tutorial

Later parts of the tutorial will introduce:

- scheduled jobs
- artifacts and buckets
- dashboards
- resource releases

At that point, controlled access is no longer optional. You need a clear split between:

- code
- readable runtime configuration
- protected credentials
- shareable platform resources

That is why this chapter belongs early in the tutorial sequence.

For a deeper reference, see [Constants and Secrets Knowledge Guide](../knowledge/infrastructure/constants_and_secrets.md).
