# ADR 0033: Read Closed Value Sets Tolerantly

Amended 2026-09-25 for [SDK issue #162](https://github.com/mainsequence-sdk/mainsequence-sdk/issues/162):
public ResourceRelease collection creation is retired. The historical
`ResourceRelease.create` sending examples below are superseded; the SDK now
blocks inherited create calls before HTTP. Tolerant response reading and
closed request parameter validation remain in force for supported operations.

Date: 2026-09-21

Status: Accepted and implemented

Issue: `mainsequence-sdk` #119, and the half of #120 that ADR 0032 left open.
Stable fingerprint:
`mainsequence-sdk|response-parsing|strict-reader-breaks-on-compatible-backend-change`

## Context

ADR 0032 made the SDK ignore a response *field* it does not declare. It left the
other half of #120 open, and named it: a response *value* the SDK does not
declare still raised.

#119 is that half, in a user's hands. The backend persists `release_kind`
`harness_agent`; `ResourceReleaseKind` listed `agent`, `fastapi` and
`static_site`. One such release in a branch made `ResourceRelease.filter(...)`
raise before the caller could read any release in the listing:

```text
1 validation error for ResourceRelease
release_kind
  Input should be 'agent', 'fastapi' or 'static_site'
```

The same file already read `harness_agent` in `ResourceReleaseRuntimeAccess`, so
the two response models disagreed about what a release kind is.

This is the same defect as the undeclared field, in a second place. The backend's
API rules call a new documented enum member a compatible change; about 60
`Literal[...]` fields and five `str, Enum` classes called it fatal. A state, a
phase or a kind the backend adds is exactly the kind of value a user often does
not read, and it failed the whole row, and the whole listing the row was in.

## Decision

The SDK reads a closed value set tolerantly when the value comes from the
backend, and keeps it closed when the value goes to the backend.

1. **One mechanism, in `mainsequence/client/value_sets.py`.**
   - `OpenStrEnum` replaces `str, Enum` for response vocabularies. A declared
     value still validates to its member, so `kind is ResourceReleaseKind.AGENT`
     and `kind == "agent"` keep working. An undeclared value becomes a
     member-like object carrying that exact string.
   - `OpenValueSet[Literal[...]]` replaces `Literal[...]` on a response field.
     It reads as a `str` that keeps an undeclared value.
2. **The values stay inside a real `Literal`.** `OpenValueSet` wraps a `Literal`
   rather than taking loose strings, so the declared values keep one spelling,
   an alias shared by a request parameter and a response field keeps one
   definition, and tools that rewrite annotations leave them alone.
3. **An undeclared value is reported once.** One `warning` names the model, the
   field, the value and the declared values. Once per process per distinct
   value, not once per row, so a listing of 500 rows carrying one new state is
   one line.
4. **An undeclared value is not a declared member.** It is deliberately not
   registered on the enum, so iterating the enum lists only what this release
   declares, and `value not in set(TheEnum)` is how code asks.
5. **`harness_agent` is declared.** `ResourceReleaseKind` gains it, and
   `ResourceReleaseRuntimeAccess.release_kind` reads that same enum instead of
   its own `Literal`, so the release and its runtime access name a kind the same
   way (#119).
6. **What the SDK sends stays closed.** A request parameter typed `Literal[...]`
   still rejects an undeclared value, because there the mistake is the caller's
   and raising is what catches it. Direct ResourceRelease creation is retired;
   repository workflows provide the creation input.
7. **An unknown discriminator is unchanged.** `AgentSession.get_insights`
   dispatches on `harness` and raises for a harness it cannot place. That is one
   object, fetched by itself, so the error is already scoped to the object that
   carries it and never fails a listing, which is what #120 asked for.

### What opened, and what did not

Opened: the response vocabularies — release kind, runtime state, phase, wake
state, admission state, severity, deployment-run step kind and state, pricing
state, log stream and level, log-source state, agent runtime state, interaction
state and action type, runtime access mode, streaming status, environment log
owner type and truncation reason, public log level, GitHub issue state and
operation type and status, image provisioning and verification state,
notification source and type, access level, MetaTable management,
schema-management, provisioning and storage-access modes.

Left closed, deliberately:

- **Request parameters** — `action: Literal["archive", "unarchive"]`,
  `output_format`, `state_reason`, the MetaTable request payloads. #120 put
  these out of scope, and they are right as they are.
- **Constant tags** — `kind: Literal["task"]`, `scope: Literal["knative_runtime"]`,
  `harness: Literal["pi"]`, `confirm_cascade_delete: Literal[True]`. These are
  not vocabularies; widening them would remove the tag.
- **`DataFrequency`** — the SDK builds these locally for the data interfaces;
  none is parsed from a response.
- **`mainsequence/code_repository_context.py`** — plain dataclasses, so their
  `Literal` annotations are hints that never validate at runtime.

## Consequences

- A backend release that adds a state, a phase or a kind no longer breaks
  installed SDKs, and one such row no longer fails the listing it is in.
- The static type of an opened `Literal` field is now `str`. Editors no longer
  complete the declared values at the field; the values stay readable in the
  source, and `declared_values()` reads them back for tests and documentation.
- An undeclared enum value is a member-like object that is not in the enum.
  Code that asks `kind is ResourceReleaseKind.AGENT` or
  `kind in set(ResourceReleaseKind)` keeps a correct answer. Code that assumed
  every parsed value is a declared member gets a value that compares equal to a
  string and to nothing else.
- Two `OpenStrEnum` objects built from the same undeclared value are equal but
  not identical, because neither is registered. `==` is how to compare them.
- A test that asserted a retired value is rejected now asserts that the release
  does not declare it and that a response carrying it parses. Two such tests
  changed: the branch provisioning-state contract in
  `tests/test_code_repository_branch_contract.py`, and the retired-release-kind
  contract in `tests/test_filter_normalization.py`.
- The SDK is now silent about drift by default except for one log line. The
  backend's pre-merge comparison against the newest SDK release is what reports
  it, as ADR 0032 already assumed.

## Alternatives considered

### Declare `harness_agent` and stop there

Rejected. It fixes #119 and leaves the defect. Seven closed issues under #120 are
one field each; this would have been the eighth, for a value instead of a field.

### `Literal[...] | str` on each response field

Rejected. It reads as "anything", gives no place to report an undeclared value,
and loses the declared values entirely — they survive in `OpenValueSet` and are
readable through `declared_values()`.

### `OpenLiteral["a", "b"]` taking loose strings

Rejected after trying it. `ruff check --fix` reads bare strings in a subscript as
forward-reference annotations and strips their quotes, silently turning
`OpenLiteral["a", "b"]` into `OpenLiteral[a, b]`. Wrapping a real `Literal` is
safe from that, and reuses a `Literal` alias directly.

### Register the undeclared value on the enum

Rejected. Iterating the enum would then list values that depend on what the
process happened to read, so declared membership stays stable.

## Required Invariants

- Parsing a response with an undeclared enum or literal value never raises.
- One row with an undeclared value never fails a listing.
- `ResourceRelease.filter(...)` returns a listing containing a `harness_agent`
  release, with `release_kind` readable as `"harness_agent"`.
- `ResourceRelease` and `ResourceReleaseRuntimeAccess` read the same release-kind
  vocabulary.
- Reading an undeclared value does not add it to the enum.
- Direct ResourceRelease collection creation is blocked before an HTTP request.
- An undeclared value is reported once, naming the model, the field and the value.
- `tests/test_tolerant_value_sets.py` holds these rules.
