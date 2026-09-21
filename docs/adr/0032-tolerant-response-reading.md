# ADR 0032: Read Backend Responses Tolerantly

Date: 2026-09-21

Status: Accepted and implemented for undeclared fields. Closed value sets
(`Enum` and `Literal` members) were left strict here and are decided by
[ADR 0033](0033-tolerant-value-set-reading.md)

Issue: `mainsequence-sdk` #120, stable fingerprint
`mainsequence-sdk|response-parsing|strict-reader-breaks-on-compatible-backend-change`

## Context

`BasePydanticModel` was `extra="forbid"`, and every list, detail, create and
patch response is passed straight into those models. A backend serializer that
gained one field therefore failed every call to that resource, in the user's
running process, for a field that user usually does not read. The backend's API
rules treat a new optional response field as a compatible change, so the SDK was
the only party calling it fatal.

The defect was patched one field at a time — #82, #88, #90, #95, #108, #115,
#118 — and the backend keeps serving fields no released SDK declares
(`Notification.created_by_user`, `Organization.is_org_admin`, `stats`).

Strict parsing was the SDK's only way to notice it had drifted from the backend.
The backend now compares the newest SDK release with what it serves before a
backend change is merged, so drift is found there, before release, instead of in
a user's process after it.

## Decision

The SDK reads backend responses tolerantly.

1. **One base class, tolerant.** `BasePydanticModel` is `extra="ignore"`. A
   response field the installed release does not declare parses and is dropped.
2. **The PATCH response follows the same rule.** `BaseObjectOrm._patch_by_reference`
   copies response keys onto the instance; it now skips keys the target model
   does not declare, unless that model keeps extras (`extra="allow"`, used by
   the enriched observability rows).
3. **A model that needs strictness declares it.** `extra="forbid"` or
   `extra="allow"` on a single model still wins over the base. The models that
   already declare either keep their behaviour unchanged.
4. **Closed value sets stay strict.** Unknown `Enum` and `Literal` members, and
   unknown discriminators, still raise. That is the other half of #120 and is
   tracked by #119; this ADR does not decide it.

   Superseded by [ADR 0033](0033-tolerant-value-set-reading.md), which opens the
   response vocabularies and closes #119.

## Consequences

- A backend release that adds a response field no longer breaks installed SDKs.
  The field is invisible to the SDK until a release declares it.
- Request models lose their typo protection. `BasePydanticModel` also backs the
  models a user fills in to send data, so a misspelled keyword that reaches such
  a model is now dropped instead of raising. This cost was accepted to keep one
  base class and one rule; a model whose input must stay closed declares
  `extra="forbid"` itself, as
  `mainsequence/meta_tables/time_index_table_updates/configuration.py` does.
- A dropped field is dropped silently. Nothing in a response tells the user that
  the backend sent more than this release reads; the backend's pre-merge
  comparison is what reports the drift.
- A field retired from a model is no longer rejected on the way in — it is
  ignored. Tests that guarded a retired field now assert that the model does not
  declare it and that a payload carrying it parses without the attribute.
- A listing still fails as a whole on an unknown enum or literal value, which is
  #119. ADR 0033 lifts this.

## Alternatives considered

### Split the base class into a tolerant response base and a strict request base

Rejected for now because it means classifying all 139 models, and many serve
both directions: the same class is parsed from a response and filled in to be
sent. The split remains the way to restore request-side strictness if dropped
keyword typos prove costly.

### Keep `extra="forbid"` and declare each new field as the backend ships it

Rejected. That is the loop this ADR exists to end: seven closed issues, one per
field, each of which broke every installed SDK until users upgraded.

### `extra="allow"` instead of `extra="ignore"`

Rejected. Keeping undeclared values would put untyped, undocumented attributes
on public models and make `model_dump()` round-trip fields the SDK does not
understand. Rows that genuinely carry open provider payloads opt in explicitly.

## Required Invariants

- Parsing a response with an undeclared field never raises, for every resource
  model.
- One row with an undeclared field never fails a listing.
- A PATCH response with an undeclared field applies the declared keys and drops
  the rest.
- A model that declares `extra="forbid"` or `extra="allow"` keeps that behaviour.
- `tests/test_tolerant_response_reading.py` holds these rules.
