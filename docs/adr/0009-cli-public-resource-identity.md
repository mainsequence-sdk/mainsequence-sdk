# ADR 0009: CLI Public Resource Identity

Date: 2026-05-25

Status: Proposed

## Related ADRs

- ADR 0007: Client-Wide UID Public Identity

## Context

The CLI is inconsistent about public resource identity.

Today it mixes multiple patterns:

- numeric `id`
- `uid`
- `type_id`
- `widget_id`
- `agent_unique_id`
- backend-specific subject ids for users and teams

The problem is not just naming. The CLI currently:

- prints numeric `ID` in places where the public contract has moved to `uid`
- accepts integer-only arguments for resource lookups that should be string
  identifiers
- builds local flows around `MAIN_SEQUENCE_PROJECT_ID`
- relies on folder names ending in `-<id>`
- teaches users to think in backend row ids even when the public API no longer
  does

This creates three kinds of defects:

1. wrong public output
2. wrong public input contract
3. local tooling and automation built on unstable backend ids

The CLI must stop inventing its own identity rules. It should follow the client
model contract for each object family.

## Decision

The CLI must use the canonical public identifier for every object family.

That means:

- if the client model contract says `uid`, the CLI uses `uid`
- if the client model contract says `type_id`, the CLI uses `type_id`
- if the client model contract says `widget_id`, the CLI uses `widget_id`
- if the client model contract says `agent_unique_id`, the CLI uses
  `agent_unique_id`
- if a surface still truly has only a public numeric id, that must be explicit
  and documented as a temporary exception or a backend contract that has not yet
  migrated

The CLI must not:

- default to numeric `id` just because it exists in a payload
- label a column `ID` when the public identifier is something else
- coerce public resource arguments through `int(...)` unless the client contract
  explicitly requires an integer id
- teach numeric row ids as the general CLI mental model

## Core principle

The CLI identity contract is derived from the client model contract, not from
backend convenience fields.

If a backend payload includes both:

- `uid`
- `id`

then:

- `uid` is the public CLI identity when the client contract says so
- `id` is migration metadata unless documented otherwise

## Scope

In scope:

- all CLI list tables
- all CLI detail views
- all CLI success and error messages that identify a resource
- all CLI resource arguments and options
- local CLI state that stores or resolves resource identity
- CLI docs, tutorials, and examples

Out of scope:

- backend-internal row ids that are not exposed as CLI resource references
- provider ids that are part of a third-party API contract
- permission subject ids until their subject contract is confirmed
- raw table row ids returned from SQL/query results

## Required final state

- every CLI surface prints the canonical public identifier for its object family
- every CLI argument accepts the canonical public identifier type
- no public CLI command assumes numeric identity unless the client contract
  explicitly requires it
- local CLI env variables use public resource identifiers
- local CLI folder mapping does not rely on numeric id suffixes for canonical
  identity
- CLI docs and tutorials teach the same public identifier used by the client

## Canonical CLI identity table

The CLI should align to this pattern:

- `Project`: `uid`
- `DataNodeStorage`: `uid`
- `DataNodeUpdate`: `uid`
- `Scheduler`: `uid`
- `Workspace`: `uid`
- `ConnectionType`: `type_id`
- `RegisteredWidgetType`: `widget_id`
- `ConnectionInstance`: public identity to be confirmed from client/backend
  contract
- `Agent`: public identity to be confirmed between `uid` and
  `agent_unique_id`; until confirmed, treat the current numeric `id` surface as
  not final
- `AgentSession`: `id` for now; current client contract is still numeric
- `User` / `Team` / permission subjects: do not migrate blindly; require an
  explicit subject-identity contract

This ADR does not claim that every object is already `uid`-based. It requires
the CLI to stop guessing and to align each command to the actual public contract
of its object family.

## Non-negotiable CLI rules

- no public CLI table should show `ID` when the canonical identifier is not
  `id`
- no public CLI detail output should bury the canonical identifier while showing
  numeric `id` as primary
- no resource argument should be typed as `int` when the canonical identifier is
  string-shaped
- no public success message should identify a migrated resource by numeric `id`
- no local CLI project setup flow should keep using `MAIN_SEQUENCE_PROJECT_ID`
  once `Project.uid` is available end to end
- no new CLI command should introduce fresh `*_id` resource arguments without an
  explicit contract review

## Current CLI problems by category

### 1. Wrong display identity

Observed examples:

- project list/search historically displayed `ID`
- several object-family tables still show `ID` without first checking whether
  the client contract has migrated
- success messages still often print `id=...` even where the public contract is
  moving away from numeric ids

### 2. Wrong input identity

Observed examples:

- many commands still type resource arguments as `int`
- many code paths use `int(...)` coercion on identifiers
- list/detail helper functions still compare against `payload["id"]` by default

### 3. Wrong local state contract

Observed examples:

- legacy compatibility still reads `MAIN_SEQUENCE_PROJECT_ID`
- older project folder lookup still falls back to `-<id>` suffix parsing
- shell snippets in docs scrape the first CLI column as a numeric id

## Migration policy

The CLI migration should proceed by object family, but every family follows the
same decision rule:

1. confirm the canonical public identifier from the client contract
2. align list output
3. align detail output
4. align command arguments
5. align local state and helper flows
6. align docs and tests

This avoids a blind global `ID -> UID` rename while still enforcing one coherent
CLI identity policy.

## Implementation stages

### Stage 1: inventory

- [ ] inventory every public CLI command by object family
- [ ] record the identifier it currently displays
- [ ] record the identifier type it currently accepts
- [ ] map each command to the client model that should define its public
      identity
- [ ] classify every numeric `id` usage as:
  - canonical public identifier
  - backend metadata
  - permission subject id
  - provider id
  - stale CLI debt

### Stage 2: display alignment

- [ ] update every list table to print the canonical identifier column
- [ ] update every detail view to print the canonical identifier first
- [ ] update success/error output to reference the canonical identifier
- [ ] remove generic `ID` column labels where they are no longer correct

### Stage 3: input contract alignment

- [ ] replace integer-only resource arguments with string identifiers where the
      client contract is no longer integer-based
- [ ] remove `int(...)` coercion from migrated resource commands
- [ ] update helper functions that compare on `payload["id"]`

### Stage 4: local state alignment

- [ ] replace `MAIN_SEQUENCE_PROJECT_ID` with `MAIN_SEQUENCE_PROJECT_UID`
- [ ] review any future local state keys for other object families
- [ ] stop relying on numeric suffix parsing for canonical identity

### Stage 5: documentation alignment

- [ ] update CLI docs
- [ ] update tutorials
- [ ] update shell snippets that scrape numeric ids from CLI output
- [ ] remove examples that teach row ids as the normal CLI resource reference

### Stage 6: cleanup

- [ ] remove temporary legacy compatibility where the public contract is already
      stable
- [ ] fail tests if migrated CLI surfaces regress to numeric `ID`

## Priority migration order

The CLI should not attempt all families at once. Prioritize by user-facing risk:

1. `Project`
2. `DataNodeStorage` / TDAG resources
3. Command Center workspaces and connections
4. agents and runtime resources
5. infrastructure/resource-release/job/image surfaces
6. user/team/permission-subject review after explicit contract confirmation

## Project-specific application of this ADR

Project is the most visible current defect and should be treated as the first
concrete implementation slice.

Required project changes under this ADR:

- `Project` CLI display uses `uid`
- project helper routes move to `{project_uid}` where the backend contract
  supports it
- local env uses `MAIN_SEQUENCE_PROJECT_UID`
- local folder lookup stops treating `-<id>` as canonical
- docs stop teaching project numeric ids as the public reference

## Immediate implementation slice

The next required slice under this ADR is:

1. migrate project command arguments from numeric `project_id` parsing to
   string `project_uid` parsing
2. replace local CLI write-paths from `MAIN_SEQUENCE_PROJECT_ID` to
   `MAIN_SEQUENCE_PROJECT_UID`
3. keep legacy numeric project id support only as an internal compatibility
   adapter when older client filters still require backend row ids
4. audit workspace and agent-session surfaces and migrate only after the
   backend/client contract is explicit

Audit result after the workspace contract update:

- `Workspace` is UID-based in the client contract and CLI resource arguments use
  `workspace_uid`.
- Workspace widget mutations still use `widget_instance_id` because that is the
  mounted widget id inside the workspace JSON, not a backend row id.
- `AgentSession` remains tracked in the agent-runtime section of the UID ADR.
- `Project` was the first family to complete the public UID migration in the CLI.

## Testing requirements

- add CLI tests per object family for canonical identifier rendering
- add CLI tests that pass UUID-like/string identifiers where appropriate
- add regression tests that fail if migrated surfaces print `ID` instead of the
  canonical field
- add local-state tests for project UID migration
- add doc/example checks where practical for common CLI snippets

## Consequences

Positive:

- CLI output becomes consistent with client/public contracts
- automation stops depending on unstable numeric ids
- tutorials and command examples become correct
- migration work becomes auditable by object family

Negative:

- some command signatures will become breaking changes
- temporary compatibility shims may be required while backend/client migrations
  are incomplete
- object families still lacking a confirmed public identity must be explicitly
  tracked rather than guessed

## Rejected alternatives

### 1. Keep using `id` unless an endpoint breaks

Rejected because it preserves silent drift and keeps teaching the wrong public
model.

### 2. Rename every CLI `ID` column to `UID`

Rejected because not every object family has `uid` as its canonical identifier.
Some use `type_id`, `widget_id`, or another contract-specific field.

### 3. Treat backend payload shape as the CLI contract

Rejected because backend payloads may include both public identity and internal
row metadata. The CLI must follow the client/public contract, not whichever
field happens to be present.
