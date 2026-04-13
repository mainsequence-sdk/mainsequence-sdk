---
name: mainsequence-assets-and-translation
description: Use this skill when the task is about market asset identity in a Main Sequence project. This skill owns public vs custom asset registration, asset lookup strategy, asset categories, translation tables, and the decision rules that connect assets to upstream market data. It does not own DataNode implementation, portfolio construction, or pricing-runtime internals.
---

# Main Sequence Assets And Translation

## Overview

Use this skill when the task is about how a Main Sequence project represents assets and how those assets map into upstream market data.

This skill is for:

- public asset registration
- custom asset registration
- `unique_identifier` rules for assets
- `Asset.filter(...)` vs `Asset.query(...)`
- asset categories
- translation tables
- deciding how a market universe should be represented

## This Skill Can Do

- choose whether an asset should come from the public master or be created as a custom asset
- explain that public assets usually use FIGI as `unique_identifier`
- explain that custom asset identity is owned by the organization and must remain stable
- choose between `msc.Asset.filter(...)` and `msc.Asset.query(...)`
- review or implement the standard registration pattern:
  - look up existing assets
  - register only missing ones
- decide when an asset universe should become an `AssetCategory`
- decide when routing should be represented as an `AssetTranslationTable`
- explain the invariant that exactly one translation rule must match each asset in scope
- review whether a translation table is actually being used as a dependency manifest

## This Skill Must Not Claim

This skill must not claim ownership of:

- DataNode producer implementation
- update-window logic
- `get_asset_list()` implementation details inside a DataNode
- portfolio strategy construction
- Virtual Fund Builder configuration details
- pricing-runtime internals

## Route Adjacent Work

- DataNodes and asset-indexed producer logic:
  `.agents/skills/data_publishing/data_nodes/SKILL.md`
- Virtual Fund Builder and portfolio construction:
  `.agents/skills/markets_platform/virtualfundbuilder/SKILL.md`
- instruments and pricing runtime:
  `.agents/skills/markets_platform/instruments_and_pricing/SKILL.md`
- APIs and FastAPI:
  `.agents/skills/application_surfaces/api_surfaces/SKILL.md`

## Read First

1. `docs/tutorial/markets_tutorial/markets_equities_with_algoseek.md`
2. `docs/knowledge/markets/assets.md`
3. `docs/knowledge/markets/asset_categories.md`
4. `docs/knowledge/markets/translation_tables.md`

If the task also changes producer code, read:

5. `.agents/skills/data_publishing/data_nodes/SKILL.md`

## Inputs This Skill Needs

Before changing asset or translation design, collect or infer:

- whether the assets are:
  - publicly mastered instruments
  - organization-owned custom instruments
- the stable identity the workflow should use
- whether the universe should be:
  - embedded directly in code
  - represented as an `AssetCategory`
- whether routing is:
  - trivial and single-source
  - multi-source and explicit
- whether lookup payloads are small or large
- whether downstream consumers are:
  - DataNodes
  - VFB
  - dashboards
  - APIs

If the asset identity model is unclear, stop before registering assets or building translation tables.

## Required Decisions

For every non-trivial markets-asset task, decide:

1. Is this a public asset or a custom asset?
2. What is the stable `unique_identifier`?
3. Should lookup use `filter()` or `query()`?
4. Is the asset universe local code, or should it become an `AssetCategory`?
5. Does routing require an `AssetTranslationTable`?
6. If a translation table is used, how will you guarantee exactly one matching rule per asset in scope?

## Build Rules

### 1. Asset identity is a real contract

Treat `unique_identifier` as a stable platform identity, not as a casual label.

For public assets:

- use the public market identity the platform expects
- in the documented equity flow, that is FIGI

For custom assets:

- the organization owns the identity
- keep it stable, meaningful, and reusable

Do not create disposable or ambiguous asset identifiers.

### 2. Register only what is missing

Use the normal idempotent pattern:

- look up existing assets first
- register only the missing ones

Do not blindly re-register every asset on every run.

### 3. Use `filter()` for normal lookups and `query()` for large payloads

Use `msc.Asset.filter(...)` when:

- the filter is small
- the lookup is simple
- the URL size is not a concern

Use `msc.Asset.query(...)` when:

- the payload is large
- `__in` filters are large
- you want explicit `per_page` control

Do not use `filter()` for large universes just because it is shorter.

### 4. Asset categories define universes, not routing

Use `AssetCategory` when the universe itself should be a named reusable platform object.

Use it for:

- reusable strategy universes
- benchmark universes
- shared portfolio universes

Do not use an asset category to solve price routing.

### 5. Translation tables define routing, not membership

Use `AssetTranslationTable` when the question is:

> for this asset, which upstream time series should be used?

Do not use a translation table to express the asset universe itself.

### 6. Translation tables must be deterministic

For assets in scope:

- zero matching rules is invalid
- more than one matching rule is invalid
- exactly one matching rule is required

Do not leave translation routing ambiguous.

### 7. Translation tables are also dependency manifests

If a workflow constructs wrapper reads from a translation table, remember that every referenced upstream time series matters, even if a specific run would not touch every rule.

Do not leave dead or invalid upstream identifiers in a shared translation table.

## Review Rules

When reviewing an assets or translation task, look for:

- unstable or weak `unique_identifier` choices
- custom assets being created when public asset registration should be used
- public assets being forced into a custom identity scheme without reason
- large lookups using `filter()` where `query()` should be used
- repeated hard-coded asset lists that should be an `AssetCategory`
- translation tables being used where a simple single-source dependency would be enough
- translation rules that can overlap
- translation rules that do not cover the real asset universe
- categories being confused with routing

## Validation Checklist

Do not claim success until you have checked:

- the asset identity model is intentional
- public vs custom asset choice is intentional
- the registration flow is idempotent
- the lookup method is intentional:
  - `filter()`
  - `query()`
- the universe representation is intentional:
  - direct asset list
  - `AssetCategory`
- translation routing is intentional:
  - no table needed
  - `AssetTranslationTable`
- every asset in scope has exactly one valid translation rule when a translation table is used

## This Skill Must Stop And Escalate When

- the workflow does not have a stable asset identity yet
- the task mixes asset registration with DataNode producer logic and the producer behavior is unclear
- translation routing is ambiguous and the intended source of truth is not explicit
- a shared universe is changing but the business meaning of that change is unknown
- the task is actually a VFB pricing or portfolio problem rather than an asset-identity problem
- the task is actually an instruments/pricing-runtime problem rather than an asset-mapping problem

Do not guess through market identity boundaries.
