---
name: mainsequence-instruments-and-pricing
description: Use this skill when the task is about instrument valuation in a Main Sequence project. This skill owns market-data registration for pricing inputs, pricing details on assets, the runtime path from stored curves and fixings into QuantLib objects, and valuation expectations for bonds, swaps, and positions. It does not own generic asset identity, portfolio construction, or DataNode producer design.
---

# Main Sequence Instruments And Pricing

## Overview

Use this skill when the task is about turning stored market data and stored instrument terms into runtime valuation.

This skill is for:

- curve and fixing storage expectations for pricing
- ETL registration vs pricing registration
- `IndexSpec`
- pricing details on assets
- rebuilding instruments from stored pricing details
- runtime pricing of bonds, swaps, and positions
- valuation-date and cache expectations

## This Skill Can Do

- explain the two handshakes that make pricing work:
  - market data to pricing indices
  - assets to instrument terms
- decide whether the missing piece is:
  - storage configuration
  - ETL registration
  - pricing registration
  - pricing details
- review or define the storage contracts for:
  - discount curves
  - fixing rates
- explain the split between constant names and resolved UID values
- review `IndexSpec` registration responsibilities
- explain when an asset is priceable vs merely identifiable
- review or build workflows that attach pricing details to assets
- explain the runtime path for pricing:
  - load curve and fixings
  - materialize runtime objects
  - price instrument or position
- review common failure modes for bonds, floaters, swaps, and positions

## This Skill Must Not Claim

This skill must not claim ownership of:

- public vs custom asset identity as a standalone markets problem
- asset categories
- translation table design
- Virtual Fund Builder portfolio construction
- generic DataNode producer design outside pricing-input storage contracts
- FastAPI or dashboard implementation

## Route Adjacent Work

- assets, categories, and translation tables:
  `.agents/skills/markets_platform/assets_and_translation/SKILL.md`
- Virtual Fund Builder and portfolio construction:
  `.agents/skills/markets_platform/virtualfundbuilder/SKILL.md`
- DataNodes and producer-side implementation:
  `.agents/skills/data_publishing/data_nodes/SKILL.md`
- APIs and FastAPI:
  `.agents/skills/application_surfaces/api_surfaces/SKILL.md`

## Read First

1. `docs/knowledge/instruments/index.md`
2. `docs/knowledge/instruments/market_data_and_registration.md`
3. `docs/knowledge/instruments/assets_and_pricing_details.md`
4. `docs/knowledge/instruments/pricing_runtime.md`
5. `docs/knowledge/instruments/examples.md`
6. `docs/tutorial/markets_tutorial/markets_fixed_income_custom_assets.md`

## Inputs This Skill Needs

Before changing an instruments or pricing workflow, collect or infer:

- whether the task is about:
  - market-data storage
  - pricing registration
  - pricing details on assets
  - runtime valuation
- the relevant index UID or curve UID
- whether curves and fixings are already stored
- whether `InstrumentsConfiguration` points to the correct storage node ids
- whether pricing details already exist on the asset
- the instrument type:
  - fixed-rate bond
  - floating-rate bond
  - vanilla swap
  - position
- the valuation date and expected data snapshot

If the data-registration path or pricing-details path is unclear, stop before claiming the instrument can be priced.

## Required Decisions

For every non-trivial instruments task, decide:

1. Is the problem in ETL registration, pricing registration, pricing details, or runtime usage?
2. Which UID is authoritative at runtime?
3. Are curves and fixings already stored in the expected shape?
4. Does the asset need pricing details attached or refreshed?
5. Is the task about a single instrument or a portfolio position wrapper?
6. Is the valuation date explicit and intentional?

## Build Rules

### 1. Keep ETL registration and pricing registration separate

Do not mix these two layers:

- ETL registration tells the system how to build and store pricing inputs
- pricing registration tells the runtime how to interpret an index UID

If one exists without the other, pricing can still fail.

### 2. Respect the storage contracts

The pricing runtime expects stable storage shapes.

Important contracts:

- discount curves:
  - identifier `discount_curves`
  - index `(time_index, unique_identifier)`
  - column `curve`
- fixing rates:
  - identifier `fixing_rates_1d`
  - index `(time_index, unique_identifier)`
  - column `rate`

Do not improvise storage shapes if pricing is expected to consume them.

### 3. Constant names and resolved UID values are not interchangeable

Use the right identifier at the right layer:

- ETL registries are keyed by constant name
- pricing registration is keyed by resolved UID value

Do not mix symbolic names and resolved runtime identifiers.

### 4. Prices do not make an asset priceable

An asset with price history is not automatically a priceable instrument.

If the SDK must rebuild the instrument later, attach pricing details.

Do not claim an asset is valuation-ready just because prices exist.

### 5. Pricing details must be intentional and reproducible

Use stored pricing details to preserve the actual instrument terms:

- dates
- coupon or spread
- conventions
- index references

Do not rebuild important instrument terms from guesswork when a stored pricing-detail path exists.

### 6. Runtime pricing is mostly a read path

At runtime, pricing should:

- resolve the index UID
- load the corresponding curve and fixings
- materialize runtime pricing objects
- price the instrument or position

Do not treat runtime pricing as the place where registration or storage should be fixed implicitly.

### 7. Valuation date must be explicit

Set valuation date on purpose.

Be aware that caches exist for stored observations and some runtime calculations.

Do not rely on incidental defaults or repeatedly mutate one reused object across unrelated valuation dates unless you have verified the behavior.

## Review Rules

When reviewing an instruments or pricing task, look for:

- ETL registration without pricing registration
- pricing registration without stored data
- wrong storage node ids in `InstrumentsConfiguration`
- percent vs decimal confusion in rates
- assets that should have pricing details but do not
- stale or incomplete pricing details being treated as authoritative
- missing fixings on floating-rate instruments
- unclear valuation date assumptions
- runtime failures that are really registration failures

## Validation Checklist

Do not claim success until you have checked:

- the task boundary is correct:
  - registration
  - pricing details
  - runtime pricing
- curves and fixings exist in the expected storage shapes
- `InstrumentsConfiguration` points to the intended storage nodes
- the right runtime UID is being used
- `IndexSpec` registration is present when required
- the asset has pricing details when valuation depends on stored instrument terms
- the valuation date is explicit
- at least one simple runtime path was validated when the task claims pricing works

## This Skill Must Stop And Escalate When

- the connector has not defined the registration boundary clearly
- stored data exists but the authoritative runtime UID is unclear
- the asset terms are incomplete but the task expects full valuation
- the workflow mixes pricing-runtime issues with VFB portfolio construction without separating them
- the problem is actually about asset identity or translation tables rather than valuation wiring
- the requested valuation semantics depend on undocumented fallback behavior

Do not guess through pricing infrastructure.
