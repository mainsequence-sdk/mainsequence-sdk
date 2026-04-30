---
name: mainsequence-virtualfundbuilder
description: Use this skill when the task is about portfolio construction with Virtual Fund Builder in a Main Sequence project. This skill owns VFB pipeline design, portfolio configuration, signal and rebalance roles, price-path expectations, forward-fill semantics, and portfolio data contracts. It does not own asset registration, generic DataNode producer design, or pricing-runtime internals.
---

# Main Sequence Virtual Fund Builder

## Overview

Use this skill when the task is about turning prices and signal weights into a portfolio the Main Sequence platform understands.

This skill is for:

- `PortfolioStrategy`
- `PortfolioFromDF`
- VFB configuration design
- signal vs rebalance role separation
- portfolio price-path setup
- forward-fill decisions
- portfolio data contracts
- built-in VFB strategy selection

## This Skill Can Do

- decide whether the workflow should use `PortfolioStrategy` or `PortfolioFromDF`
- design the top-level `PortfolioConfiguration`
- decide whether the traded universe should come from an asset category or from the signal node
- choose and review `PricesConfiguration`
- explain and enforce the split between:
  - signal strategy
  - rebalance strategy
  - execution configuration
- reason about `translation_table_unique_id` inside the portfolio price pipeline
- explain the three different fill systems:
  - bar interpolation
  - price forward-fill to now
  - signal validity forward-fill
- review or build VFB-compatible DataFrame contracts
- choose among common built-ins such as:
  - `FixedWeights`
  - `MarketCap`
  - `ExternalWeights`
  - `ETFReplicator`
  - `IntradayTrend`
  - `ImmediateSignal`

## This Skill Must Not Claim

This skill must not claim ownership of:

- public vs custom asset registration
- asset category creation semantics
- translation table design as a standalone markets problem
- generic DataNode producer implementation
- SimpleTable schema design
- pricing-runtime internals for instruments

## Route Adjacent Work

- assets, asset categories, and translation tables:
  `.agents/skills/mainsequence/markets_platform/assets_and_translation/SKILL.md`
- DataNodes and producer-side logic:
  `.agents/skills/mainsequence/data_publishing/data_nodes/SKILL.md`
- instruments and pricing runtime:
  `.agents/skills/mainsequence/markets_platform/instruments_and_pricing/SKILL.md`
- APIs and FastAPI:
  `.agents/skills/mainsequence/application_surfaces/api_surfaces/SKILL.md`

## Read First

1. `docs/tutorial/virtualfundbuilder/markets_portfolios_and_virtual_funds.md`
2. `docs/knowledge/virtualfundbuilder/index.md`
3. `docs/knowledge/virtualfundbuilder/portfolio_pipeline.md`
4. `docs/knowledge/virtualfundbuilder/data_contracts.md`
5. `docs/knowledge/virtualfundbuilder/prices_and_forward_fill.md`
6. `docs/knowledge/virtualfundbuilder/built_in_strategies.md`

## Inputs This Skill Needs

Before changing a VFB workflow, collect or infer:

- whether the portfolio should be:
  - computed by VFB
  - imported from an external DataFrame
- how the asset universe is defined:
  - `assets_category_unique_id`
  - signal-defined `get_asset_list()`
- how prices are sourced:
  - upstream bars source
  - translation table
  - external prices
- which signal strategy is in scope
- which rebalance strategy is in scope
- whether the portfolio is:
  - daily
  - intraday
- whether the task depends on fee modeling
- whether the output must sync into Markets metadata cleanly

If the price path or signal/rebalance responsibilities are unclear, stop before changing the portfolio configuration.

## Required Decisions

For every non-trivial VFB task, decide:

1. Is this a `PortfolioStrategy` workflow or a `PortfolioFromDF` workflow?
2. Is the universe category-driven or signal-driven?
3. How are prices resolved into the portfolio timeline?
4. Is a translation table required?
5. Which rebalance strategy should execute the signal?
6. What forward-fill behavior is valid for:
   - bars
   - prices
   - signal weights
7. Is a built-in strategy sufficient, or is a custom strategy actually needed?

## Build Rules

### 1. Keep the pipeline roles separate

Do not blur these roles:

- signal strategy decides desired weights
- rebalance strategy decides execution path
- prices determine valuation
- execution configuration determines fee drag

If these concerns are mixed together, the portfolio becomes hard to reason about.

### 2. Use `PortfolioStrategy` unless the portfolio already exists elsewhere

Use `PortfolioStrategy` when VFB should compute the portfolio.

Use `PortfolioFromDF` only when the portfolio time series already exists externally and needs to be normalized and stored.

Do not use `PortfolioFromDF` just to avoid understanding VFB contracts.

### 3. Universe definition must be intentional

Choose one of these on purpose:

- asset-category-driven universe
- signal-driven universe

Do not leave the universe ambiguous between both patterns.

### 4. Translation tables are part of price plumbing

If `PricesConfiguration.translation_table_unique_id` is used, the traded assets, translation rules, and upstream bars source must agree.

Do not treat translation tables as optional decoration when the price path depends on them.

### 5. Forward fill means three different things

Keep these separate:

- bar interpolation fills gaps in the bar series
- `forward_fill_to_now` extends valuation continuity
- signal validity forward-fill controls how long weights remain economically valid

Do not claim that `forward_fill_to_now=True` makes an old signal valid.

### 6. VFB contracts are exact

Match the expected DataFrame shape exactly.

Important examples:

- signal output:
  - MultiIndex `("time_index", "unique_identifier")`
  - column `signal_weight`
- final portfolio output:
  - exact required portfolio columns

Do not hand-wave schema differences inside VFB.

### 7. Built-ins are not interchangeable

Use built-ins according to the actual problem:

- `FixedWeights` for simple constant allocations
- `MarketCap` for category-driven market-cap weighting
- `ExternalWeights` or `ExternalPrices` for Artifact-backed external inputs
- `ImmediateSignal` as the normal rebalance starting point

Do not assume every built-in rebalance class is equally mature without checking the branch behavior.

## Review Rules

When reviewing a VFB task, look for:

- mixing signal logic with rebalance logic
- ambiguous universe definition
- translation table dependency that was not validated
- `forward_fill_to_now` being used to hide stale signals
- wrong long-form vs wide-form assumptions
- wrong or missing exact column names
- use of `PortfolioFromDF` when the portfolio should really be computed
- custom strategy complexity where a built-in would be clearer

## Validation Checklist

Do not claim success until you have checked:

- the correct VFB entrypoint is being used:
  - `PortfolioStrategy`
  - `PortfolioFromDF`
- the asset-universe pattern is intentional
- the price-path setup is intentional
- translation-table usage is intentional when prices depend on it
- signal, rebalance, and fee responsibilities are separated
- forward-fill behavior is correct for bars, prices, and signal validity
- DataFrame contracts match exact VFB expectations
- built-in strategy choice is intentional

## This Skill Must Stop And Escalate When

- the task does not yet have a stable asset universe
- the pricing source is unclear
- the translation-table contract is unclear but price routing depends on it
- the request mixes VFB concerns with raw DataNode producer implementation
- the task is really about instruments/pricing runtime rather than portfolio construction
- the branch behavior of a specialized built-in strategy is unclear but the workflow depends on it

Do not guess through portfolio semantics.
