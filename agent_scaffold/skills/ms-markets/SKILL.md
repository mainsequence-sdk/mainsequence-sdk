---
name: mainsequence-ms-markets
description: Use this skill when a Main Sequence project needs financial markets functionality. This skill points agents to the supported ms-markets library and its packaged skills instead of duplicating market-domain instructions in the SDK scaffold.
---

# Main Sequence ms-markets

`ms-markets` is the Main Sequence supported library for financial markets workflows.
Use it for market assets, market MetaTables, asset-indexed DataNodes, portfolios,
orders, trades, and related market-domain APIs.

This SDK scaffold skill is intentionally tiny. It does not own market-domain
implementation rules. Install `ms-markets` and copy its packaged skills into the
host project.

## Install

For a `uv` project:

```bash
uv add ms-markets
```

For a plain Python environment:

```bash
pip install ms-markets
```

## Copy ms-markets Skills

After `ms-markets` is installed in the active environment, run:

```bash
msm copy-msm-skills --path .
```

Then use the copied skills under:

```text
.agents/skills/ms_markets/
```

If the `msm` command is missing, the active environment does not have
`ms-markets` installed or activated.

## Boundary

Keep generic Main Sequence platform work routed through the normal
`.agents/skills/mainsequence/` skills. Use the copied `ms_markets` skills for
financial market-specific model, DataNode, API, and example work.
