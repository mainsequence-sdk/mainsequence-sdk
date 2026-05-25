---
name: mainsequence-dashboards-streamlit
description: Use this skill when the task is about building a Streamlit dashboard in a Main Sequence project. This skill owns dashboard folder structure, app-owned page bootstrapping, sidebar and session patterns, and dashboard-specific validation rules. It does not own FastAPI APIs, Command Center workspace documents, or release orchestration semantics.
---

# Main Sequence Streamlit Dashboards

## Overview

Use this skill when the task is about building or reviewing a Streamlit dashboard in a Main Sequence project.

This skill is for:

- dashboard folder structure
- app-owned Streamlit page bootstrapping
- app-owned Streamlit helpers and components
- dashboard-side reads from platform data products

## This Skill Can Do

- define the expected folder structure for a Streamlit dashboard
- enforce that each dashboard root contains both:
  - `app.py`
  - `README.md`
- choose a plain Streamlit page bootstrap using `st.set_page_config(...)`
- review dashboard-owned helpers for entity selection, effective date settings, and user display
- use model-driven form helpers when the dashboard edits rich Pydantic models
- review dashboard reads from `APIDataNode` and other platform objects from the dashboard side
- keep dashboard implementation aligned with Main Sequence packaging and release expectations

## This Skill Must Not Claim

This skill must not claim ownership of:

- FastAPI API design
- Command Center workspace document design
- AppComponent contracts
- project-resource or release orchestration semantics
- DataNode producer implementation
- SimpleTable schema design

## Route Adjacent Work

- FastAPI and application APIs:
  `.agents/skills/mainsequence/application_surfaces/api_surfaces/SKILL.md`
- Command Center workspaces:
  `.agents/skills/mainsequence/command_center/workspace_builder/SKILL.md`
- AppComponents and custom forms for widgets:
  `.agents/skills/mainsequence/command_center/app_components/SKILL.md`
- jobs, images, project resources, and releases:
  `.agents/skills/mainsequence/platform_operations/orchestration_and_releases/SKILL.md`
- DataNodes:
  `.agents/skills/mainsequence/data_publishing/data_nodes/SKILL.md`
- SimpleTables:
  `.agents/skills/mainsequence/data_publishing/simple_tables/SKILL.md`

## Read First

1. `docs/tutorial/dashboards/streamlit/streamlit_integration_1.md`
2. `docs/tutorial/dashboards/streamlit/streamlit_integration_2.md`
3. `docs/knowledge/dashboards/streamlit/index.md`

If the dashboard is being deployed or released, also read:

7. `.agents/skills/mainsequence/platform_operations/orchestration_and_releases/SKILL.md`

## Inputs This Skill Needs

Before changing a Streamlit dashboard, collect or infer:

- the dashboard root folder
- the location of:
  - `app.py`
  - `README.md`
- whether the dashboard is:
  - single-page
  - multipage
- whether the page should use:
  - plain Streamlit only
  - app-owned helper components
- whether the dashboard works with:
  - data assets
  - valuation dates
  - model configuration forms
- which platform data products the dashboard reads

If the dashboard root or entrypoint is unclear, stop before changing packaging-sensitive behavior.

## Required Decisions

For every non-trivial Streamlit dashboard task, decide:

1. What is the dashboard root folder?
2. Does the root contain both `app.py` and `README.md`?
3. What page setup and app-owned helper structure should the dashboard use?
4. Which app-owned helper components should be reused instead of duplicating UI code?
5. Does the dashboard need model-driven forms?
6. Is the task really dashboard implementation, or is it actually an API, AppComponent, or release problem?

## Build Rules

### 1. Every dashboard must have `README.md` next to `app.py`

This is mandatory.

Each dashboard root must contain:

- `app.py`
- `README.md`

Do not build or document a dashboard folder that omits `README.md`.

### 2. Use app-owned Streamlit page setup

The normal page bootstrap should use direct Streamlit calls inside the app:

- `import streamlit as st`
- `st.set_page_config(...)`

Each page should own its title, layout, sidebar, and app-specific presentation behavior.

Do not import `mainsequence.dashboards.streamlit`; the SDK no longer ships dashboard UI scaffolding.

### 3. Keep Streamlit UI in the dashboard project

Streamlit UI code belongs to the dashboard project, not the SDK.

Keep helpers close to the dashboard for:

- theme setup and page branding
- sidebar patterns
- user context display
- asset or valuation-date controls

Use the SDK for platform data/client calls and release workflows.

### 4. Keep dashboard code focused on dashboard concerns

The dashboard layer should read and present data cleanly.

Do not push producer semantics, release orchestration, or unrelated API logic into Streamlit pages just because it is convenient.

### 5. Use the right helper for the right UI problem

Examples:

- keep logged-user display as app-owned Streamlit helpers
- use `APIDataNode.build_from_identifier(...)` when a dashboard knows which published table it needs to read

Do not push general-purpose dashboard UI helpers into the SDK.

### 6. Multipage apps should stay structurally clear

For multipage apps:

- keep `app.py` as the landing page or root entrypoint
- keep secondary pages under `pages/`
- keep shared helpers in small modules such as `common.py` when needed

Do not let page bootstrapping diverge from page to page without reason.

## Review Rules

When reviewing a Streamlit dashboard task, look for:

- missing `README.md` next to `app.py`
- stale imports from `mainsequence.dashboards.streamlit`
- missing Streamlit dependencies in the dashboard project when the app imports Streamlit directly
- hand-built model forms that should use model-driven helpers
- dashboard code taking on API or release responsibilities
- inconsistent page structure across a multipage app
- dashboard logic that reads platform objects in an unclear or fragile way

## Validation Checklist

Do not claim success until you have checked:

- the dashboard root is correct
- `README.md` exists next to `app.py`
- pages use clear app-owned Streamlit setup
- shared UI helper reuse is intentional and local to the dashboard project
- model forms use the model-driven helpers when rich domain models are involved
- the dashboard reads the intended published data products
- the task did not confuse dashboard implementation with release orchestration or API design

## This Skill Must Stop And Escalate When

- the task is really about release creation rather than dashboard implementation
- the task is really about FastAPI or widget contracts rather than Streamlit
- the dashboard root and entrypoint are not clear
- the dashboard depends on platform data products whose read path is still unclear

Do not guess through packaging-sensitive dashboard structure.
