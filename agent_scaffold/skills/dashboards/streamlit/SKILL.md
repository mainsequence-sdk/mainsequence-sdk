---
name: mainsequence-dashboards-streamlit
description: Use this skill when the task is about building a Streamlit dashboard in a Main Sequence project. This skill owns dashboard folder structure, page bootstrapping, use of the tested Main Sequence Streamlit helpers, sidebar and session patterns, and dashboard-specific validation rules. It does not own FastAPI APIs, Command Center workspace documents, or release orchestration semantics.
---

# Main Sequence Streamlit Dashboards

## Overview

Use this skill when the task is about building or reviewing a Streamlit dashboard in a Main Sequence project.

This skill is for:

- dashboard folder structure
- page bootstrapping with the Main Sequence scaffold
- use of `mainsequence.dashboards.streamlit`
- reusable platform-tested components
- instrument form rendering in Streamlit
- dashboard-side reads from platform data products

## This Skill Can Do

- define the expected folder structure for a Streamlit dashboard
- enforce that each dashboard root contains both:
  - `app.py`
  - `README.md`
- choose the recommended page bootstrap using:
  - `PageConfig`
  - `run_page(...)`
- review when to use the packaged Streamlit helpers instead of custom UI code
- apply the tested components for:
  - asset selection
  - valuation date settings
  - logged-in user display
- use the instrument form factory when the dashboard edits rich Pydantic models
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
4. `docs/knowledge/dashboards/streamlit/scaffold_and_theming.md`
5. `docs/knowledge/dashboards/streamlit/components.md`
6. `docs/knowledge/dashboards/streamlit/instrument_forms.md`

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
  - the packaged scaffold only
  - additional helper components
- whether the dashboard works with:
  - assets
  - valuation dates
  - instrument configuration forms
- which platform data products the dashboard reads

If the dashboard root or entrypoint is unclear, stop before changing packaging-sensitive behavior.

## Required Decisions

For every non-trivial Streamlit dashboard task, decide:

1. What is the dashboard root folder?
2. Does the root contain both `app.py` and `README.md`?
3. Should the page use the standard scaffold?
4. Which packaged helper components should be reused instead of custom UI?
5. Does the dashboard need model-driven instrument forms?
6. Is the task really dashboard implementation, or is it actually an API, AppComponent, or release problem?

## Build Rules

### 1. Every dashboard must have `README.md` next to `app.py`

This is mandatory.

Each dashboard root must contain:

- `app.py`
- `README.md`

Do not build or document a dashboard folder that omits `README.md`.

### 2. Use the Main Sequence Streamlit scaffold by default

The normal page bootstrap should use:

- `from mainsequence.dashboards.streamlit.scaffold import PageConfig, run_page`

Then call `run_page(...)` at the top of each page.

Do not hand-roll page bootstrapping when the packaged scaffold already covers the normal case.

### 3. Prefer the tested helper package over custom rewrites

Use `mainsequence.dashboards.streamlit` as the default starting point for Streamlit work on Main Sequence.

These helpers already handle platform-tested behavior for:

- theme setup
- logo and favicon handling
- sidebar patterns
- user context display
- instrument model forms

Do not rebuild these pieces from scratch unless the dashboard has a real requirement the helper package cannot satisfy.

### 4. Keep dashboard code focused on dashboard concerns

The dashboard layer should read and present data cleanly.

Do not push producer semantics, release orchestration, or unrelated API logic into Streamlit pages just because it is convenient.

### 5. Use the right helper for the right UI problem

Examples:

- use packaged components for asset selection and logged-user display
- use the instrument form factory for rich model-driven forms
- use `APIDataNode.build_from_identifier(...)` when a dashboard knows which published table it needs to read

Do not replace stable platform-aware helpers with ad hoc widgets without a concrete reason.

### 6. Multipage apps should stay structurally clear

For multipage apps:

- keep `app.py` as the landing page or root entrypoint
- keep secondary pages under `pages/`
- keep shared helpers in small modules such as `common.py` when needed

Do not let page bootstrapping diverge from page to page without reason.

## Review Rules

When reviewing a Streamlit dashboard task, look for:

- missing `README.md` next to `app.py`
- dashboard pages that do not use the standard scaffold
- custom sidebar or auth UI that should reuse the packaged helpers
- hand-built model forms that should use the instrument form factory
- dashboard code taking on API or release responsibilities
- inconsistent page structure across a multipage app
- dashboard logic that reads platform objects in an unclear or fragile way

## Validation Checklist

Do not claim success until you have checked:

- the dashboard root is correct
- `README.md` exists next to `app.py`
- pages use the standard scaffold unless there is a verified reason not to
- helper-package reuse is intentional
- instrument forms use the model-driven helpers when rich finance models are involved
- the dashboard reads the intended published data products
- the task did not confuse dashboard implementation with release orchestration or API design

## This Skill Must Stop And Escalate When

- the task is really about release creation rather than dashboard implementation
- the task is really about FastAPI or widget contracts rather than Streamlit
- the dashboard root and entrypoint are not clear
- the requested custom UI would replace tested helpers without a verified requirement
- the dashboard depends on platform data products whose read path is still unclear

Do not guess through packaging-sensitive dashboard structure.
