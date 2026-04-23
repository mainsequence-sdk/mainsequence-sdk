---
name: command-center-workspace-analysis
description: Use this skill when the task is about interpreting a Main Sequence Command Center live workspace snapshot archive after capture. This skill explains the difference between the persisted workspace definition, the live runtime state, and per-widget evidence; teaches how to inspect manifest warnings, workspace-live-state, widget artifact folders, controls, dependency graphs, and screenshots; and guides agents to answer questions about what the workspace was actually doing at capture time rather than only what the shared workspace JSON intended.
---

# Workspace Snapshot Skill

Use this skill when you need to understand the actual captured state of a Command Center workspace after a live snapshot has already been produced.

This skill is not for building a workspace definition from scratch. Use the workspace builder skill for authoring or mutating the shared workspace JSON. Use this skill when the question is about what the mounted workspace was actually doing at capture time.

## Capturing A Full Runtime Snapshot

If the task needs a full snapshot of the current live Command Center runtime, use:

- `mainsequence/client/command_center/workspace_snapshot.py`
- specifically:
  - `get_workspace_snapshot(...)`

This helper drives the real browser client and captures the browser-built workspace ZIP archive. It is the correct path when you need the full runtime snapshot instead of only the persisted workspace JSON.

Before using it, make the environment prerequisites explicit:

- Playwright must be installed
- Chromium browser binaries must be installed

The helper itself expects the SDK snapshot extra and browser binaries to be present. If they are missing, stop and surface that prerequisite instead of pretending the snapshot can be captured.

## What This Snapshot Represents

A live workspace snapshot is a browser-built ZIP archive produced from the mounted Command Center runtime.

It contains three different kinds of truth:

1. Persisted workspace definition
- The sanitized shared workspace JSON.
- This answers what the workspace is structurally configured to contain.

2. Live workspace runtime state
- The mounted dashboard state at capture time.
- This answers what was actually mounted, visible, hidden, connected, refreshed, and resolved when the snapshot was taken.

3. Per-widget evidence
- Structured widget state embedded in `workspace-live-state.json` plus optional widget-specific exports.
- This answers what each widget was actually showing or producing at capture time.

The key distinction is:

- `workspace-definition.json` tells you the intended shared workspace structure.
- `workspace-live-state.json` tells you the actual captured runtime state.
- `workspace-live-state.json` widget records and widget artifact files tell you the widget-level evidence.

Do not answer runtime questions from `workspace-definition.json` alone if the live snapshot includes richer runtime evidence.

## Where This Fits Relative to Other Command Center Skills

Use this skill alongside the other Command Center skills in `agent_scaffold/skills/command_center`:

- `workspace_builder`
  - for creating or patching workspace/widget definitions
  - not for interpreting live capture evidence
- `app_components`
  - for AppComponent-specific configuration and execution reasoning
  - use it when the snapshot points to an AppComponent behavior question
- `api_mock_prototyping`
  - only if snapshot evidence points to API payload design or mock-response work

This skill should be the first stop when the user asks questions like:

- "What state was the workspace in?"
- "Which widgets were actually mounted?"
- "Why does the snapshot show this result?"
- "Was this widget hidden, missing, or permission-denied?"
- "What data did the widget actually have at capture time?"
- "What did the dashboard look like when the snapshot was taken?"

## Archive Overview

The live snapshot is a ZIP archive. It is not the normal workspace export/import JSON.

The archive usually contains:

- `manifest.json`
- `workspace-definition.json`
- `workspace-live-state.json`
- `controls.json`
- `relationships/widget-graph.json`
- `relationships/widget-graph.png`
- `screenshots/viewport.png`
- `screenshots/full-canvas.png`
- `screenshots/hidden-widgets-sheet.png`
- optional widget folders such as `widgets/<widget-id>-<uuid>/`
- optional widget files inside those folders, such as:
  - `screenshot.png`
  - `data.json`
  - `data.csv`
  - `chart-data.json`
  - `response.json`

Not every optional artifact will exist in every snapshot.

## What Each Top-Level File Means

### `manifest.json`

This is the first file to inspect.

It tells you:

- archive schema and version
- capture profile
- workspace id and title
- generated time
- file inventory
- warnings
- errors

Use it to decide whether the snapshot is trustworthy enough for a strong conclusion.

If `manifest.json` contains warnings or errors, treat missing screenshots or artifacts as a capture limitation, not automatically as a widget failure.

### `workspace-definition.json`

This is the normal sanitized workspace export embedded into the archive.

Use it to answer:

- what widgets are part of the shared workspace definition
- layout and structure questions
- bindings and configuration questions
- what the workspace was intended to contain

Do not use it by itself to answer:

- what was visible
- what had loaded
- what data was present
- what was hidden
- what the user actually saw at capture time

### `workspace-live-state.json`

This is the most important file for runtime reasoning.

It currently records:

- capture schema and version
- profile
- workspace id and title
- view mode
- live controls state
- refresh progress and refresh timestamps
- resolved widget dependency graph
- one record per mounted widget instance
- widget summary counts

This is the best file for answering:

- what was actually mounted
- which widgets were visible vs hidden
- which widgets were sidebar vs canvas
- why a widget was hidden
- which artifacts belong to each widget
- what the runtime dependency graph looked like

Each widget record in `workspace-live-state.json` contains:

- `instanceId`
- `widgetId`
- `title`
- `category`
- `kind`
- `source`
- `placementMode`
- `hidden`
- `hiddenReason`
- optional `layout`
- optional `parentRowId`
- optional `domTextContent`
- optional `screenshotPath`
- `artifactPaths`
- structured `snapshot`

This means `workspace-live-state.json` is the bridge between the shared workspace document and the widget-specific evidence files.

### `controls.json`

Use this when the question is about dashboard control state, date range, refresh behavior, or current control selections at capture time.

### `relationships/widget-graph.json`

This is the structural dependency graph between widgets.

Use it to answer:

- which widgets depend on which upstream widgets
- how a widget is fed
- what the dependency chain looked like at capture time

### `relationships/widget-graph.png`

This is a convenience visualization of the same dependency information. It is useful for quick orientation, but the JSON graph is the better source for precise reasoning.

### `screenshots/viewport.png`

This is the visible viewport capture at the moment of snapshot.

Use it to answer:

- what was on screen
- what the user would have seen without scrolling

### `screenshots/full-canvas.png`

This is the best-effort capture of the actual workspace canvas.

Use it to answer:

- what the full dashboard canvas looked like
- where widgets were placed visually
- what the visual composition of the workspace was

This is more important than the graph screenshots for real workspace-state questions.

### `screenshots/hidden-widgets-sheet.png`

This is a report-style screenshot for hidden/sidebar/collapsed content that may not appear on the visible canvas.

Use it as supporting evidence, not as the primary runtime picture of the workspace.

## Widget Artifact Folders

Each widget gets a folder like:

- `widgets/<widget-id>-<uuid>/`

The folder name is human-friendlier than the raw instance id, but the canonical widget identity is still the `instanceId` inside `workspace-live-state.json`.

Inside a widget folder you may see:

- `screenshot.png`
  - widget-local visual evidence, if a visible DOM capture was possible
- `data.json`
  - tabular or record-shaped data export
- `data.csv`
  - CSV export for row-based widget data
- `chart-data.json`
  - series-oriented chart export
- `response.json`
  - structured response payload for response-style widgets

Do not expect `snapshot.json` in widget folders. Structured widget evidence is embedded in the matching widget record in `workspace-live-state.json`, under `snapshot` when available. Use that widget record's `artifactPaths` plus `manifest.json` to locate any exported files.

The exact files depend on what the widget exposed and on the selected snapshot profile.

## Capture Profiles

The snapshot currently supports:

- `full-data`
  - richer data payloads when widgets support them
- `evidence`
  - same structure, but widgets may truncate data-heavy payloads

Always check the profile before making a strong statement like:

- "the widget had no data"
- "the full response was captured"

Under `evidence`, missing deep data may be intentional.

## How To Use The Snapshot To Answer Questions

Follow this order.

### 1. Start with `manifest.json`

Check:

- warnings
- errors
- profile
- which files actually exist

If a screenshot or widget export is missing, verify whether the manifest shows a capture warning before claiming the widget failed.

### 2. Read `workspace-live-state.json`

Use it to establish:

- which widgets existed at runtime
- which widget instance you care about
- visibility and placement
- hidden reasons
- artifact paths
- structured snapshot summaries

This should be your default runtime truth source.

### 3. Open the relevant widget folder

Use the widget record from `workspace-live-state.json` to locate:

- any widget-local screenshot
- any data/response/chart artifact

Do not search the archive blindly by title first. Resolve the widget through the live-state record.

### 4. Compare against `workspace-definition.json` only when needed

Use this comparison when the question is:

- "Was the widget configured this way?"
- "Is this runtime behavior inconsistent with the saved workspace definition?"
- "Did the snapshot reflect the persisted workspace structure?"

### 5. Use `relationships/widget-graph.json` for dependency questions

If the problem is about missing inputs, stale consumers, upstream chains, or graph reasoning, use the relationship graph instead of guessing from layout.

## What This Snapshot Is Good For

This snapshot is strong evidence for:

- mounted workspace state
- widget visibility and placement
- dependency structure
- captured control state
- widget summaries and structured widget evidence
- widget-local exported data when present

This snapshot is weaker for:

- exact browser-perfect visuals when screenshot warnings exist
- hidden widgets that only expose partial evidence
- widgets whose heavy data was trimmed under `evidence`

## Trust Boundaries And Limitations

Keep these rules explicit:

- screenshot capture is best-effort and browser-dependent
- hidden/sidebar widgets may still have valid structured evidence even without a visible screenshot
- `workspace-definition.json` is structural truth, not live runtime truth
- a widget title is human-readable but not the canonical key
- the canonical widget identity is `instanceId`
- missing optional artifacts do not automatically mean missing runtime state

If the archive contains warnings, reflect that uncertainty in the answer.

## Recommended Reasoning Patterns

Use these patterns when answering snapshot questions.

### Runtime state question

Example:
- "Was the widget visible?"

Use:
- `workspace-live-state.json`

Then support with:
- `screenshots/viewport.png`
- widget `screenshot.png` if present

### Data/result question

Example:
- "What output did the chart actually have?"

Use:
- the widget record's `snapshot` field in `workspace-live-state.json`, if present
- `chart-data.json` or `data.json`

Then compare with:
- `workspace-definition.json` only if configuration context matters

### Dependency question

Example:
- "Why did widget B depend on widget A?"

Use:
- `relationships/widget-graph.json`
- `workspace-live-state.json` widget records

### Workspace composition question

Example:
- "What did the dashboard look like overall?"

Use:
- `screenshots/full-canvas.png`
- `screenshots/viewport.png`
- `workspace-live-state.json` layouts

## Validation Checklist

Before answering from a snapshot, verify:

- the snapshot profile
- whether `manifest.json` contains warnings or errors
- whether the answer is grounded in:
  - persisted definition
  - live runtime state
  - widget-specific evidence
- the correct widget `instanceId`
- whether missing artifacts are true absence or capture limitations

## Short Rule

Use `workspace-definition.json` for what the workspace is.

Use `workspace-live-state.json` for what the workspace was doing.

Use `workspace-live-state.json` widget records plus any files listed in `artifactPaths` for what a specific widget actually showed or produced.
