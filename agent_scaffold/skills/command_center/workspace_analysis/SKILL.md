---
name: command-center-workspace-analysis
description: Use this skill when the task is about analyzing a Main Sequence Command Center workspace snapshot and answering questions about the current state of the workspace. This skill is for interpreting what the workspace was actually showing at capture time. Each widget snapshot contains that widget's current state, and agents should answer questions from those widget dumps instead of from persisted workspace metadata.
---

# Workspace Analysis Skill

Use this skill when you need to analyze a captured Command Center workspace snapshot and answer questions about the state of the workspace.

This skill is for interpretation, not workspace authoring. Use the widget snapshots together with the user's question and any business context provided in the prompt.

Use it when the user asks things like:

- "What is this workspace showing?"
- "What conclusions can I draw from the data in the workspace?"
- "The user asked a question. Can this workspace answer it, and how should I answer using only this data?"
- "When answering, focus only on the data in the workspace and the user context. Do not invent or assume anything that is not supported by the captured workspace state."

Do not use this skill to mutate workspace definitions or explain the persisted workspace JSON model. Use the relevant workspace authoring skill for that.

## Core Principle

This snapshot exists to answer questions about what the workspace was showing at capture time.

It is not a metadata archive.

It is not a workspace-definition export.

It is not a dependency-graph report.

It is a per-widget state dump intended to be interpreted by an agent.

The most important rule is:

- answer from widget snapshots
- do not answer from old workspace metadata when the widget dump already contains the real state

## How To Capture A Snapshot

Use the CLI command:

```bash
mainsequence cc workspace snapshot <workspace_id>
```

Optional output directory:

```bash
mainsequence cc workspace snapshot <workspace_id> --output-path /tmp/my-workspace-snapshot
```

Important behavior:

- the CLI opens the live Command Center workspace in a browser automation session
- it first loads the normal workspace view
- it waits 30 seconds so the workspace has time to load data and settle
- it then requests the snapshot route
- it expands the snapshot into a directory on disk

Default output location:

```text
~/mainsequence/workspaces/workspace-<workspace_id>-<timestamp>/
```

If `--output-path` looks like a file path such as `snapshot.zip`, its stem is used as the output directory.

## What The Snapshot Contains

The snapshot is produced from the live frontend state, not from persisted workspace JSON alone.

The important point is that it captures what the workspace was actually showing after load time, including widget state at capture time.

You should expect the extracted snapshot directory to contain:

- per-widget snapshot files used for agent interpretation
- widget `snapshot.json` payloads that reflect the current widget state
- additional files the frontend includes in the generated snapshot
- Playwright scroll screenshots added by the SDK under:

```text
screenshots/playwright-scroll/
  index.json
  workspace-scroll-001.png
  workspace-scroll-002.png
  ...
```

Do not assume that the top-level directory contains only one exact fixed file layout beyond the widget snapshots and the Playwright screenshot folder. Inspect the extracted snapshot directory you were given.

## What Each Widget Snapshot Means

Each widget directory should contain one `snapshot.json`.

That file is the source of truth for the widget.

It contains:

- widget identity
- widget source
- connection type name when applicable
- placement and hidden state
- the structured `snapshot` payload returned by `buildAgentSnapshot(...)`

That `snapshot` payload represents the widget's current state.

Agents should read it as:

- what kind of widget this is
- whether it is ready, loading, empty, error, or idle
- what the widget says it is showing
- the actual bounded state or data payload that the widget exposed for agent interpretation

## How To Reason About Different Widget Roles

Not every widget should be interpreted the same way. Some widgets express business meaning directly. Others only provide setup or transport context.

### Presentation widgets

Examples:

- tables
- graphs
- chart widgets
- notes
- statistics
- visual workspace widgets

These are interpretable.

Their `snapshot.json` should be read as actual visible or semantic workspace state.

This is where agents should answer questions like:

- what business data is being presented
- what the visual implies about the user's question
- what conclusion the user can support from the displayed data
- what limitation or ambiguity remains in the displayed data

### Passthrough infrastructure widgets

Examples:

- connection query widgets
- connection stream query widgets
- transform widgets

These are not the final business meaning the user is trying to inspect.

They are plumbing.

Their snapshots should be treated as metadata-only state:

- configured source
- configured path
- connection type
- transform mode
- status

Do not treat them as the final user-facing answer unless there is no downstream presentation widget and the source widget itself is the only place where the relevant data appears.

Do not infer that a dataset dump from a source widget is what the user is "seeing" if the real interpreted state lives in downstream presentation widgets.

## How To Answer Questions

When analyzing a workspace snapshot:

1. Inspect the extracted snapshot directory.
2. Find the widget snapshot folders.
3. Read each `snapshot.json`.
4. Identify which widgets are presentation widgets versus passthrough widgets.
5. Identify which widgets actually answer the user's question.
6. Prefer the downstream presentation widget state when answering "what does this workspace show?"
7. Use passthrough widgets only for setup, lineage, or failure-context explanations.
8. Use the screenshot files only as supporting context when needed.
9. Answer in terms of what the data means, not just what the widget contains.

Good answers should:

- answer the user's actual question using the business meaning of the displayed data
- explain what conclusion is supported by the workspace state
- call out uncertainty or missing evidence when the workspace does not fully answer the question
- use widget mechanics only when they are necessary to explain why the data can or cannot support an interpretation

Better answer styles are:

- "This workspace is showing current positions by instrument, so it can answer exposure questions but not execution-history questions."
- "The chart indicates that the curve moves downward after the short end, so the interpretable takeaway is the shape of the discount curve, not the raw number of plotted points."
- "The source widget is configured, but the user-facing table is empty, so the workspace does not currently support a business conclusion from the displayed data."
- "This transform widget only prepares the dataset. The answer to the user's question comes from the downstream chart, which shows the filtered result."

Bad answers are:

- repeating structural metadata without interpreting the widget state
- focusing on filesystem structure instead of widget dumps
- treating source widgets as the final answer when presentation widgets already interpret the data
- describing row counts, point counts, or widget inventory when that does not answer the user's question

## Answering Standard

The default goal is not to describe the dashboard mechanically. The goal is to explain what the dashboard means in the context of the user's question.

Prefer answers like:

- "The workspace is showing the current composition of the portfolio, so the relevant interpretation is concentration by holding, not just the fact that a table exists."
- "The dashboard is answering a pricing question through the downstream chart, which shows the resulting curve behavior. The transform and source widgets are only intermediate steps."
- "This workspace can answer whether the strategy is long or short a given exposure, but it cannot answer why the position was created because no execution or rationale data is shown."

Avoid answers like:

- "There are 25 rows in the table."
- "The graph has 3 lines."
- "There are 6 widgets on the dashboard."

Those details are only useful if they materially change the interpretation.

## Relationship To Other Command Center Skills

Use this skill alongside other Command Center skills when needed:

- `workspace_builder`
  - for creating or mutating workspace/widget definitions
  - not for analyzing current workspace state
- `app_components`
  - when a widget snapshot shows an AppComponent-specific request/response issue
- `api_mock_prototyping`
  - when the snapshot points to a mock payload or API contract question

This skill should usually be the first stop whenever the task is:

- analyze a workspace snapshot
- explain what the workspace was showing
- answer questions about widget state
- compare several widget states in one workspace

## Interpretation Rules

- Trust `snapshot.json` over older archive metadata.
- Prefer widget state over workspace structure.
- Prefer presentation widgets over passthrough widgets for user-facing answers.
- Treat missing or empty widget state as meaningful.
- Keep answers grounded in the actual widget dump, not assumptions about what the widget usually does.
- If the workspace only partially answers the user's question, say exactly which part is supported by the data and which part is not.
- Remember that the snapshot is taken from the loaded live workspace state after a 30-second bootstrap wait, so the intent is to capture the rendered state, not only saved configuration.
