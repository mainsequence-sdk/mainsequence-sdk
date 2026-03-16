# Streamlit Helpers

The `mainsequence.dashboards.streamlit` package is the SDK's shared helper layer for Streamlit dashboards.

It exists for one reason: building dashboards that behave well both locally and on the Main Sequence Platform without every project re-solving the same problems.

!!! warning "IMPORTANT"
    We recommend using these helpers as your default starting point for Streamlit work on Main Sequence.
    They have already been tested against the platform's packaging, auth, asset lookup, and deployment flow, so they remove a lot of avoidable friction.

This package is not a dashboard framework in the abstract. It is a practical set of helpers for common Main Sequence dashboard needs:

- bootstrapping a page with the right theme and logo behavior
- reusing sidebar controls that already work with platform objects
- rendering logged-in user information from platform auth context
- building instrument configuration forms from Pydantic models

## What is inside the package

### `scaffold.py`

This is the main entry point for most apps. It gives you `run_page(...)` and `PageConfig`, which handle page setup, theme wiring, logo/icon selection, CSS injection, and a few platform-friendly UI defaults.

See [Scaffold and Theming](scaffold_and_theming.md).

### `components/`

This folder contains reusable Streamlit UI helpers for common platform objects and session patterns:

- asset selection
- valuation-date settings
- logged-user display

See [Components](components.md).

### `instruments/`

This folder contains the instrument form factory, which renders Streamlit inputs from Pydantic models and has special handling for QuantLib-style fields such as schedules, calendars, day counters, and conventions.

See [Instrument Forms](instrument_forms.md).

### `core/`

This holds lower-level theme helpers used by the scaffold, such as CSS injection and spinner replacement. Most projects should use them through `run_page(...)` first and only drop down to the lower-level helpers if they need custom behavior.

### `assets/`

These are packaged theme and branding assets used by the scaffold:

- default `config.toml`
- default logo
- default favicon
- spinner frames

This is why the scaffold can bootstrap a usable look and feel even in a brand-new dashboard folder.

### `pages/`

This package folder is currently just structural. The reusable helpers live in `scaffold.py`, `components/`, `core/`, and `instruments/`.

## Recommended reading order

If you are starting a new dashboard:

1. Read [Scaffold and Theming](scaffold_and_theming.md)
2. Reuse the helpers from [Components](components.md)
3. If your dashboard edits or configures instrument models, use [Instrument Forms](instrument_forms.md)

## Where this fits in the tutorial

This knowledge section complements:

- [Part 5.1 — Streamlit Integration I](../../../tutorial/dashboards/streamlit/streamlit_integration_1.md)
- [Part 5.2 — Streamlit Integration II](../../../tutorial/dashboards/streamlit/streamlit_integration_2.md)

The tutorial shows how to build one concrete dashboard. This section explains the reusable SDK helpers that make that workflow easier to maintain across projects.
