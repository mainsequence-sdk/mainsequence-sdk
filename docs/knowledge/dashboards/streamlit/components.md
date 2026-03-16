# Components

The `mainsequence.dashboards.streamlit.components` package contains reusable UI helpers for common Main Sequence dashboard tasks.

These are not generic "widget utilities." They are platform-oriented helpers that already know how to work with common Main Sequence objects and session patterns.

!!! warning "IMPORTANT"
    We recommend using these helpers instead of rebuilding the same controls in every project.
    They already reflect patterns that have been tested with Main Sequence assets, auth, and dashboard session behavior.

## Available helpers

The public component exports are:

- `sidebar_asset_single_select`
- `sidebar_date_settings`
- `render_logged_user_username`
- `render_logged_user_details`
- `sidebar_logged_user_username`
- `sidebar_logged_user_details`

## `sidebar_asset_single_select(...)`

This helper gives you a sidebar asset search and selection flow.

What it does:

- lets the user type a search term
- searches Main Sequence assets by name and unique identifier
- shows results in a select box
- only changes the active asset when the user clicks `Load asset`
- persists the loaded asset in `st.session_state`

Why this is useful:

- asset selection is a very common dashboard interaction
- it is easy to get session behavior wrong when you build it yourself
- the helper already handles search, loaded state, and clearing behavior cleanly

This is a good default whenever a page needs to pivot around one selected asset.

## `sidebar_date_settings(...)`

This helper wraps a valuation-date control plus a "use last available curve" toggle.

What it does:

- renders a date selector
- stores the chosen date in session-backed config
- renders a checkbox for the last-observation behavior
- updates the process env var used by downstream curve-building logic

This is especially useful for pricing and fixed-income dashboards where the user is constantly switching valuation dates and curve behavior.

One implementation detail matters:

- it assumes your dashboard project already uses the shared `date_selector(...)` helper at `dashboards.components.date_selector`

So this component is best seen as a higher-level wrapper around an existing date-selection pattern, not a stand-alone date widget for every possible project.

## Logged-user helpers

The logged-user helpers are:

- `render_logged_user_username(...)`
- `render_logged_user_details(...)`
- `sidebar_logged_user_username(...)`
- `sidebar_logged_user_details(...)`

These helpers resolve the authenticated user from the platform auth context and render a simple summary or a fuller detail view.

They are useful when your dashboard needs to:

- show who is currently authenticated
- display organization or plan context
- render a trust-building "you are logged in as..." panel
- debug access questions without sending people to raw API calls

The implementation already handles fallback between token/header-driven user resolution paths, which is exactly the sort of small infrastructure detail that should not be reimplemented in every dashboard.

## Example usage

```python
from mainsequence.dashboards.streamlit.components import (
    render_logged_user_username,
    sidebar_asset_single_select,
)

user = render_logged_user_username(show_organization=True)
asset = sidebar_asset_single_select(title="Find asset", key_prefix="prices")
```

This gives you a clean, platform-aware user and asset entry point with very little code.

## When to use these helpers

Use them when:

- your dashboard is a normal Main Sequence dashboard
- you want predictable session behavior
- you want controls that already work with platform models

Skip or replace them only if your app has a very different interaction model.

For the common case, these helpers are the right default.
