# Scaffold and Theming

The normal entry point for a Main Sequence Streamlit app is:

```python
from mainsequence.dashboards.streamlit.scaffold import PageConfig, run_page
```

Then, at the top of each page:

```python
run_page(
    PageConfig(
        title="My Dashboard",
        use_wide_layout=True,
        inject_theme_css=True,
    )
)
```

This is the recommended pattern because it handles the repetitive setup that most dashboard pages need anyway.

## What `run_page(...)` does

When you call `run_page(...)`, it does the page-level work that usually gets repeated across dashboards:

- calls `st.set_page_config(...)`
- resolves the default or custom logo and favicon
- injects the packaged theme CSS
- replaces the default Streamlit spinners with the packaged spinner style
- hides the deploy button
- optionally hides Streamlit's native multipage sidebar
- initializes session state through your callback
- builds a page context through your callback
- renders a header through your callback, or falls back to a simple title

In practice, this means you can keep each Streamlit page focused on business logic rather than UI bootstrapping.

## Why the scaffold is useful on Main Sequence

The point of the scaffold is not only visual consistency.

It also reduces a few practical problems that show up again and again in deployed dashboards:

- forgetting page config on some pages
- inconsistent logo or icon handling
- CSS hacks copied differently across projects
- theme files not being present in a fresh app directory
- small differences between local runs and packaged runs

The helper package already handles these concerns in a way that has been exercised on the platform.

## `PageConfig` in plain English

`PageConfig` is the contract for page setup.

The most useful fields are:

- `title`: page title shown in the browser and header fallback
- `build_context`: callback that builds a context object from session state
- `render_header`: custom header renderer
- `init_session`: callback that seeds session defaults
- `logo_path`: optional custom logo override
- `page_icon_path`: optional custom icon override
- `use_wide_layout`: use wide layout instead of centered
- `hide_streamlit_multipage_nav`: hide Streamlit's native multipage navigation
- `inject_theme_css`: inject the packaged CSS helpers

If you do not need custom context or a custom header yet, you can start with only `title`.

## Automatic theme bootstrapping

One detail matters a lot for real projects: the scaffold can bootstrap `.streamlit/config.toml` into the app directory.

Why this matters:

- a fresh dashboard folder often does not have its own `.streamlit/config.toml`
- without it, the app may not pick up the expected theme
- the scaffold copies the packaged default theme once and reruns so the theme applies

That makes the out-of-the-box experience much smoother, especially for new projects and tutorial dashboards.

## Lower-level theme helpers

The scaffold relies on `mainsequence.dashboards.streamlit.core.theme`.

That module exposes lower-level helpers such as:

- `inject_css_for_dark_accents()`
- `override_spinners()`
- `remove_deploy_button()`
- `explain_theming()`

Most projects should call `run_page(...)` instead of wiring those pieces by hand. Drop down to the lower-level helpers only when you have a specific reason to customize the default behavior.

## Recommended usage pattern

For most dashboards:

1. call `run_page(...)` at the top of every page
2. keep any app-specific context in `build_context`
3. use the packaged components for common sidebar and user UI
4. keep custom CSS small and additive

That pattern gives you a much more predictable dashboard codebase than hand-rolled page setup on every file.
