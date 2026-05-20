# Streamlit Dashboards

Streamlit remains a supported dashboard deployment target on the Main Sequence Platform.

The SDK no longer ships `mainsequence.dashboards.streamlit` scaffolding, theme helpers, or reusable dashboard UI components. Dashboard projects should own their Streamlit layout, styling, sidebar widgets, and page helpers directly.

Use the SDK from Streamlit dashboards for platform work:

- read data products with `APIDataNode` and structured filters
- query assets, releases, constants, users, and other platform resources through `mainsequence.client`
- deploy dashboards through the CLI `streamlit_dashboard` release flow

## Dashboard code ownership

A Streamlit dashboard should declare its own app dependencies and helper modules in the dashboard project.

That means:

- call `st.set_page_config(...)` directly from your app
- keep reusable UI helpers inside the dashboard folder or project package
- use normal Streamlit widgets for sidebar controls and session state
- keep dashboard deployment metadata such as `README.md` next to `app.py`

The SDK should provide platform capabilities. The application should own presentation code.

## Instrument forms

Model-driven Streamlit forms for instrument configuration live under `mainsequence.instruments.streamlit`.

Install the optional dependencies before using them:

```bash
pip install "mainsequence[instruments-streamlit]"
```

See [Instrument Forms](instrument_forms.md).

## Tutorial

The tutorial Streamlit chapters show how to build and deploy a dashboard using plain Streamlit app code plus SDK client calls:

- [Part 5.1 — Streamlit Integration I](../../../tutorial/dashboards/streamlit/streamlit_integration_1.md)
- [Part 5.2 — Streamlit Integration II](../../../tutorial/dashboards/streamlit/streamlit_integration_2.md)
