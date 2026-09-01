# Streamlit Dashboard Support Removed

Main Sequence no longer supports managed Streamlit dashboard discovery,
deployment, release, authentication, or runtime operation. The
`streamlit_dashboard` release kind, dashboard repository-resource discriminator,
SDK creation helper, and dedicated CLI commands have been removed without
compatibility aliases.

Existing Streamlit applications are not converted automatically. They must be
deliberately rewritten for a supported application surface. See the
[Streamlit dashboard removal guide](../../../migrations/streamlit-dashboard-removal.md)
for the exact SDK symbols and commands that were removed.
