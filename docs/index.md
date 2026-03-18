# Main Sequence Documentation

Main Sequence is a platform for building data products, deploying workflows, and exposing them as reusable platform resources.

The Main Sequence platform allows you to:

1. rapidly build and deploy data products and data workflows as a unified API with a normalized structure through `DataNode`s
2. rapidly deploy RBAC-enabled dashboards on the platform
3. rapidly deploy agents using the Google Agent SDK

The key idea is that you can focus on development and deployment, while the platform handles the DevOps layer.

## Choose a reading path

### Tutorial

Use the tutorial if you want a guided path from project setup to data nodes, orchestration, markets workflows, and dashboards.

Recommended starting points:

- [Part 1 — Setting a Project (CLI)](tutorial/setting_a_project.md)
- [Part 1A — Setting a Project (GUI + VS Code Extension)](tutorial/setting_a_project_gui.md)
- [Part 2 — Creating a Data Node](tutorial/creating_a_simple_data_node.md)

### Knowledge

Use the Knowledge section if you already know the workflow you are building and want the deeper conceptual guide.

Key entry points:

- [Data Nodes](knowledge/data_nodes.md)
- [Constants and Secrets](knowledge/infrastructure/constants_and_secrets.md)
- [Scheduling Jobs](knowledge/infrastructure/scheduling_jobs.md)
- [Streamlit Helpers](knowledge/dashboards/streamlit/index.md)
- [Virtual Fund Builder](knowledge/virtualfundbuilder/index.md)
- [Instruments](knowledge/instruments/index.md)

### CLI

Use the CLI section for command-focused operational work:

- [CLI Overview](cli/index.md)

### Reference

Use the generated reference when you need the API surface directly:

- [Reference Index](reference/index.md)

## What this SDK covers

The SDK is organized around a few main areas:

- `mainsequence.tdag`: `DataNode`s, persistence, orchestration, and update workflows
- `mainsequence.client`: API client models for projects, jobs, tables, assets, and sharing
- `mainsequence.virtualfundbuilder`: portfolio construction and portfolio time series workflows
- `mainsequence.instruments`: pricing-oriented market data and instrument tooling
- `mainsequence.dashboards.streamlit`: reusable Streamlit scaffolding and helper components
- `mainsequence.cli`: the `mainsequence` command-line interface

## If you are new here

The fastest onboarding path is usually:

1. create a project
2. build your first `DataNode`
3. understand RBAC and shared resources
4. schedule jobs
5. expose the result through dashboards or downstream readers

That sequence is exactly how the tutorial is structured.
