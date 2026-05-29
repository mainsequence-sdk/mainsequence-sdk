# Main Sequence Documentation

Main Sequence is a platform for building data products, deploying workflows, and exposing them as reusable platform resources.

The Main Sequence platform allows you to:

1. rapidly build and deploy data products and data workflows as a unified API with normalized table contracts through `MetaTable`s and `DataNode`s
2. rapidly deploy RBAC-enabled dashboards on the platform
3. rapidly deploy agents using the Google Agent SDK

The key idea is that you can focus on development and deployment, while the platform handles the DevOps layer.

## Choose a reading path

### Tutorial

Use the tutorial if you want a guided path from project setup to canonical tables, DataNode update workflows, orchestration, and dashboards.

The main tutorial path is intentionally CLI-only.

Recommended starting points:

- [Part 1 — Setting a Project (CLI)](tutorial/setting_a_project.md)
- [Part 2 — Working With MetaTables](tutorial/working_with_meta_tables.md)
- [Part 3 — Creating a Data Node](tutorial/creating_a_simple_data_node.md)
- [Part 3.1 — Role-Based Access Control](tutorial/role_based_access_control.md)
- [Part 3.2 — Create Your First API](tutorial/create_your_first_api.md)
- [Part 6 — Turn Your Project Into an Agent](tutorial/project_to_agent.md)

### Knowledge

Use the Knowledge section if you already know the workflow you are building and want the deeper conceptual guide.

Key entry points:

- [Data Nodes](knowledge/data_nodes.md)
- [Command Center](knowledge/command_center/index.md)
- [Command Center Widget Data Contracts](knowledge/command_center/widget_data_contracts.md)
- [MetaTables](knowledge/meta_tables/index.md)
- [Constants and Secrets](knowledge/infrastructure/constants_and_secrets.md)
- [Scheduling Jobs](knowledge/infrastructure/scheduling_jobs.md)
- [Streamlit Dashboards](knowledge/dashboards/streamlit/index.md)

### CLI

Use the CLI section for command-focused operational work:

- [CLI Overview](cli/index.md)

### Reference

Use the generated reference when you need the API surface directly:

- [Reference Index](reference/index.md)

## What this SDK covers

The SDK is organized around a few main areas:

- `mainsequence.meta_tables`: `DataNode`s, persistence, orchestration, and update workflows
- `mainsequence.client`: API client models for projects, jobs, tables, platform resources, and sharing
- `mainsequence.cli`: the `mainsequence` command-line interface

## If you are new here

The fastest onboarding path is usually:

1. create a project
2. model a canonical table with a backend-managed `MetaTable`
3. build your first `DataNode` as an opinionated MetaTable-backed update workflow
4. understand RBAC and shared resources
5. add an API or another consumer surface
6. schedule jobs
7. expose the result through dashboards, downstream readers, or agent-facing project surfaces

That sequence is exactly how the tutorial is structured.
