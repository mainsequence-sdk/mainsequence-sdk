# Main Sequence Documentation

Main Sequence is a platform for building data products, deploying workflows, and exposing them as reusable platform resources.

The Main Sequence platform allows you to:

1. rapidly build and deploy data products and data workflows as a unified API with normalized table contracts through `MetaTable`s and `DataNode`s
2. rapidly deploy RBAC-enabled dashboards on the platform
3. rapidly deploy agents using the Google Agent SDK

The key idea is that you can focus on development and deployment, while the platform handles the DevOps layer.

## Choose a reading path

### Knowledge

Use the Knowledge section if you already know the workflow you are building and want the deeper conceptual guide.

Key entry points:

- [Data Nodes](knowledge/data_nodes.md)
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

## Tutorial ownership

The beginner tutorial is maintained in its own self-contained Project rather
than inside this SDK reference site. This repository remains the source of truth
for SDK APIs, concepts, CLI behavior, and generated reference documentation.
