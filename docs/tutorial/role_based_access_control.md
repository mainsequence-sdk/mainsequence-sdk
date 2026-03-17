# Part 2.1: Role-Based Access Control (RBAC)

## Quick Summary

In this part, you will:

- understand the Main Sequence RBAC model at a practical level
- learn which platform resources can be shared directly
- see how `DataNodeStorage` controls who can read and who can write to a published dataset
- use the CLI to share the DataNodes you created in Part 2
- understand where constants and secrets fit inside the same access model

DataNodes created in this part: **none**.

Platform resources introduced in this part: **`Project`**, **`DataNodeStorage`**, **`Constant`**, **`Secret`**, **`Bucket`**, and **`ResourceRelease`**.

## Why this comes right after your first DataNode

Once you have created your first `DataNode`, the next question is no longer only "does it run?".

The next practical question is:

> who should be allowed to read this dataset, and who should be allowed to maintain or publish into it?

That is the RBAC question.

This belongs early in the tutorial because once a `DataNode` becomes useful, it usually stops being personal code and starts becoming a shared platform resource.

## RBAC in Main Sequence

Main Sequence applies role-based access control at the resource level.

In plain English, RBAC answers:

- who can view a resource
- who can edit a resource
- who can safely use it inside a broader workflow

That matters because the platform is built around shared objects, not just private scripts.

Examples of resources where access boundaries matter:

- `Project`
- `DataNodeStorage`
- `Constant`
- `Secret`
- `Bucket`
- `ResourceRelease`

You can think of these as different layers of collaboration:

- `Project`: who can work inside the code and execution boundary
- `DataNodeStorage`: who can read or maintain a published table
- `Constant` and `Secret`: who can access runtime configuration
- `Bucket`: who can access files and artifacts
- `ResourceRelease`: who can access deployed resources such as dashboards and agents

## The important detail for DataNodes

When people say "share a DataNode", the platform object you are really sharing is usually the `DataNodeStorage`.

That distinction matters:

- the Python `DataNode` class defines how data is produced
- the `DataNodeStorage` is the published table other users, jobs, dashboards, and `APIDataNode`s consume

So in practice:

- `view` access controls who can read the published dataset
- `edit` access controls who can maintain, update, or administrate that dataset

This is the most practical RBAC boundary for data work on Main Sequence.

## Find the DataNode storages you created in Part 2

The first table from Part 2 has a friendly identifier based on `example_random_number`.

!!! warning "IMPORTANT"
    In the tutorial code for `DailyRandomAddition`, we did not define a friendly `get_table_metadata()` identifier.
    So the first DataNode is easier to find by identifier, while the dependent one may need to be located through the CLI list output and its `Source Class`.

Find the first tutorial table:

```bash
mainsequence data-node list --filter identifier__contains=example_random_number
```

If you want to inspect the full record once you have the id:

```bash
mainsequence data-node detail <DATA_NODE_STORAGE_ID>
```

For the dependent node, list visible data nodes and use the `Source Class` column to identify the row created by `DailyRandomAddition`:

```bash
mainsequence data-node list
```

## Share the DataNode you just created

Once you have the storage id, you can inspect who can read it:

```bash
mainsequence data-node can_view <DATA_NODE_STORAGE_ID>
```

Inspect who can edit it:

```bash
mainsequence data-node can_edit <DATA_NODE_STORAGE_ID>
```

Grant user `7` read access:

```bash
mainsequence data-node add_to_view <DATA_NODE_STORAGE_ID> 7
```

Grant user `7` edit access:

```bash
mainsequence data-node add_to_edit <DATA_NODE_STORAGE_ID> 7
```

Remove those permissions again if needed:

```bash
mainsequence data-node remove_from_view <DATA_NODE_STORAGE_ID> 7
mainsequence data-node remove_from_edit <DATA_NODE_STORAGE_ID> 7
```

Here, the practical split is:

- `add_to_view`: let someone consume the dataset
- `add_to_edit`: let someone maintain or publish into the dataset

## Read access vs write access for DataNodes

This is the simplest human-friendly rule:

- if someone only needs the table in dashboards, notebooks, or downstream reads, give them `view`
- if someone is responsible for updating, administering, or maintaining the table, give them `edit`

That gives you a clean way to separate readers from maintainers without cloning code or duplicating datasets.

## Constants and secrets are part of the same RBAC model

The same idea applies to configuration resources.

A constant is a shared, non-sensitive runtime setting.

A secret is a protected runtime credential.

Examples:

- share a constant when other users should be able to read or change a common operating parameter
- share a secret only when another user or workflow truly needs that credential

Example CLI flow for constants:

```bash
mainsequence constants create MODEL__DEFAULT_WINDOW 252
mainsequence constants can_view 42
mainsequence constants add_to_view 42 7
```

Example CLI flow for secrets:

```bash
mainsequence secrets create POLYGON_API_KEY your-secret-value
mainsequence secrets can_view 42
mainsequence secrets add_to_view 42 7
```

## Why this matters for the rest of the tutorial

Later parts of the tutorial introduce:

- multi-asset data nodes
- scheduled jobs
- artifacts and buckets
- dashboards and releases

At that point, RBAC stops being an admin topic and becomes part of daily engineering work.

You need to know:

- who can read the published tables
- who can maintain them
- who can access the runtime configuration behind them
- who can access the deployed resources built on top of them

For a deeper reference, see [Constants and Secrets Knowledge Guide](../knowledge/infrastructure/constants_and_secrets.md).
