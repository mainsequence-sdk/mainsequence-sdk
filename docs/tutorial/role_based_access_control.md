# Part 3.1: Role-Based Access Control (RBAC)

## Quick Summary

In this part, you will:

- understand the Main Sequence RBAC model at a practical level
- learn which platform resources can be shared directly
- see how MetaTable-backed storage controls who can read and who can write to a published dataset
- use the CLI to share the DataNodes you created in Part 3
- understand where constants and secrets fit inside the same access model

DataNodes created in this part: **none**.

Platform resources introduced in this part: **`Project`**, **`MetaTable`**, **`TimeIndexMetaData`**, **`Constant`**, **`Secret`**, **`Bucket`**, and **`ResourceRelease`**.

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
- `MetaTable`
- `TimeIndexMetaData`
- `Constant`
- `Secret`
- `Bucket`
- `ResourceRelease`

You can think of these as different layers of collaboration:

- `Project`: who can work inside the code and execution boundary
- `MetaTable` / `TimeIndexMetaData`: who can read or maintain a published table
- `Constant` and `Secret`: who can access runtime configuration
- `Bucket`: who can access files and artifacts
- `ResourceRelease`: who can access deployed resources such as dashboards and APIs

## The important detail for DataNodes

When people say "share a DataNode", the platform object you are really sharing
is the MetaTable-backed storage table. During the compatibility transition,
some CLI output still names that DataNode-produced table `TimeIndexMetaData`.

That distinction matters:

- the Python `DataNode` class defines how data is produced
- the MetaTable-backed storage table is the published table other users, jobs, dashboards, and `APIDataNode`s consume

So in practice:

- `view` access controls who can read the published dataset
- `edit` access controls who can maintain, update, or administrate that dataset

This is the most practical RBAC boundary for data work on Main Sequence.

## Find the MetaTable storage you created in Part 3

The first table from Part 3 has a friendly identifier based on `example_random_number`.

Find the first tutorial table:

```bash
mainsequence meta-table list --filter identifier__contains=example_random_number
```

If you want to inspect the full record once you have the uid:

```bash
mainsequence meta-table detail <META_TABLE_UID>
```

For the dependent node, list tutorial MetaTables and use the `Identifier`
column to identify the table created for `DailyRandomAddition`:

```bash
mainsequence meta-table list --filter identifier__contains=example_random_add
```

## Share the DataNode table you just created

Once you have the MetaTable uid, you can inspect who can read it:

```bash
mainsequence meta-table can_view <META_TABLE_UID>
```

Inspect who can edit it:

```bash
mainsequence meta-table can_edit <META_TABLE_UID>
```

Grant user `7` read access:

```bash
mainsequence meta-table add_to_view <META_TABLE_UID> 7
```

Grant user `7` edit access:

```bash
mainsequence meta-table add_to_edit <META_TABLE_UID> 7
```

Remove those permissions again if needed:

```bash
mainsequence meta-table remove_from_view <META_TABLE_UID> 7
mainsequence meta-table remove_from_edit <META_TABLE_UID> 7
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

- multi-entity data nodes
- scheduled jobs
- artifacts and buckets
- dashboards and releases

At that point, RBAC stops being an admin topic and becomes part of daily engineering work.

You need to know:

- who can read the published tables
- who can maintain them
- who can access the runtime configuration behind them
- who can access the deployed resources built on top of them

For a deeper reference, see [Users and Access](../knowledge/infrastructure/users_and_access.md) and [Constants and Secrets](../knowledge/infrastructure/constants_and_secrets.md).
