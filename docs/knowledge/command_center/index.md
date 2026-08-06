# Command Center

Command Center is the application layer used to assemble interactive workspaces from reusable widgets.

This is where a lot of the higher-level user experience work happens:

- shared workspaces
- mounted widget instances
- widget-scoped mutations
- AppComponent widgets
- form-driven interactions

The practical split is:

- `Workspace` is the shared container
- registered widget types define what can be mounted
- an AppComponent widget connects a UI widget to an application endpoint
- forms define how richer inputs should be rendered and edited

There are three different contract surfaces to keep separate:

- input contracts, such as `EditableFormDefinition`, when a widget needs a specialized form
- output contracts, such as the models in `mainsequence.command_center.sdk.data_models`, when an API should feed a Main Sequence widget directly
- resource contracts under `mainsequence.command_center.sdk.resource` for canonical lists,
  pagination, discovered actions, and summaries

## Package Namespace

Command Center is owned by the top-level `mainsequence.command_center` package. The former
`mainsequence.client.command_center` path remains as a deprecated compatibility namespace, but new
code should import from the top-level package.

## SDK Helper Layout

The Command Center Python package is split by responsibility:

- `mainsequence.command_center.connections`: connection type and connection instance APIs,
  including Adapter from API public config validation
- `mainsequence.command_center.sdk.contracts`: provider-facing contract models and helpers for
  Adapter from API, response mappings, tabular frames, table visual metadata, and UI contracts
- `mainsequence.command_center.sdk.providers`: convenience builders for provider-side contracts
- `mainsequence.command_center.sdk.resource`: the Python projection of the standalone
  Command Center SDK resource contract
- `mainsequence.command_center.workspaces.widgets`: widget payload, table/pro-table props, tabular
  transform props, binding, registry, and connection-query helpers
- `mainsequence.command_center.workspaces`: workspace models, create/fetch/update clients,
  snapshots, AppComponent definitions, documents, mounted-widget payloads, and widget-scoped
  mutation helpers

The former top-level `contracts`, `data_models`, `providers`, `widgets`, `workspace`,
`workspace_snapshot`, and `app_component` modules were removed. Use the canonical nested packages
above; there are no compatibility shims for those flattened module names.

## What This Section Covers

- [Workspaces](workspaces.md): how to think about workspace structure and when to mutate a single widget instead of rewriting the whole workspace
- [Resource SDK For FastAPI](resource_sdk.md): how to build canonical Command Center resource APIs
- [Connections](connections.md): SDK helpers and strict validation for Command Center connection instances, including `command_center.adapter_from_api`
- [Forms](forms.md): when an AppComponent should rely on the default argument resolution and when it should return a custom `EditableFormDefinition`
- [Widget Data Contracts](widget_data_contracts.md): the exact response shapes some Main Sequence widgets expect when an API feeds them directly

## Operational Rule

When the user asks to change one widget, do not default to rewriting the entire workspace.

Use the widget-scoped methods on `Workspace` when the target is one mounted widget:

- `patch_workspace_widget(...)`
- `delete_workspace_widget(...)`
- `move_workspace_widget(...)`

That is the same operational pattern already used in the CLI and SDK examples.
