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

There are two different contract surfaces to keep separate:

- input contracts, such as `EditableFormDefinition`, when a widget needs a specialized form
- output contracts, such as the models in `mainsequence.client.command_center.data_models`, when an API should feed a Main Sequence widget directly

## SDK Helper Layout

The Command Center SDK client is split by responsibility:

- `mainsequence.client.command_center.connections`: connection type and connection instance APIs,
  including Adapter from API public config validation
- `mainsequence.client.command_center.contracts`: provider-facing contract models and helpers for
  Adapter from API, response mappings, tabular frames, and UI contracts
- `mainsequence.client.command_center.providers`: convenience builders for provider-side contracts
- `mainsequence.client.command_center.widgets`: widget payload, binding, registry, and
  connection-query helpers
- `mainsequence.client.command_center.workspaces`: workspace document and mounted-widget payload
  helpers
- `mainsequence.client.command_center.workspace`: workspace create, fetch, update, and
  widget-scoped mutation methods

## What This Section Covers

- [Workspaces](workspaces.md): how to think about workspace structure and when to mutate a single widget instead of rewriting the whole workspace
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
