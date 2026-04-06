# Command Center Workspaces

Command Center workspaces store shared application structure:

- workspace metadata
- shared controls
- layout data
- mounted widget instances

The important operational detail is that you do not always need to fetch and rewrite the full workspace document when the user wants to change one widget.

For widget-specific mutations, prefer the widget-scoped workspace endpoints exposed by `mainsequence.client.command_center.Workspace`:

- `patch_workspace_widget(...)`
- `delete_workspace_widget(...)`
- `move_workspace_widget(...)`

These endpoints mutate one mounted widget instance directly, without requiring a full workspace fetch/patch round-trip.

## When To Use Full Workspace Update vs Widget Mutation

Use a full workspace create/update when you are changing:

- workspace metadata
- shared `controls`
- shared `grid`
- shared `autoGrid`
- multiple widgets at once as one coordinated document change

Use widget-scoped mutation when you are changing one mounted widget:

- widget title
- widget props
- widget layout
- widget runtime state
- widget placement inside the workspace
- deleting one widget instance

## Why This Matters

Widget-scoped mutation is safer and simpler for targeted edits:

- less payload construction
- less chance of accidentally overwriting unrelated widgets
- no need to re-send the entire workspace JSON
- clearer intent when the user says "change this widget" or "move this widget"

## `patch_workspace_widget(...)`

Use this to partially update one mounted widget instance in place.

```python
from mainsequence.client.command_center import Workspace

workspace = Workspace.get(7)
result = workspace.patch_workspace_widget(
    "widget-existing",
    widget={
        "title": "Funding Curve Source v2",
        "props": {"nodeId": 123},
        "runtimeState": {"tab": "bindings"},
    },
)
```

Important behavior:

- the request body must contain `widget`
- the update is shallow, not a deep merge
- if you send `props`, `layout`, `bindings`, or `row`, that top-level value replaces the old one
- `runtimeState` is write-only convenience for the current user's workspace user-state
- `runtimeState` is not stored in shared workspace JSON
- if `widget.id` is present, it must match the target widget instance id
- this endpoint does not allow replacing `widget.row.children`
- `widget.widgetId` may change, but it must stay a valid registered widget type

Success response fields:

- `workspaceId`
- `widgetInstanceId`
- `parentWidgetId`
- `widget`
- `updatedAt`

## `delete_workspace_widget(...)`

Use this to remove one mounted widget instance.

```python
from mainsequence.client.command_center import Workspace

workspace = Workspace.get(7)
workspace.delete_workspace_widget("widget-existing")
```

If the target widget still contains `row.children`, you must pass `recursive=True`:

```python
workspace.delete_workspace_widget("row-1", recursive=True)
```

Important behavior:

- leaf widgets delete directly
- if a widget still has nested `row.children`, the backend can reject the delete unless `recursive=true`
- deleting a widget also removes runtime state for the deleted widget ids from the current user's workspace user-state

## `move_workspace_widget(...)`

Use this to reorder or relocate an existing widget instance inside the same workspace.

```python
from mainsequence.client.command_center import Workspace

workspace = Workspace.get(7)
result = workspace.move_workspace_widget(
    "widget-existing",
    parent_widget_id="row-1",
    index=0,
)
```

Important behavior:

- `parent_widget_id=None` moves the widget to the top level
- `index=None` appends to the end of the target list
- this endpoint only moves the widget and does not modify widget content
- runtime state is unchanged
- the backend rejects moves into the widget itself or one of its descendants

## Recommended Operational Rule

When a user asks to mutate a specific widget:

1. identify the exact workspace id
2. identify the exact widget instance id
3. use the widget-scoped endpoint instead of rewriting the full workspace

If the target widget instance id is ambiguous, confirm it before mutating.

That is especially important when a workspace contains multiple widgets of the same registered `widgetId`.

For custom application inputs rendered inside AppComponent widgets, see [Forms](forms.md).
