# Command Center

The Python SDK exposes only the generic backend resources required to call Command Center APIs:

- `Workspace`
- `WorkspaceWidgetMutationResult`
- `RegisteredWidgetType`
- `ConnectionType`
- `ConnectionInstance`

They all live in one module:

```python
from mainsequence.client.command_center_models import (
    ConnectionInstance,
    ConnectionType,
    RegisteredWidgetType,
    Workspace,
)
```

The Python SDK does not define widget payloads, AppComponent models, tabular-frame schemas,
Adapter-from-API contracts, resource-response recipes, or FastAPI behavior. Those contracts are
owned by the standalone
[`@dev-mainsequence/command-center-sdk`](https://github.com/mainsequence-sdk/command-center-sdk)
package and should be validated against that package's schemas and fixtures.

There is deliberately no `mainsequence.client.command_center` compatibility package. Import the
backend models directly from `mainsequence.client.command_center_models`.

## What This Section Covers

- [Workspaces](workspaces.md): generic workspace operations and widget-scoped backend mutations
- [Connections](connections.md): generic connection type and instance discovery

## Operational Boundary

Use the Python models when Python code must call the Main Sequence backend. Use the standalone
TypeScript SDK as the canonical source for frontend-facing contracts and application recipes.
