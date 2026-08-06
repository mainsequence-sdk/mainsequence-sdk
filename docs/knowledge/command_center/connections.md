# Command Center Connections

The Python SDK provides generic backend clients for connection catalog entries and configured
connection instances:

```python
from mainsequence.client.command_center_models import ConnectionInstance, ConnectionType

connection_type = ConnectionType.get(type_id="postgresql.database")
connection = ConnectionInstance.get(uid="warehouse-primary")
connections = ConnectionInstance.filter(
    type_id="postgresql.database",
    workspace_uid="11111111-1111-4111-8111-111111111111",
)
```

These models map backend responses and provide normal ORM-style transport methods. They do not
define or validate any connection-type-specific `publicConfig`, query contract, response mapping,
or provider behavior.

Connection-specific schemas, including Adapter-from-API contracts, are owned by the standalone
[`@dev-mainsequence/command-center-sdk`](https://github.com/mainsequence-sdk/command-center-sdk).
Validate those payloads with its exported schemas and contract fixtures before sending them through
the generic Python client.
