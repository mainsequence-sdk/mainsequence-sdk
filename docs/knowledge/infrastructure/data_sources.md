# DataSource access

A DataSource is a platform resource that identifies a storage service in an
organization and Environment. The SDK exposes its directory metadata and the
connection material granted to a runtime workload. Applications own their storage
engines, table contracts, SQL execution, and operation-specific capability checks.

## Directory lookup

Directory reads use the current SDK authentication session:

```python
from mainsequence.client import DataSource

source = DataSource.get_by_uid(data_source_uid)
print(source.uid, source.display_name)

sources = DataSource.filter(organization_environment_uid=environment_uid)
```

The platform decides which resources the authenticated principal can discover.
The metadata adapter exposes neutral `environment_uid` and `environment_name`
properties while retaining the platform response field names. It tolerates additional response fields. It does not expose
connection credentials through directory reads or implement collection creation.

## Runtime connection material

Connection lookup requires the SDK's runtime-credential authentication mode:

```text
MAINSEQUENCE_AUTH_MODE=runtime_credential
MAINSEQUENCE_RUNTIME_CREDENTIAL_ID=<workload credential ID>
MAINSEQUENCE_RUNTIME_CREDENTIAL_SECRET=<workload credential secret>
```

Configure the SDK endpoint through its normal authentication configuration. The
existing runtime credential provider exchanges and refreshes the access token.
Call the connection method after authorizing the application operation. The
platform backend authorizes access to the requested source:

```python
from mainsequence.client import DataSource
from mainsequence.client.models_data_sources import DataSourceRuntimeError

try:
    source = DataSource.get_runtime_connection(
        data_source_uid,
    )
except DataSourceRuntimeError as error:
    handle_connection_failure(error.code)
else:
    connection = source.connection
    password = (
        connection.password.get_secret_value()
        if connection.password is not None
        else None
    )
    # Pass the connection values directly to the application's storage adapter.
```

The result is a `RuntimeDataSource` with source, organization, and Environment
UUIDs, status, class type, access mode, a boolean capability map, and a typed
`DataSourceConnection`. Organization metadata is optional response data, never
a client-side authorization requirement. The response must identify the requested
DataSource UID; the SDK does not compare ownership or expected Environment fields.
The consuming application interprets `status`, `storage_access_mode`, and the
capabilities required by its operation before opening a connection.

Passwords and TLS material use `SecretStr` and are excluded from representations
and model serialization. Optional `extra_arguments` are also excluded; read them
explicitly only if the storage adapter needs them. Never log or persist raw
connection values. The SDK fetches connection material on every call so a later
lookup observes credential rotation and access revocation. Applications should
not cache this material across operations.

A rejected access token triggers one forced refresh and one repeated request.
Other statuses and transport failures do not retry. Neither connection lookup nor
credential exchange follows redirects. Connection requests default to five-second
connect and read timeouts, configurable with `timeout=`.

| Error code | Meaning |
| --- | --- |
| `runtime_credential_not_configured` | Runtime credential mode/provider is unavailable. |
| `runtime_credential_rejected` | Credential exchange failed or the refreshed token was rejected. |
| `data_source_runtime_access_denied` | The platform denied runtime access. |
| `data_source_not_available` | The source is unavailable to this workload. |
| `data_source_unavailable` | Transport failed or the platform returned another unsuccessful status. |
| `invalid_data_source_scope` | An input UID is invalid. |
| `invalid_runtime_data_source_response` | The response is malformed or required connection fields are invalid. |
| `data_source_identity_mismatch` | The response identifies a different DataSource. |

Errors contain the code only, without response bodies or connection credentials.
Workload access to a DataSource does not authorize the human calling an application;
the application must enforce its own resource policy separately.
