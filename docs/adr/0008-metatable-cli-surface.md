# ADR 0008: MetaTable CLI Surface

## Status

Accepted

## Related ADRs

- ADR 0007: Client-Wide UID Public Identity

## Context

MetaTables are now the SDK contract for row-oriented relational application data.
The client model already exposes registration, contract validation, introspection,
labels, sharing, UID lookup, and governed compiled SQL execution through
`mainsequence.client.models_metatables.MetaTable`.

The CLI currently has mature resource ergonomics for listing, detail views,
typed deletion, labels, sharing, and query execution on other SDK resources, but
MetaTables do not yet have their own first-class command group. That leaves users
with an inconsistent path: they can register and operate MetaTables from Python,
but not from the same operational CLI surface used for DataNodes, projects,
constants, secrets, and command-center resources.

This ADR defines the CLI shape for MetaTables and the implementation tasks
needed to make the CLI a supported operational surface without bringing back any
deprecated row-table API.

## Decision

Add a top-level `mainsequence meta-table` Typer command group backed by
`mainsequence.client.models_metatables.MetaTable`.

The command group must use UID-based public identity. Every command that targets
one MetaTable accepts `META_TABLE_UID` as a string and calls `MetaTable.get(uid=...)`
or a detail action on the retrieved instance. No public MetaTable command may
coerce the resource reference with `int(...)`.

The canonical public group name is hyphenated:

```bash
mainsequence meta-table ...
```

The CLI may also expose a hidden underscore alias:

```bash
mainsequence meta_table ...
```

This alias exists only for consistency with current CLI command naming patterns.
It is not a separate API surface and must call the same implementation functions.

## Required command surface

### Discovery and detail

- `mainsequence meta-table list`
- `mainsequence meta-table list --filter KEY=VALUE`
- `mainsequence meta-table list --show-filters`
- `mainsequence meta-table list --data-source-uid <DATA_SOURCE_UID>`
- `mainsequence meta-table detail <META_TABLE_UID>`

List output must print `uid` as the first column. The default table should include
UID, storage hash, identifier, namespace, management mode, data source UID,
physical schema, physical table name, and open/shared status.

Detail output must include UID, storage hash, identifier, namespace, description,
management mode, data source, physical schema/table, contract version, labels,
protection state, open/shared state, columns, indexes, foreign keys, incoming
foreign keys, and creation metadata when present.

### Registration and contract validation

- `mainsequence meta-table register --file registration.json`
- `mainsequence meta-table register --file registration.yaml`
- `mainsequence meta-table validate-contract --file contract.json`
- `mainsequence meta-table validate-contract --file contract.yaml`
- `mainsequence meta-table validate-existing-contract <META_TABLE_UID> --file contract.json`
- `mainsequence meta-table introspect <META_TABLE_UID>`

`register` must accept a full `MetaTableRegistrationRequest` payload and delegate
to `MetaTable.register(...)`.

`validate-contract` must accept a `MetaTableValidateContractRequest` payload and
delegate to `MetaTable.validate_contract(...)`.

`validate-existing-contract` must retrieve the target MetaTable by UID and then
call `validate_existing_contract(...)` on that instance.

`introspect` must retrieve the target MetaTable by UID and call `introspect(...)`.

The CLI must not import SQLAlchemy or add SQLAlchemy to package dependencies.
SQLAlchemy helpers remain part of the Python SDK authoring path; CLI payloads are
JSON/YAML documents.

### Governed SQL execution

- `mainsequence meta-table execute-operation --file operation.json`
- `mainsequence meta-table execute-operation --file operation.yaml`
- `mainsequence meta-table run-query <META_TABLE_UID> <SQL>`

`execute-operation` is the complete path. It accepts a
`MetaTableCompiledSQLOperation` payload and delegates to
`MetaTable.execute_operation(...)`.

`run-query` is a read-oriented convenience command. It must:

- require `META_TABLE_UID`;
- accept SQL as an argument or through `--file`;
- build a compiled SQL operation with `operation="select"`;
- include a scope table entry with `meta_table_uid=<META_TABLE_UID>` and
  `access="read"`;
- support optional `--max-rows` and `--statement-timeout-ms`;
- reject obvious non-read statements before calling the backend;
- delegate final authorization and execution to `MetaTable.execute_operation(...)`.

Write operations must use `execute-operation --file ...` so the user declares the
full operation, statement, parameters, scope, and limits explicitly.

### Labels and sharing

- `mainsequence meta-table add-label <META_TABLE_UID> --label LABEL`
- `mainsequence meta-table remove-label <META_TABLE_UID> --label LABEL`
- `mainsequence meta-table can_view <META_TABLE_UID>`
- `mainsequence meta-table can_edit <META_TABLE_UID>`
- `mainsequence meta-table add_to_view <META_TABLE_UID> <USER_ID>`
- `mainsequence meta-table add_to_edit <META_TABLE_UID> <USER_ID>`
- `mainsequence meta-table remove_from_view <META_TABLE_UID> <USER_ID>`
- `mainsequence meta-table remove_from_edit <META_TABLE_UID> <USER_ID>`
- `mainsequence meta-table add_team_to_view <META_TABLE_UID> <TEAM_ID>`
- `mainsequence meta-table add_team_to_edit <META_TABLE_UID> <TEAM_ID>`
- `mainsequence meta-table remove_team_from_view <META_TABLE_UID> <TEAM_ID>`
- `mainsequence meta-table remove_team_from_edit <META_TABLE_UID> <TEAM_ID>`

Label commands must route through `LabelableObjectMixin` on `MetaTable`.

Sharing commands must route through `ShareableObjectMixin` on `MetaTable`. User
and team subject identifiers remain the existing permission-subject identifiers
until the backend permission contract migrates.

### Deletion

- `mainsequence meta-table delete <META_TABLE_UID>`

Deletion must retrieve the MetaTable by UID, print a preview, require typed
verification, and then call the model's `delete(...)` path. The preview must show
UID, storage hash, identifier, namespace, management mode, physical schema/table,
protection state, and data source. The command must not expose direct database
drop behavior unless the backend adds a typed MetaTable delete contract for that
specific behavior.

## Required examples

The implementation must include examples that use this shape in CLI docs,
tutorials, and command tests.

### Discovery examples

```bash
mainsequence meta-table list --data-source-uid <DATA_SOURCE_UID>
mainsequence meta-table list --filter namespace=tutorial --filter management_mode=platform_managed
mainsequence meta-table detail <META_TABLE_UID>
```

### Backend-managed registration example

`meta-table.registration.yaml`:

```yaml
data_source_uid: <DATA_SOURCE_UID>
management_mode: platform_managed
storage_hash: tutorial_assets
identifier: tutorial_assets
namespace: tutorial
description: Backend-managed asset registry used by the tutorial.
protect_from_deletion: true
open_for_everyone: false
labels:
  - tutorial
  - registry
provisioning:
  create_if_missing: true
table_contract:
  version: relational-table.v1
  physical:
    schema: tutorial
    table_name: assets
  columns:
    - name: id
      data_type: integer
      nullable: false
      primary_key: true
    - name: symbol
      data_type: text
      nullable: false
      unique: true
    - name: name
      data_type: text
      nullable: false
    - name: is_active
      data_type: boolean
      nullable: false
  indexes:
    - name: tutorial_assets_symbol_idx
      columns:
        - symbol
      unique: true
  foreign_keys: []
```

```bash
mainsequence meta-table register --file meta-table.registration.yaml
mainsequence meta-table detail <META_TABLE_UID>
mainsequence meta-table introspect <META_TABLE_UID>
```

### Contract validation example

`meta-table.contract.yaml`:

```yaml
management_mode: platform_managed
storage_hash: tutorial_assets
table_contract:
  version: relational-table.v1
  physical:
    schema: tutorial
    table_name: assets
  columns:
    - name: id
      data_type: integer
      nullable: false
      primary_key: true
    - name: symbol
      data_type: text
      nullable: false
      unique: true
```

```bash
mainsequence meta-table validate-contract --file meta-table.contract.yaml
mainsequence meta-table validate-existing-contract <META_TABLE_UID> --file meta-table.contract.yaml
```

### Governed query examples

Read-oriented convenience command:

```bash
mainsequence meta-table run-query <META_TABLE_UID> \
  "select id, symbol, name from tutorial.assets where is_active = true order by symbol" \
  --max-rows 100 \
  --statement-timeout-ms 5000
```

Full operation payload for explicit scope and parameters:

`meta-table.operation.yaml`:

```yaml
operation: select
version: compiled-sql.v1
dialect: postgresql
statement:
  sql: "select id, symbol, name from tutorial.assets where is_active = %(is_active)s order by symbol"
  parameters:
    is_active: true
  paramstyle: pyformat
scope:
  tables:
    - meta_table_uid: <META_TABLE_UID>
      alias: assets
      access: read
limits:
  max_rows: 100
  statement_timeout_ms: 5000
```

```bash
mainsequence meta-table execute-operation --file meta-table.operation.yaml
```

### Label and sharing examples

```bash
mainsequence meta-table add-label <META_TABLE_UID> --label tutorial
mainsequence meta-table can_view <META_TABLE_UID>
mainsequence meta-table add_to_view <META_TABLE_UID> <USER_ID>
mainsequence meta-table remove-label <META_TABLE_UID> --label tutorial
```

### Delete example

```bash
mainsequence meta-table delete <META_TABLE_UID>
```

The delete example must show typed verification in the tutorial text before the
destructive call is made.

## Tutorial guidelines

The tutorial must teach MetaTables as the CLI-visible row-oriented application
data contract. It should use a market-domain scenario only as application data,
not as an SDK-bundled market library.

Required tutorial structure:

- Start with the mental model: a MetaTable is registered metadata plus governed
  runtime access to a physical relational table.
- Use a backend-managed registration payload first.
- Explain `data_source_uid`, `management_mode`, `storage_hash`, `identifier`,
  `namespace`, and `table_contract`.
- Show `validate-contract` before `register`.
- Show `detail` and `introspect` after registration.
- Show `list --filter ...` with UID-oriented filters.
- Show `run-query` for read-oriented inspection.
- Show `execute-operation --file ...` for the full governed SQL contract.
- Show labels as discovery metadata.
- Show sharing commands as permission operations.
- Show delete only with explicit verification and deletion caveats.
- Keep SQLAlchemy as an optional Python authoring path; CLI examples must use
  JSON or YAML payload files.
- Do not add dependency installation steps for SQLAlchemy.
- Do not use integer resource IDs for MetaTable lookup.
- Do not reference deprecated row-table APIs, file paths, command groups, or
  tutorials.

## Non-goals

- Do not add SQLAlchemy to `pyproject.toml`.
- Do not introduce direct database connection handling in the CLI.
- Do not add migration management, Alembic operations, or schema diffing.
- Do not expose unmanaged raw SQL execution that bypasses
  `MetaTable.execute_operation(...)`.
- Do not add public command aliases using deprecated row-table terminology.
- Do not document integer resource IDs as MetaTable lookup arguments.

## Required implementation tasks

### 1. API wrapper layer

- [ ] Add `list_meta_tables(filters=None, timeout=None)` to `mainsequence/cli/api.py`.
- [ ] Add `get_meta_table(meta_table_uid, timeout=None)` using `MetaTable.get(uid=...)`.
- [ ] Add `register_meta_table(payload, timeout=None)` using `MetaTable.register(...)`.
- [ ] Add `validate_meta_table_contract(payload, timeout=None)` using `MetaTable.validate_contract(...)`.
- [ ] Add `validate_existing_meta_table_contract(meta_table_uid, payload, timeout=None)`.
- [ ] Add `introspect_meta_table(meta_table_uid, timeout=None)`.
- [ ] Add `execute_meta_table_operation(operation, timeout=None)`.
- [ ] Add `run_meta_table_query(meta_table_uid, sql, max_rows=None, statement_timeout_ms=None, timeout=None)`.
- [ ] Add `delete_meta_table(meta_table_uid, timeout=None)`.
- [ ] Add MetaTable label wrapper functions using `_mutate_labelable_object_labels(..., object_lookup_field="uid")`.
- [ ] Add MetaTable share wrapper functions using `_get_shareable_object_access_state(...)`, `_mutate_shareable_object_access(...)`, and `_mutate_shareable_object_team_access(...)` with `object_lookup_field="uid"`.

### 2. CLI command group

- [ ] Add `meta_table_group = typer.Typer(help="MetaTable commands")`.
- [ ] Register `app.add_typer(meta_table_group, name="meta-table")`.
- [ ] Register hidden alias `app.add_typer(meta_table_group, name="meta_table", hidden=True)`.
- [ ] Import the new API wrapper functions into `mainsequence/cli/cli.py`.
- [ ] Add `METATABLE_MODEL_REF` or equivalent metadata wiring for filter help.
- [ ] Add `list`, `detail`, `register`, `validate-contract`, `validate-existing-contract`, `introspect`, `execute-operation`, `run-query`, `delete`, label, and sharing commands.
- [ ] Add hidden underscore aliases only where the current CLI convention already expects them, such as label commands.

### 3. Payload parsing and output formatting

- [ ] Reuse the existing JSON/YAML file parsing style already used by workspace and project commands.
- [ ] Add a MetaTable row formatter for list output.
- [ ] Add a MetaTable detail formatter for core fields.
- [ ] Add compact column/index/foreign-key formatters for detail output.
- [ ] Add an operation result formatter that prints affected row counts, returned rows, warnings, and backend metadata when present.
- [ ] Ensure `--json` emits the raw JSON-compatible payload for every MetaTable command.

### 4. Filter behavior

- [ ] Use `MetaTable.FILTERSET_FIELDS` for `--show-filters`.
- [ ] Keep first-class CLI options UID-oriented, including `--data-source-uid`.
- [ ] Do not add first-class `--data-source-id` or other integer resource options.
- [ ] Classify backend-only filter fields before documenting them in tutorials.
- [ ] Add tests proving UUID-like filter values are preserved as strings.

### 5. Governed SQL safeguards

- [ ] Build `run-query` payloads as `MetaTableCompiledSQLOperation` dictionaries.
- [ ] Support named parameters from JSON/YAML with `--params-file`.
- [ ] Support `--max-rows` and `--statement-timeout-ms`.
- [ ] Reject empty SQL before calling the backend.
- [ ] Reject obvious write statements in `run-query`; users must use `execute-operation` for writes.
- [ ] Leave final SQL authorization and scope validation to the backend.

### 6. Documentation

- [ ] Add MetaTable CLI usage to `docs/cli/index.md`.
- [ ] Add MetaTable CLI usage to `docs/knowledge/cli.md`.
- [ ] Add CLI examples to `docs/tutorial/working_with_meta_tables.md`.
- [ ] Port the required examples from this ADR into maintained docs.
- [ ] Structure the tutorial according to the tutorial guidelines in this ADR.
- [ ] Include backend-managed registration examples using JSON/YAML payload files.
- [ ] Include governed query examples using `run-query` and `execute-operation`.
- [ ] Make any market-domain tutorial code application-owned instead of SDK-owned.
- [ ] Ensure docs do not reference deprecated row-table APIs.

### 7. Tests

- [ ] Add API wrapper tests proving every helper delegates through `MetaTable`.
- [ ] Add CLI tests for `meta-table list`.
- [ ] Add CLI tests for `meta-table detail`.
- [ ] Add CLI tests for `meta-table register --file`.
- [ ] Add CLI tests for `meta-table validate-contract --file`.
- [ ] Add CLI tests for `meta-table introspect`.
- [ ] Add CLI tests for `meta-table execute-operation --file`.
- [ ] Add CLI tests for `meta-table run-query`.
- [ ] Add CLI tests for label commands.
- [ ] Add CLI tests for sharing commands.
- [ ] Add CLI tests for typed delete verification.
- [ ] Add tests proving `META_TABLE_UID` is never coerced to an integer.

## Acceptance criteria

- `mainsequence meta-table --help` shows the new command group.
- `mainsequence meta-table list --show-filters` shows MetaTable filters from the client model.
- All MetaTable CLI resource arguments accept UUID-like UID strings.
- MetaTable CLI commands delegate through `mainsequence.client.models_metatables.MetaTable`.
- MetaTable governed SQL execution goes through `MetaTable.execute_operation(...)`.
- MetaTable registration and validation accept JSON and YAML payload files.
- MetaTable docs and tests do not reintroduce deprecated row-table APIs.
- The focused CLI and MetaTable test suite passes.
