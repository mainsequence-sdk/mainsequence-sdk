# Architecture decisions

Decision records preserve the reasoning and status of SDK architecture changes.
A **Proposed** record does not describe implemented behavior. Read each record
for its status and any superseding decisions.

The implemented context change is [ADR 0035](0035-independent-git-source-and-platform-context.md), a partial successor
to [ADR 0031](0031-process-lifetime-code-repository-branch-context.md). The SDK
ownership boundary is recorded in [ADR 0034](0034-extract-metatables-python-package.md).

## Records

- [ADR 0002: Multidimensional DataNode Update Contract](0002-multidimensional-data-node-update-contract.md)
- [ADR 0016: DType Serialization and Parsing Contract](0016-dtype-serialization-and-parsing-contract.md)
- [ADR 0017: MetaTable Schema Graph Client API](0017-metatable-schema-graph-client.md)
- [ADR 0018: Platform-Managed MetaTable Runtime Physical Binding](0018-platform-managed-metatable-runtime-binding.md)
- [ADR 0019: Class-Based MetaTable Foreign Keys](0019-class-based-metatable-foreign-keys.md)
- [ADR 0020: Alembic-Based MetaTable Migrations](0020-metatable-migration-artifact-registry.md)
- [ADR 0021: Migration-First Platform-Managed MetaTables](0021-platform-managed-metatables-migration-first.md)
- [ADR 0022: Thin SDK Alembic-Owned MetaTable Migrations](0022-thin-sdk-alembic-owned-metatable-migrations.md)
- [ADR 0023: Alembic-Owned Foreign Keys And Indexes](0023-alembic-owned-foreign-keys-and-indexes.md)
- [ADR 0024: Typed Reserved MetaTable Collection Create](0024-typed-reserved-metatable-collection-create.md)
- [ADR 0025: Platform Time-Index Unique Grain Index](0025-platform-time-index-unique-grain-index.md)
- [ADR 0026: SDK-Owned Migration Scaffolding And Helpers](0026-sdk-owned-migration-scaffolding.md)
- [ADR 0027: Automatic DataNode Dependency Tree Self-Healing](0027-data-node-dependency-tree-refresh.md)
- [ADR 0028: MetaTable Storage Hash As Utility](0028-metatable-storage-hash-as-utility.md)
- [ADR 0029: A2A Message Send Only](0029-a2a-message-send-only.md)
- [ADR 0030: Server-Owned Dynamic Platform Skill Catalog](0030-server-owned-dynamic-platform-skill-catalog.md)
- [ADR 0031: Git-Native Process CodeRepositoryBranch Context](0031-process-lifetime-code-repository-branch-context.md)
- [ADR 0032: Read Backend Responses Tolerantly](0032-tolerant-response-reading.md)
- [ADR 0033: Read Closed Value Sets Tolerantly](0033-tolerant-value-set-reading.md)
- [ADR 0034: Extract the MetaTables Python API from `mainsequence`](0034-extract-metatables-python-package.md)
- [ADR 0035: Independent Git source and platform execution context](0035-independent-git-source-and-platform-context.md)

- [ADR-0036: Request-scoped logged user](0036-request-scoped-logged-user.md)

- [0036: Generic operation correlation in owner logs](0036-generic-operation-log-filter.md)

- [ADR 0037: The session lives in the operating system credential store](0037-machine-session-in-the-os-credential-store.md)

- [ADR 0038: Project MCP on FastAPI releases](0038-project-mcp-on-fastapi-releases.md)
