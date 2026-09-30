# Main Sequence Documentation

Main Sequence is a platform for building and operating CodeRepositories, jobs,
APIs, Agents, and reusable platform resources. The Python SDK provides the
authentication, client models, CLI workflows, Git-native source context,
observability, instrumentation, and scaffold utilities used by those workflows.

MetaTables is an independent domain package, installed as
`mainsequence-metatable` and imported as `metatables`. The SDK documentation
keeps the extraction history and migration boundary, while current table
implementation guidance belongs to that package.

## Choose A Reading Path

### Knowledge

Use the knowledge guides for platform concepts and end-to-end behavior:

- [Authentication](knowledge/infrastructure/auth.md)
- [Git source and Environment context](knowledge/infrastructure/context.md)
- [Users and Access](knowledge/infrastructure/users_and_access.md)
- [Constants and Secrets](knowledge/infrastructure/constants_and_secrets.md)
- [Scheduling Jobs](knowledge/infrastructure/scheduling_jobs.md)
- [Artifacts](knowledge/infrastructure/artifacts.md)
- [Resource releases](knowledge/infrastructure/resource_releases.md)
- [FastAPI request user context](knowledge/fastapi/index.md)
- [Server caller assertions](knowledge/server/caller_assertions.md)

### CLI

Use the [CLI overview](cli/index.md) for authentication, Agents,
CodeRepositories, jobs and runs, images, resources and releases, sharing,
scaffold maintenance, Docker, and local-development commands. The installed
`mainsequence --help` output is authoritative for the installed SDK version.

### Reference

Use the [generated API reference](reference/index.md) when you need exact Python
classes, methods, parameters, or return types.

### Architecture And Migrations

- [Architecture decision index](adr/index.md)
- [ADR 0034: Extract the MetaTables Python API](adr/0034-extract-metatables-python-package.md)
- [MetaTables SDK removal](migrations/metatables-sdk-removal.md)
- [Independent Git and Environment context](migrations/independent-context.md)
- [CodeRepository ontology hard cut](migrations/v8-code-repository-ontology.md)
- [Streamlit dashboard support removal](migrations/streamlit-dashboard-removal.md)

The beginner tutorial is maintained in its own self-contained CodeRepository.
This repository remains the source of truth for SDK APIs, CLI behavior,
architecture decisions, and generated reference documentation.
