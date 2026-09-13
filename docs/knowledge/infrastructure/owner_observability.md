# Owner-Scoped Runtime Observability

Runtime observability starts from the Main Sequence object the user already
understands. SDK callers do not discover Knative services, revisions, pods,
namespaces, or provider resources.

## Python Client

Application-runtime owners expose backend-owned capability links through their
`observability` field. The SDK validates those links, uses the authenticated
backend session, and preserves the backend-derived Organization Environment
scope. Callers do not pass or override an Environment UID.

JobRun application logs are the deliberate exception to an environment-scoped
capability URL. The JobRun UID already fixes its persisted Environment, so
`JobRun.get_logs()` accepts a backend link with or without an
`organization_environment_uid` query parameter. A supplied value remains a
backend consistency check. Resource-usage links and every other owner log link
remain explicitly environment-scoped.

```python
from datetime import UTC, datetime

job_run = JobRun.get(pk="<JOB_RUN_UID>")
logs = job_run.get_logs(
    start_time="2026-09-13T10:00:00Z",
    end_time="2026-09-13T11:00:00Z",
    limit=100,
    level="error",
)
usage = job_run.get_resource_usage()

release = ResourceRelease.get(pk="<RESOURCE_RELEASE_UID>")
logs = release.get_logs(
    start_time=datetime(2026, 9, 13, 10, tzinfo=UTC),
    end_time=datetime(2026, 9, 13, 11, tzinfo=UTC),
)
usage = release.get_resource_usage(start=1787806800, end=1787810400)

agent = Agent.get(pk="<AGENT_UID>")
logs = agent.get_logs(agent_session_uid="<OPTIONAL_SESSION_UID>")
usage = agent.get_resource_usage()

session = AgentSession.get(pk="<AGENT_SESSION_UID>")
logs = session.get_logs()
```

`AgentSession.get_logs()` is fixed to the session in the owner path and does
not accept a session override. A null capability link means that observability
is not available for that owner or release kind.

Log cursors are opaque. Pass `next_cursor` back as `cursor` without parsing or
modifying it. Normalized log fields are typed, and additional backend
enrichment remains available on each `OwnerLogRow`. `occurred_at` and lowercase
`level` are the canonical response fields. The runtime query names `start`,
`end`, and `severity` remain accepted as deprecated aliases; do not combine an
alias with its canonical field.

## Environment-Scoped Collection Search

Use the class-level `search_logs()` clients to search across owners in one
product family. Every request, including a cursor-page request, requires an
explicit Organization Environment and a timezone-aware `[start_time, end_time)`
window:

```python
page = JobRun.search_logs(
    organization_environment_uid="<ORGANIZATION_ENVIRONMENT_UID>",
    start_time="2026-09-13T10:00:00Z",
    end_time="2026-09-13T11:00:00Z",
    level="error",
    job_uid="<OPTIONAL_JOB_UID>",
    limit=100,
)

if page.next_cursor:
    next_page = JobRun.search_logs(
        organization_environment_uid=page.organization_environment_uid,
        start_time=page.start_time,
        end_time=page.end_time,
        level="error",
        job_uid="<OPTIONAL_JOB_UID>",
        cursor=page.next_cursor,
        limit=page.limit,
    )
```

The five clients and their family selectors are:

| Client | Endpoint | Optional family selectors |
| --- | --- | --- |
| `DeploymentRun.search_logs()` | `/api/v1/deployment-runs/logs/` | `deployment_run_uid`, `target_type`, `target_uid`, `step_uid`, `source` |
| `JobRun.search_logs()` | `/api/v1/job-runs/logs/` | `job_run_uid`, `job_uid` |
| `ResourceRelease.search_logs()` | `/api/v1/resource-releases/logs/` | `resource_release_uid` |
| `Agent.search_logs()` | `/api/v1/agents/logs/` | `agent_uid`, `agent_session_uid` |
| `AgentSession.search_logs()` | `/api/v1/agent-sessions/logs/` | `agent_session_uid`, `agent_uid` |

All five accept `cursor`, `limit`, `level`, and `event`. Runtime-family clients
also accept `request_id` and `outcome`. Collection rows always identify their
owner with `owner_type` and `owner_uid`; family-safe additive fields are
preserved on `EnvironmentLogSearchRow`. A non-null `truncation_reason` explains
whether the page, result-chain, or examined-candidate limit stopped the query.

## Deployment Logs

`DeploymentRun.get_logs()` is deliberately separate from application runtime
logs. It returns deployment pipeline entries and sources, accepting
`start_time`, `end_time`, `step_uid`, `source`, `level`, and `event`. The
DeploymentRun path fixes the owner and the backend derives its persisted
Environment. `organization_environment_uid` is optional and, when supplied,
acts only as a consistency check.

## CLI

The corresponding commands are:

```bash
mainsequence code-repository jobs runs logs <JOB_RUN_UID>
mainsequence code-repository jobs runs resource-usage <JOB_RUN_UID>
mainsequence code-repository resources logs <RESOURCE_RELEASE_UID>
mainsequence code-repository resources resource-usage <RESOURCE_RELEASE_UID>
mainsequence agent logs <AGENT_UID>
mainsequence agent resource-usage <AGENT_UID>
mainsequence agent session logs <AGENT_SESSION_UID>
```

Log commands accept bounded time-window and normalized filters such as
`--start`, `--end`, `--limit`, `--severity`, `--request-id`, `--event`, and
`--outcome`. Agent logs additionally accept `--agent-session-uid`.
