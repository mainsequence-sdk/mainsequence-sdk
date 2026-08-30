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
job_run = JobRun.get(pk="<JOB_RUN_UID>")
logs = job_run.get_logs(limit=100, severity="ERROR")
usage = job_run.get_resource_usage()

release = ResourceRelease.get(pk="<RESOURCE_RELEASE_UID>")
logs = release.get_logs(start=1787806800, end=1787810400)
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
enrichment remains available on each `OwnerLogRow`.

## Deployment Logs

`DeploymentRun.get_logs()` is deliberately separate from application runtime
logs. It returns deployment pipeline entries and sources, retaining its
`step_uid`, `source`, and `level` filters. The SDK adds the Organization
Environment resolved from the process-frozen Git code repository context. Callers do
not provide it.

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
