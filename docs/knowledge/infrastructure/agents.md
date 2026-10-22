# Agents

An **Agent** is a Harness Agent that people talk to and give Tasks to; the SDK
model is `Agent`. A repository branch declares it in its workflow file, and the
Agent runs as its own workload from its runtime release, a `ResourceRelease`.

Who can use an Agent is decided by its policies. This page says what each
policy allows; [Who Has Access to an Object](object_access.md) describes the
access methods and their models. Organization admins hold every action.

## Who can use an Agent

An Agent's access is decided by its policies:

- `viewer`: talk to the Agent and give it Tasks, see every Task of the Agent
  with its requests and results, and read your own log lines;
- `user`: talk to the Agent and give it Tasks, seeing only the Tasks you asked
  for;
- `operator`: also stop any Task or session run of the Agent, other people's
  included;
- `editor`: also change the Agent's model defaults and labels, delete it,
  manage what its workload can reach, and see who has access;
- `owner`: also decide who has access.

`user` is the right policy for most people. The Agent works with its own
access, which can be broader than yours, unless it acts for the person asking.
On an Agent that acts for the person asking, a Task is visible only to the
people who asked for it and to Organization admins, whatever the policies. A
session stays its owner's: no policy reads other people's conversations.

The Agent's creator holds `owner`. Repository workflows create Agents as the
Organization's automation identity, which holds no policy on them, and a policy
on the Agent's branch or repository gives nothing on the Agent. Turning on
acting for the requester is in no policy: only Organization admins do it, so
`explain_access("agents.set_acts_for_requester")` names no policy that gives
it.

Read and change the policies with the access methods on `Agent`:

```python
from mainsequence.client.agent_runtime_models import Agent

agent = Agent.get_by_uid(agent_uid)

for binding in agent.get_access().bindings:
    print(binding.uid, binding.policy.id, binding.principal.type, binding.principal.uid)

binding = agent.attach_policy("user", principal_type="team", principal_uid=team_uid)
agent.detach_policy(binding.uid)

answer = agent.explain_access("agents.read_tasks", principal_uid=user_uid)
print(answer.allowed, answer.granting_policies)
```

The access methods address the Agent by its UID alone and take no
Environment; with a workload's runtime credentials they reach only the Agents
of that workload's Environment. A refused change raises
`PermissionDeniedError`, whose `payload` names the `required_action` and the
`granting_policies`. A repository workflow file's `access` entry for an Agent
gives its workload the policy it names, such as `policy: user`.
`mainsequence access policies --type agents` lists the policies with their
actions.

## Workflow-owned deployments

Agent image builds and deployments follow the normal repository workflow,
including its promotion rules and prerequisite steps. Agent policies offer no
manual runtime-update command. Discovery returns Agent identity, branch,
Environment and A2A profile without a `runtime_update` remediation object.
A newer branch commit awaiting a tag or prerequisite does not itself prevent
communication with the already deployed image; follow session runtime admission.

## Its runtime release and backing Job

An Agent's policies also decide its runtime release and that release's
backing Job: `viewer` and `operator` on the Agent give `viewer` on the release,
`editor` and `owner` give `editor`, and `user` gives nothing there. Access to
the branch does not reach that release, and the release takes no policy of its
own: `ResourceRelease.attach_policy()` on it raises `BadRequestError` whose
`payload` has `code` `sharing_managed_by_owner`. Give access on the Agent
instead. See
[Who can use a release](resource_releases.md#who-can-use-a-release).

## From the command line

These commands read and change an Agent's policies:

```bash
mainsequence agent access list <AGENT_UID>
mainsequence agent access grant <AGENT_UID> user --user <USER_UID>
mainsequence agent access grant <AGENT_UID> viewer --team <TEAM_UID>
mainsequence agent access revoke <AGENT_UID> <BINDING_UID>
mainsequence agent access explain <AGENT_UID> agents.use
```

`<USER_UID>` is the public UUID of the person or workload identity receiving
the policy, `<TEAM_UID>` the team's, and `<BINDING_UID>` the UID of a binding,
which `access list` shows. The commands address the Agent by its UID alone and
need no registered branch. A refused command prints the platform's message and
the policies that would allow it. See [Agents](../../cli/index.md#agents) in
the CLI reference.
