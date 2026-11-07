# Caller context

The [caller assertion](caller_assertions.md) tells an application **who is
calling**. Caller context tells it **who the call is for**.

A service user, such as an
[external workload](../infrastructure/users_and_access.md#external-workloads)
that serves its own users, can send its own context with a call to Main
Sequence: which of its users the call is for, and its own information about the
call. It sends that context in the W3C [`baggage`](https://www.w3.org/TR/baggage/)
header. The W3C [`traceparent`](https://www.w3.org/TR/trace-context/) travels
with it and ties the work to one trace; the context itself is the baggage.

Main Sequence carries the context to the applications that run on it (FastAPI
releases, Job runs and Agents) and records it as the caller's claim. It never
uses it to authenticate, authorize, admit or bill a call, defines no keys, and
builds no feature on it. Applications use it to make their own rules and
filters, for example to return only the data of the person a call is for.

## Keys

Each member of the header is one key and its value:

```text
baggage: mainsequence.<application>.<name>=<value>,mainsequence.<application>.<name>=<value>
```

- Only keys of the form `mainsequence.<application>.<name>` are recorded and
  carried into the work a call creates, such as Job runs and Agent Tasks. Other
  keys stop there, and W3C properties (anything after a member's `;`) are
  dropped. Values are kept as received. Give every key that an application on
  Main Sequence must receive this form.
- The application chooses its segment and its names, with no registry and no
  approval. Main Sequence gives no key a meaning and treats every key the same
  way. Two applications that choose the same segment collide only on calls that
  cross both, and a collision grants nothing in Main Sequence.
- Values are opaque strings. Encode a `,`, `;`, `=` or space inside a value
  with percent-encoding, as W3C Baggage requires; OpenTelemetry's propagators
  do it for you.
- **Values are public by contract.** Baggage is passed on downstream, to third
  parties too, and Main Sequence records it. Never put a secret, a credential
  or a proof in it. A value that refers to a person is an opaque, stable id
  that means nothing without your own records: never an email, a name, a phone
  number or a government id.

## Trust

Anyone who can call an application can send it any context. Context is a
claim, and it is worth exactly as much as the application's trust in the caller
that delivered it. Main Sequence never signs it and never checks a value.

- **Believe context only from callers you trust to vouch for it**, checked
  against the verified caller at the hop where you read it. At a FastAPI
  release, that caller is `User.get_logged_user()`.
- **From any other caller, ignore it**, or treat it as that caller's own
  unverified claim.
- **Trust is per hop.** An application that receives a call from another
  application trusts that application to have checked the original caller. It
  does not see, and does not need, the original caller's identity.
- **Whoever may run a Job may set its runs' context.** A run carries the
  context of the request that created it, so a Job whose code makes rules from
  that context gives run permission only to service users it trusts; see
  [Who can use a Job](../infrastructure/scheduling_jobs.md#who-can-use-a-job).

What Main Sequence guarantees:

- at a release or an Agent, the context arrives next to the authenticated
  caller, as received;
- the context of a Job run or an Agent Task is exactly the context of the
  request that created it, never merged with another request's and never
  changed afterwards;
- no Main Sequence decision reads it.

It does not guarantee that any value is true.

Caller context is not a requester. It names no Main Sequence User and grants
nothing; an application that acts for a Main Sequence person does so only
through [requester-bound access](../infrastructure/users_and_access.md#applications-that-act-for-their-requester).

## Where context arrives

| Where the application runs | What it receives |
| --- | --- |
| A FastAPI release | The request's `baggage` and `traceparent` headers, on every request, next to the caller assertion. Never as environment variables: each request brings its own. |
| A Job run | `BAGGAGE` and `TRACEPARENT` environment variables, in W3C header format, when the run was created by a request that carried context. |
| An Agent Task | `baggage` and `traceparent` headers on the Task's dispatch signal. |

Work keeps the context of the request that created it: a Job run or an Agent
Task started by a call with context carries that context. A Job, its schedule,
a release or an Agent never stores one, so a scheduled run, or anything the
platform starts on its own, has no context.

A call a Job run makes to Main Sequence with its run credentials and no
`baggage` header is recorded with the run's own context, so the work it creates
inherits that context even when the run's code does not pass it on. A header,
when the run sends one, wins.

## Missing context

**Missing context means no external user.** Fail closed: refuse what needs a
person, and never fall back to broader access. Context can be missing because
the caller sent none, because it was malformed or too large, or because an
older SDK on the way did not pass it on. Context from a caller you do not trust
counts as missing too.

## Limits

The limits are those of OpenTelemetry's W3C baggage parser:

- a header over 8192 bytes is ignored whole;
- a member over 4096 bytes is skipped;
- at most 180 members are read.

A malformed member is skipped. Bad baggage never fails a request: what cannot
be read counts as missing. Keep context to a few short keys.

## With the SDK

The SDK gives no key a meaning and adds no accessor of its own: application
code reads the context with OpenTelemetry's standard API,
[`opentelemetry.baggage`](https://opentelemetry-python.readthedocs.io/en/latest/api/baggage.html).

- **FastAPI releases.** `install_request_identity(app)` makes each request's
  `baggage` and `traceparent` the OpenTelemetry context of that request, next
  to the caller it verifies. See
  [Caller context](../fastapi/index.md#caller-context) on the FastAPI page.
- **Job runs.** `import mainsequence` restores `TRACEPARENT` and `BAGGAGE` as
  the process's OpenTelemetry context. See
  [Caller context in a run](../infrastructure/scheduling_jobs.md#caller-context-in-a-run).
- **Calls the SDK makes**, to Main Sequence and to Agents, carry the current
  `traceparent` and `baggage`. A Job run or an Agent
  Task that a release starts while it handles a request therefore receives that
  request's context. A header you set yourself wins, and without a current
  context nothing is added.
- **Log lines** written through the SDK's logger carry the current context's
  `mainsequence.` keys as a `baggage` field.
- **Agents** built with the Agents SDK receive a Task's context on its dispatch
  signal; the Agents SDK documentation describes how their code reads it.

## Example

Advisor is an invented application: a service outside Main Sequence, connected
as the external workload `advisor-backend`, that calls the FastAPI release
`portfolio-api` for its own users. It chooses two keys:

- `mainsequence.advisor.person`: Advisor's opaque id for the person;
- `mainsequence.advisor.request`: Advisor's id for the request.

**1. `advisor-backend` sends its context** with each call it makes as its
external workload, signed in with
[workload identity](../infrastructure/auth.md#workload-identity-auth):

```text
traceparent: 00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01
baggage: mainsequence.advisor.person=p-7f3a9c,mainsequence.advisor.request=r-0042
```

With OpenTelemetry, the service sets the keys on its context and its
propagator writes the headers:

```python
from opentelemetry import baggage, propagate

context = baggage.set_baggage("mainsequence.advisor.person", "p-7f3a9c")
context = baggage.set_baggage("mainsequence.advisor.request", "r-0042", context=context)
headers = {}
propagate.inject(headers, context=context)  # W3C baggage, and traceparent when tracing
```

**2. `portfolio-api` checks its caller, then reads the person.** It believes
an Advisor person only when the verified caller is `advisor-backend`'s workload
User, whose UID it read from a call that workload made:

```python
from fastapi import FastAPI, HTTPException
from opentelemetry import baggage

from mainsequence.client import User
from mainsequence.server.fastapi import install_request_identity

ADVISOR_BACKEND = "<workload User UID of advisor-backend>"

app = FastAPI()
install_request_identity(app)


def advisor_person() -> str | None:
    """The Advisor person this call is for, or None."""
    if User.get_logged_user().uid != ADVISOR_BACKEND:
        return None  # only advisor-backend vouches for Advisor people
    return baggage.get_baggage("mainsequence.advisor.person")


@app.get("/portfolio")
def portfolio():
    person = advisor_person()
    if person is None:
        raise HTTPException(status_code=403, detail="This operation needs an Advisor person.")
    return load_portfolio(person)  # that person's data, and nothing else
```

A call from any other caller gets no Advisor person, whatever its baggage says.
A call from `advisor-backend` without context gets none either.

**3. The work it starts keeps the context.** While it handles the request,
`portfolio-api` runs the Job `portfolio-rebalance` with `job.run_job()`. The
SDK passes the request's context on, so the run starts with `BAGGAGE` and
`TRACEPARENT`:

```python
import mainsequence  # restores the run's TRACEPARENT and BAGGAGE
from opentelemetry import baggage

person = baggage.get_baggage("mainsequence.advisor.person")
if person is None:
    raise SystemExit("This run needs an Advisor person.")  # a scheduled run has none
```

Run permission on `portfolio-rebalance` is given to `portfolio-api`'s workload
and to no other service user, so the run can trust its context.

**4. Main Sequence records the context as a claim** on the work it carried, and
no Main Sequence decision reads it.
