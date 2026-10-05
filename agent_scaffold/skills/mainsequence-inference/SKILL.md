---
name: mainsequence-inference
description: Use the Main Sequence Python SDK for direct recorded model completions and structured extraction, with thinking levels, provider-native options, conversation history and safe replay. Use for model calls that do not need an Agent or A2A session.
---

# Main Sequence Recorded Inference

Use `mainsequence.client.InferenceClient` to call the recorded inference endpoint.
Main Sequence authenticates the caller, uses existing stored provider credentials,
and records input/output. No Agent, AgentSession, Tau runtime or application-side
provider key is required. This skill teaches the SDK client; the platform owns
provider availability, permissions and execution behavior.

## Establish the client and model

Use the installed `mainsequence.client.inference` implementation and its matching
[inference documentation](https://mainsequence-sdk.github.io/mainsequence-sdk/knowledge/infrastructure/inference/).
If the installed SDK lacks `InferenceClient`, report the missing capability. Do
not silently upgrade the SDK, restore retired `Agent.respond` calls, or replace
recorded inference with direct provider HTTP calls.

Reuse existing Main Sequence authentication and the current provider catalog.
Select an exact available provider/model with configured credentials; examples
below illustrate request shapes, not a guarantee that an account can use a model.
There is no inference CLI command; use the Python client.

For local CodeRepository code using human credentials, resolve the Environment
from the current Git checkout through the existing SDK context:

```python
from mainsequence.client import InferenceClient
from mainsequence.code_repository_context import get_code_repository_context

context = get_code_repository_context()
if context.organization_environment_uid is None:
    raise RuntimeError("This checkout has no resolved Organization Environment")
client = InferenceClient(
    organization_environment_uid=context.organization_environment_uid,
)
```

For an authenticated deployed runtime, use `InferenceClient()`; the server derives
its exact Environment. A contradictory selector fails. Outside a CodeRepository,
human callers provide an authorized Environment UID from their application
context. Never invent an Environment, use another branch as a fallback, or add
inference environment variables. Provider secrets remain in existing server-side
credential storage and must not appear in messages or `provider_options`.

## Select a configured provider

Pass optional `custom_id="openai-work"` to `client.complete()` to select that named
credential within the caller's Environment. Omit it to retain the first available
owned credential, then the first shared credential. A missing or ambiguous explicit
name fails; do not retry using a different credential. Recorded requests retain the
exact credential UID for replay. Custom endpoints use their existing provider
identifier and do not accept `custom_id`.

Sharing a configured provider authorizes recipients, including workload users,
to use its credentials and receive them in local Tau. They may copy those credentials;
revoking a share cannot remove copies already delivered. Explain this before
guiding someone through sharing. This inference client executes on the server;
provider management remains with the existing provider clients.

## Construct a completion envelope

Every call supplies complete ordered text messages: `system`, `user`, `assistant`.
Earlier requests are never automatically prepended. An omitted `conversation_uid`
creates a conversation; an existing UID groups another independent request with
its own full context. Each new logical call needs its own idempotency key.

```python
def extract_weight(client, document, *, idempotency_key, conversation_uid=None):
    return client.complete(
        provider="openai",
        model="gpt-5.4",  # Replace with the exact available catalog selection.
        messages=[
            {"role": "system", "content": "Extract the fund weight as a fraction."},
            {"role": "user", "content": document},
        ],
        thinking_level="high",
        provider_options={"store": False, "text": {"verbosity": "low"}},
        response_schema={
            "type": "object",
            "properties": {"weight": {"type": "number"}},
            "required": ["weight"],
            "additionalProperties": False,
        },
        metadata={"operation": "fund-weight-extraction"},
        conversation_uid=conversation_uid,
        idempotency_key=idempotency_key,
    )
```

`response_schema`, `thinking_level`, `provider_options`, `provider_storage`,
`metadata` and `conversation_uid` are optional. Metadata is recorded content,
not authorization or a credential selector. Keep sensitive payloads out of logs.
The current contract accepts bounded text/JSON; it does not execute tools, stream,
fetch document URLs, or accept file/image attachments. Extract document text first.

Only consume `result.output["content"]` when `result.status == "completed"`,
`result.content_available` is true, and `result.output` is present.
A validated common `response_schema` returns parsed JSON there; ordinary text
completions return a string. A replay can return a pending result without output.
Preserve `conversation_uid` and `request_uid` from `InferenceResult` for inspection.

## Thinking and native provider options

`thinking_level` accepts the catalog vocabulary `off`, `minimal`, `low`, `medium`,
`high`, `xhigh`, but the exact model and adapter must support the requested level.
Unsupported mappings fail; do not silently lower the level to make a call work.
Omitting the field preserves native thinking options, or the provider default
when none are supplied. `off` is an explicit selection, not omission.

Use `provider_options` for native JSON body fields the caller knows. Objects,
arrays, numbers, false and null are preserved. Disjoint nested fields merge;
colliding explicit common/native controls fail even when their values agree.
Do not combine `thinking_level` with native thinking selection or
`provider_storage.store_response` with native `store`. Omit the corresponding
common convenience when choosing native configuration.

| Family | Native `provider_options` example | Meaning |
| --- | --- | --- |
| OpenAI Responses | `{"reasoning": {"effort": "high"}, "store": false, "text": {"verbosity": "low"}}` | Native reasoning and storage; Chat Completions uses `reasoning_effort` instead. The catalog chooses the API. |
| Anthropic Messages | `{"thinking": {"type": "adaptive"}, "output_config": {"effort": "high"}, "max_tokens": 4096}` | For an adaptive model; older models can use their native thinking budget. Do not send an invented OpenAI `store` flag. |
| Ollama compatible API | `{"reasoning_effort": "high"}` | Select the existing Organization custom-provider identifier and a compatible model/API. Native Ollama `/api/chat` fields are not automatically translated. |
| OpenRouter Chat | `{"reasoning": {"effort": "high"}, "provider": {"require_parameters": true, "zdr": true}}` | The nested `provider` object is upstream routing, distinct from the top-level catalog selector. ZDR routing is not proof of account-wide zero retention. |

These are independent native-only examples: omit `thinking_level` when using
them. Main Sequence forwards accepted native fields without inventing a normalized
thinking level or claiming a provider honored an unknown option. Native fields
cannot override the canonical model/input, credentials, endpoint, headers,
streaming/background lifecycle, provider continuation, tool execution or alternate
model selection. Common OpenRouter thinking/schema controls already add
`provider.require_parameters`; do not also set that path in native options.

## Provider storage versus Main Sequence history

**Main Sequence keeps every admitted conversation exchange until explicitly
deleted.** Provider storage controls never disable this history. Do not add an
expiry setting, capture opt-in, `record_history=False`, or a new policy registry.

- OpenAI: `provider_storage={"store_response": False}` compiles to `store: false`.
  Choose this convenience or native `provider_options={"store": False}`.
- Anthropic: `store_response=False` means the stateless Messages contract. It
  does not promise that operational logs or caches do not exist.
- Ollama and OpenRouter: the common storage convenience is not supported as a
  universal per-request guarantee. Use supported native/account controls; do not
  add or drop flags silently. Ollama is configured through existing custom
  providers; `keep_alive` governs model residency, not conversation retention.
- `provider_storage={"require_zero_data_retention": True}` currently rejects
  before dispatch because verified account eligibility is unavailable. Neither
  OpenAI `store: false` nor OpenRouter `provider.zdr` establishes that guarantee.

## Handle outcomes and replay

Create and persist the key with the application's logical request **before** its
first submission. Keys contain 1–128 letters, digits, `.`, `_`, `:` or `-`. Do not
put key generation inside a retry loop. Preserve the exact envelope for replay,
including conversation creation/attachment, model, thinking, native options,
metadata and schema. Reuse the same User, Environment and authenticated workload;
idempotency is scoped to that context, not global across unrelated JobRuns.

- `InferenceExecutionError` exposes `result` and `status_code`. Retain the
  returned IDs and inspect `client.request(error.result.request_uid)`. Invalid
  structured output, provider failure and unknown outcomes are not successes.
- `InferenceTransportError` retains `idempotency_key`; the outcome may already
  be recorded or executing. Inspect known request/history IDs, or explicitly
  replay the identical envelope and key. Never substitute a new key after an
  ambiguous result. Lower-level HTTP failures also do not justify a fresh key.
- `admitted`/`dispatching` means pending; `completed` means persisted success;
  `failed`/`unknown` requires inspection. Replaying unknown work does not execute
  again. A changed envelope with the same key conflicts (409); deleted content
  yields a recorded 410. Do not hide these cases behind an automatic retry loop.

The client does not automatically retry network failures. It permits one refresh
following an explicit authentication 401, preserving body and key. A longer
client `timeout` does not extend the server's execution deadline. Neither a
transport timeout nor `unknown` proves the provider stopped processing.
Missing usage remains unknown; inspect `usage["complete"]` where present instead
of treating absent token counts as a free call. Native-only structured output
is not automatically validated by the common `response_schema` validator.

## Read history and delete only when requested

```python
conversations = client.conversations(limit=25, offset=0)
conversation = client.conversation(result.conversation_uid)
request = client.request(result.request_uid)
history = client.history(result.conversation_uid, limit=25, offset=0)
insights = client.insights(result.conversation_uid)
```

Lists/history return `count`, `next`, `previous`, `results`; request retrieval
returns `InferenceResult`. History records each complete submitted context and its
output in admission order, not model-managed memory. Insights summarize outcomes,
submitted messages and known tokens with unknown-usage counts. Read methods never
invoke the model. The owner and authorized same-Organization administrators can
inspect content within the admitted Environment; only the owner can append.

For an explicit content-deletion request, use
`client.delete_content(conversation_uid)`. It removes protected inputs, outputs,
schemas and options, while keeping request identity and non-content outcome/usage
records. Replay never reconstructs deleted content or runs the provider again,
even if a response arrives after deletion. Do not schedule automatic deletion.

## Verify an integration

Mock the provider-facing boundary or SDK transport for local tests. Check the
chosen thinking/native envelope, parsed structured result, stable retry identity,
pending/error handling and history retrieval. Make a live provider call only
when the task authorizes it and the existing account is ready; a successful call
does not verify the provider's retention practices. Report mocked versus live
verification clearly.
