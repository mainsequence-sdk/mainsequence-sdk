# Recorded model conversations

`InferenceClient` calls Main Sequence's recorded inference API using the SDK's
existing authentication. Provider keys stay on the server. No Agent or
AgentSession is required. Configure a provider/model and its stored credentials
through the existing provider control plane first.

```python
from uuid import uuid4
from mainsequence.client import InferenceClient

client = InferenceClient(organization_environment_uid="<authorized-environment-uuid>")
key = str(uuid4())  # Persist this with the job/document before sending.
result = client.complete(
    provider="openai",
    model="gpt-5.4",  # Select a model available in your catalog.
    messages=[{"role": "user", "content": "Extract fund weights from: Fund A 60%, Fund B 40%."}],
    thinking_level="high",
    provider_options={"store": False, "text": {"verbosity": "low"}},
    response_schema={
        "type": "object",
        "properties": {
            "allocations": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {"fund": {"type": "string"}, "weight": {"type": "number"}},
                    "required": ["fund", "weight"],
                    "additionalProperties": False,
                },
            },
        },
        "required": ["allocations"],
        "additionalProperties": False,
    },
    idempotency_key=key,
)
print(result.output)
print(client.history(result.conversation_uid))
print(client.insights(result.conversation_uid))
```

Human credentials select an authorized Environment explicitly. Runtime credentials
can use `InferenceClient()` and derive their exact Environment on the server;
a contradictory selector is rejected. Provider credentials belong to the
authenticated responsible User, or the selected Organization custom provider.

Every request supplies complete ordered messages (`system`, `user`, `assistant`,
with text content). Set `conversation_uid` to group another request into that
conversation. Earlier history is never automatically prepended. Main Sequence
records each request/result and keeps content until explicitly deleted.

## Provider controls

All four families use the same envelope:

| Family | Execution and native options |
| --- | --- |
| OpenAI | Responses/Chat Completions as selected by the catalog; `reasoning.effort` or `reasoning_effort`, `store`, `text` and other native body options |
| Anthropic | Messages; native `thinking`, `output_config`, `max_tokens`; common thinking levels require a supported model mapping |
| Ollama | An Organization custom provider using its OpenAI-compatible API and native fields supported by that API |
| OpenRouter | Chat Completions; nested `provider_options.provider` controls upstream routing and is distinct from the top-level catalog selection |

`thinking_level` is optional. Omission preserves supplied native thinking fields
or the provider default. `provider_options` accepts arbitrary JSON body fields,
preserving objects, arrays, numbers, false and null. It cannot override model,
messages, credentials, endpoint, streaming, tool execution or provider continuation.
Explicit common/native collisions are rejected; disjoint nested fields merge.
Do not provide both common thinking selection and native thinking selection.

Provider storage controls never disable Main Sequence history. For OpenAI,
`provider_storage={"store_response": False}` compiles to `store: false`; use this
or native `provider_options.store`, not both. Anthropic uses its stateless
Messages contract without an invented `store` field. Ollama and OpenRouter do
not have a universally verifiable per-request storage switch; use documented
native/account controls. Strict `require_zero_data_retention=True` currently
rejects before transmission when account eligibility cannot be verified.

## Results, replay and inspection

`InferenceResult` includes conversation/request UIDs, sequence, selected model,
outcome, usage completeness, retained input/output and effective controls. A
validated `response_schema` produces parsed JSON in `output["content"]`.

The idempotency key is required (1–128 letters, digits, `.`, `_`, `:`, `-`). Reuse
exactly the same key and input after an uncertain outcome. Changing the input
with the same key is a conflict. Replaying a pending request returns its current
state; replaying a completed request returns its stored result. The SDK does
not automatically retry inference network failures or generate replacement keys.

- `InferenceExecutionError` contains the recorded `result` and `status_code`.
  Inspect `result.request_uid`; a timeout or invalid structured output is not success.
- `InferenceTransportError` retains `idempotency_key`: a transport error or
  malformed success leaves the outcome uncertain. Inspect history or replay the
  identical request/key. Never retry with a newly generated key.
- Validation and authorization errors use the SDK's existing HTTP error handling.

The SDK permits one authentication refresh following an explicit 401, preserving
body and key. Missing usage is unknown, not evidence of a free call.

```python
client.conversations(limit=25, offset=0)
client.conversation(result.conversation_uid)
client.request(result.request_uid)
client.history(result.conversation_uid, limit=25, offset=0)
client.insights(result.conversation_uid)
client.delete_content(result.conversation_uid)
```

The owner and authorized Organization administrators can inspect/delete content.
Only the owner can append. Deletion removes protected content while retaining
non-content outcomes, usage and idempotency identity. A replay of deleted content
returns a recorded 410 error and never executes the model again. Content deleted
during an active call is not recreated by the eventual provider response.
