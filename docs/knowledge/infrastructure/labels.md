# Labels

Some SDK objects expose a `labels` field together with the client `LabelableObjectMixin`.

Current examples include:

- `CodeRepository`

## What Labels Are For

Labels are organizational metadata only.

Use them to:

- group related objects
- annotate ownership or workflow state
- make browsing and manual discovery easier

## What Labels Do Not Do

Labels do not change:

- runtime behavior
- execution semantics
- storage identity
- hashing
- permissions
- scheduling
- functionality of the underlying object

They are helpers for humans, not runtime configuration.

## SDK Usage

Objects that inherit `LabelableObjectMixin` expose:

- `add_label(...)`
- `remove_label(...)`

Example:

```python
from mainsequence.client.models_foundry import CodeRepository

code_repository = CodeRepository.get_by_uid("<CODE_REPOSITORY_UID>")
code_repository.add_label(["rates", "research"])
code_repository.remove_label("archive")
```
