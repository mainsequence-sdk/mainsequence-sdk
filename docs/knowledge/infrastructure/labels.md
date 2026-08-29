# Labels

Some SDK objects expose a `labels` field together with the client `LabelableObjectMixin`.

Current examples include:

- `CodeRepository`
- `TimeIndexMetaTable`
- `MetaTable`

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

## CLI Usage

The CLI exposes the same verbs on the object groups that support labels:

```bash
mainsequence code-repository add-label <CODE_REPOSITORY_UID> --label rates --label research
mainsequence code-repository remove-label <CODE_REPOSITORY_UID> --label archive

mainsequence time-index-table add-label <TIME_INDEX_META_TABLE_UID> --label curated
mainsequence time-index-table remove-label <TIME_INDEX_META_TABLE_UID> --label archive
```

Each command calls the SDK model method for that object. The label mutation updates organizational metadata only.
