# Labels

Some SDK objects expose a `labels` field together with the client `LabelableObjectMixin`.

Current examples include:

- `Project`
- `DataNodeStorage`
- `SimpleTableStorage`
- `command_center.Workspace`

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
from mainsequence.client.models_tdag import Project

project = Project.get(123)
project.add_label(["rates", "research"])
project.remove_label("legacy")
```

## CLI Usage

The CLI exposes the same verbs on the object groups that support labels:

```bash
mainsequence project add-label 123 --label rates --label research
mainsequence project remove-label 123 --label legacy

mainsequence data-node add-label 42 --label curated
mainsequence data-node remove-label 42 --label legacy

mainsequence simple_table add-label 41 --label reference-data
mainsequence simple_table remove-label 41 --label deprecated

mainsequence cc workspace add-label 7 --label trading --label desk
mainsequence cc workspace remove-label 7 --label old-layout
```

Each command calls the SDK model method for that object. The label mutation updates organizational metadata only.
