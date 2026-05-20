# ADR 0004: Move Client Market Models To `mainsequence.client.markets.models`

Date: 2026-05-20

Status: Proposed

## Context

The SDK currently keeps the market and virtual account management models in:

```text
mainsequence/client/models_vam.py
```

That module contains market-facing models such as assets, asset categories,
translation tables, portfolios, accounts, execution venues, trades, virtual
funds, and orders. The module name is no longer a good fit for the public API
surface, and it sits directly under `mainsequence.client` instead of a market
specific package.

The intended implementation file is:

```text
mainsequence/client/markets/models/core.py
```

The intended canonical import path is:

```python
from mainsequence.client.markets.models import Asset, Portfolio
```

Existing imports from `mainsequence.client.models_vam` must keep working during
the transition.

## Decision

Move the implementation from:

```text
mainsequence/client/models_vam.py
```

to:

```text
mainsequence/client/markets/models/core.py
```

Add package initializers:

```text
mainsequence/client/markets/__init__.py
mainsequence/client/markets/models/__init__.py
```

The canonical import path for these models is:

```python
from mainsequence.client.markets.models import Asset
from mainsequence.client.markets.models import Portfolio
from mainsequence.client.markets.models import AssetTranslationTable
```

`mainsequence.client.markets.models.core` is the implementation module behind
the package. It is not the canonical import path for SDK users or internal SDK
code. `mainsequence.client.markets.models.__init__` must re-export the public
model symbols from `core.py`.

The moved implementation must use absolute package imports only. Do not use
relative imports in the moved module.

Use:

```python
from mainsequence.client.base import (
    BaseObjectOrm,
    BasePydanticModel,
    HtmlSaveException,
)
from mainsequence.client.exceptions import raise_for_response
from mainsequence.client.models_tdag import DataNodeUpdate
from mainsequence.client.utils import (
    DATE_FORMAT,
    MAINSEQUENCE_ENDPOINT,
    DoesNotExist,
    make_request,
)
from mainsequence.client.utils import MARKETS_CONSTANTS as CONSTANTS
```

Do not use:

```python
from ...base import BaseObjectOrm, BasePydanticModel, HtmlSaveException
from ...exceptions import raise_for_response
from ...models_tdag import DataNodeUpdate
from ...utils import DATE_FORMAT, MAINSEQUENCE_ENDPOINT, DoesNotExist, make_request
from ...utils import MARKETS_CONSTANTS as CONSTANTS
```

Keep `mainsequence/client/models_vam.py` as a backwards-compatible shim. The
shim must log a deprecation warning and point users to the new import path:

```python
from mainsequence.logconf import logger

logger.warning(
    "mainsequence.client.models_vam is deprecated and will be removed in a "
    "future release. Use mainsequence.client.markets.models instead, "
    "for example: from mainsequence.client.markets.models import Asset, "
    "Portfolio."
)

from mainsequence.client.markets.models import *  # noqa: F401,F403
```

`mainsequence.client` may continue to re-export these classes for user
convenience, but it must import them from the new canonical module:

```python
from mainsequence.client.markets.models import *
```

This keeps:

```python
from mainsequence.client import Asset, Portfolio
```

working without importing the deprecated shim.

## Non-Goals

This refactor must not:

- rename market model classes
- change model fields or validation behavior
- change endpoint paths or request behavior
- change portfolio, account, order, virtual fund, or asset semantics
- remove `mainsequence.client.models_vam` during the first migration
- introduce relative imports into the moved module
- redesign the broader `mainsequence.client` package layout

## Implementation Tasks

### Task 1: Create Target Package

Create:

```text
mainsequence/client/markets/
mainsequence/client/markets/__init__.py
mainsequence/client/markets/models/
mainsequence/client/markets/models/__init__.py
```

Keep package initializers small. `mainsequence.client.markets.models` is the
canonical import package and must re-export public model symbols from
`mainsequence.client.markets.models.core`.

### Task 2: Move The Implementation

Move:

```text
mainsequence/client/models_vam.py
```

to:

```text
mainsequence/client/markets/models/core.py
```

Preserve model behavior and public class names. This should be a mechanical move
except for import path corrections.

### Task 3: Convert Imports In The Moved Module To Absolute Paths

Replace relative imports in `mainsequence.client.markets.models.core` with full
package imports.

Required imports include:

```python
from mainsequence.client.base import (
    BaseObjectOrm,
    BasePydanticModel,
    HtmlSaveException,
)
from mainsequence.client.exceptions import raise_for_response
from mainsequence.client.models_tdag import DataNodeUpdate
from mainsequence.client.utils import (
    DATE_FORMAT,
    MAINSEQUENCE_ENDPOINT,
    DoesNotExist,
    make_request,
)
from mainsequence.client.utils import MARKETS_CONSTANTS as CONSTANTS
```

Run a search inside the moved module:

```bash
rg "from \\.|import \\." mainsequence/client/markets/models/core.py
```

There should be no relative imports left in this module.

### Task 4: Add Backwards-Compatible Shim

Replace the old `mainsequence/client/models_vam.py` implementation with a thin
shim that:

- logs a deprecation warning through `mainsequence.logconf.logger`
- names the deprecated path, `mainsequence.client.models_vam`
- names the canonical path, `mainsequence.client.markets.models`
- includes a copyable example import
- re-exports symbols from `mainsequence.client.markets.models`

The shim should not contain model implementation logic.

### Task 5: Preserve Public Re-Exports

Update `mainsequence/client/__init__.py` so `mainsequence.client` imports market
models from the canonical module:

```python
from mainsequence.client.markets.models import *
```

It should not import from:

```python
from mainsequence.client.models_vam import *
```

This avoids emitting deprecation logs for users who import directly from
`mainsequence.client`.

### Task 6: Update Documentation And Reference Pages

Update documentation that names the old module path. At minimum, check:

```bash
rg "mainsequence\\.client\\.models_vam|models_vam" docs examples README.md CHANGELOG.md
```

Add reference documentation for:

```text
mainsequence.client.markets.models
```

Keep any `models_vam` documentation as migration documentation only, and make it
clear that the old path is deprecated.

### Task 7: Add Compatibility Tests

Add tests that verify:

- `from mainsequence.client.markets.models import Asset` works
- `from mainsequence.client.models_vam import Asset` still works
- old-path imports log the deprecation warning and include the new import path
- `from mainsequence.client import Asset` works without importing the old shim
- old and new imports return the same class object

Representative assertion:

```python
from mainsequence.client.markets.models import Asset as NewAsset
from mainsequence.client.models_vam import Asset as OldAsset

assert OldAsset is NewAsset
```

### Task 8: Verify Packaging

Check packaging configuration so the new package is included in source
distributions and wheels.

Run targeted checks:

```bash
python -m py_compile mainsequence/client/markets/models/core.py
pytest tests/test_client.py
```

Also run import checks:

```bash
python - <<'PY'
from mainsequence.client.markets.models import Asset, Portfolio
from mainsequence.client.models_vam import Asset as DeprecatedAsset
from mainsequence.client import Asset as ClientAsset

assert DeprecatedAsset is Asset
assert ClientAsset is Asset
print("client market model imports ok")
PY
```

### Task 9: Correct The Library And Migrate Internal Imports

After the shim and compatibility tests are in place, update SDK implementation
code so internal imports use the canonical path:

```python
from mainsequence.client.markets.models import Asset
from mainsequence.client.markets.models import Portfolio
from mainsequence.client.markets.models import AssetTranslationTable
```

Internal SDK code must not import from:

```python
from mainsequence.client.models_vam import Asset
```

Run:

```bash
rg "mainsequence\\.client\\.models_vam|from \\.models_vam|from \\.\\.models_vam" mainsequence tests examples docs
```

Every remaining old-path reference should be one of:

- the compatibility shim
- deprecation-warning tests
- migration documentation that intentionally names the old path

Normal library implementation code should use
`mainsequence.client.markets.models`.

## Risks

- `mainsequence.client.__init__` can accidentally import the shim and emit
  warnings for normal users.
- Internal imports can keep using the deprecated path and hide migration gaps.
- Generated docs or CLI model references can keep old module names.
- The moved module can accidentally introduce relative imports that make future
  moves harder.
- Deprecation logs can become noisy if the shim is imported indirectly by common
  package imports.

## Open Questions

- Which release should remove `mainsequence.client.models_vam`?
- Should `mainsequence.client.markets.models.__init__` re-export all core
  symbols, or should it define an explicit public `__all__`?
