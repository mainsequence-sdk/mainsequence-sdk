# ADR 0004: Move Client Market Models To `mainsequence.client.markets.models`

Date: 2026-05-20

Status: Proposed

## Context

The SDK previously kept the market and virtual account management models in:

```text
mainsequence/client/models_vam.py
```

That module contains market-facing models such as assets, asset categories,
translation tables, portfolios, accounts, trades, virtual
funds, and orders. The module name is no longer a good fit for the public API
surface, and it sits directly under `mainsequence.client` instead of a market
specific package.

The market model package should split implementation by domain:

```text
mainsequence/client/markets/models/
  __init__.py
  core.py
  assets.py
  accounts_and_portfolios.py
```

The canonical import path should be:

```python
from mainsequence.client.markets.models import Asset, Portfolio
```

`mainsequence.client` already re-exports these models for users who import from
the top-level client package. Because that public surface remains available,
there is no need to keep a `mainsequence.client.models_vam` compatibility
module.

## Decision

Move the implementation from the old monolithic module:

```text
mainsequence/client/models_vam.py
```

to these domain modules:

```text
mainsequence/client/markets/models/core.py
mainsequence/client/markets/models/assets.py
mainsequence/client/markets/models/accounts_and_portfolios.py
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

`mainsequence.client.markets.models.core` is for shared definitions used across
market model domains, such as `Calendar` and common query helpers.
`mainsequence.client.markets.models.assets` owns asset-related models.
`mainsequence.client.markets.models.accounts_and_portfolios` owns account,
portfolio, fund, execution, trade, and order models.

None of those implementation modules are the canonical import path for SDK users
or internal SDK code. `mainsequence.client.markets.models.__init__` must
re-export the public model symbols from the domain modules.

Delete:

```text
mainsequence/client/models_vam.py
```

Do not keep a compatibility shim at `mainsequence.client.models_vam`.

`mainsequence.client` may continue to re-export these classes for user
convenience, but it must import them from the canonical module:

```python
from mainsequence.client.markets.models import *
```

The moved implementation must use absolute package imports only. Do not use
relative imports in any module under `mainsequence.client.markets.models`.

Use:

```python
from mainsequence.client.base import BaseObjectOrm, BasePydanticModel
from mainsequence.client.exceptions import raise_for_response
from mainsequence.client.markets.models.assets import Asset, AssetMixin
from mainsequence.client.markets.models.core import Calendar
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
from ...base import BaseObjectOrm, BasePydanticModel
from ...exceptions import raise_for_response
from .assets import Asset, AssetMixin
from .core import Calendar
from ...models_tdag import DataNodeUpdate
from ...utils import DATE_FORMAT, MAINSEQUENCE_ENDPOINT, DoesNotExist, make_request
from ...utils import MARKETS_CONSTANTS as CONSTANTS
```

## Non-Goals

This refactor must not:

- rename market model classes
- change model fields or validation behavior
- change endpoint paths or request behavior
- change portfolio, account, order, virtual fund, or asset semantics
- keep `mainsequence.client.models_vam` as a public or compatibility module
- introduce relative imports under `mainsequence.client.markets.models`
- redesign the broader `mainsequence.client` package layout

## Implementation Tasks

### Task 1: Create Target Package

Create:

```text
mainsequence/client/markets/
mainsequence/client/markets/__init__.py
mainsequence/client/markets/models/
mainsequence/client/markets/models/__init__.py
mainsequence/client/markets/models/assets.py
mainsequence/client/markets/models/accounts_and_portfolios.py
```

Keep package initializers small. `mainsequence.client.markets.models` is the
canonical import package and must re-export public model symbols from the domain
modules.

### Task 2: Move The Implementation

Move shared definitions, such as `Calendar`, `COMPOSITE_TO_ISO`, and
`_set_query_param_on_url`, to:

```text
mainsequence/client/markets/models/core.py
```

Move asset-specific models and helpers to:

```text
mainsequence/client/markets/models/assets.py
```

Move account, portfolio, fund, execution, trade, and order models to:

```text
mainsequence/client/markets/models/accounts_and_portfolios.py
```

Preserve model behavior and public class names. This should be a mechanical move
except for import path corrections.

### Task 3: Convert Imports In The Moved Modules To Absolute Paths

Replace relative imports in all `mainsequence.client.markets.models` modules
with full package imports.

Representative imports include:

```python
from mainsequence.client.base import BaseObjectOrm, BasePydanticModel
from mainsequence.client.exceptions import raise_for_response
from mainsequence.client.markets.models.assets import Asset, AssetMixin
from mainsequence.client.markets.models.core import Calendar
from mainsequence.client.models_tdag import DataNodeUpdate
from mainsequence.client.utils import (
    DATE_FORMAT,
    MAINSEQUENCE_ENDPOINT,
    DoesNotExist,
    make_request,
)
from mainsequence.client.utils import MARKETS_CONSTANTS as CONSTANTS
```

Run a search inside the moved package:

```bash
rg "from \\.|import \\." mainsequence/client/markets/models
```

There should be no relative imports left in this package.

### Task 4: Remove The Old Module

Delete:

```text
mainsequence/client/models_vam.py
```

Do not leave a shim. Existing `from mainsequence.client import Asset` style
imports remain supported through `mainsequence.client.__init__`.

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

### Task 6: Update Documentation And Reference Pages

Update documentation that names the old module path. At minimum, check:

```bash
rg "mainsequence\\.client\\.models_vam|models_vam" docs examples README.md CHANGELOG.md
```

Add reference documentation for:

```text
mainsequence.client.markets.models
```

Remove `models_vam` reference documentation.

### Task 7: Add Import Tests

Add tests that verify:

- `from mainsequence.client.markets.models import Asset` works
- `from mainsequence.client import Asset` works without importing
  `mainsequence.client.models_vam`

Representative assertion:

```python
from mainsequence.client import Asset as ClientAsset
from mainsequence.client.markets.models import Asset as MarketAsset

assert ClientAsset is MarketAsset
```

### Task 8: Verify Packaging

Check packaging configuration so the new package is included in source
distributions and wheels.

Run targeted checks:

```bash
python -m py_compile mainsequence/client/markets/models/core.py
python -m py_compile mainsequence/client/markets/models/assets.py
python -m py_compile mainsequence/client/markets/models/accounts_and_portfolios.py
pytest tests/test_client_markets_models_compat.py
```

Also run import checks:

```bash
python - <<'PY'
from mainsequence.client.markets.models import Asset, Portfolio
from mainsequence.client import Asset as ClientAsset

assert ClientAsset is Asset
print("client market model imports ok")
PY
```

### Task 9: Correct The Library And Migrate Internal Imports

Update SDK implementation code so internal imports use the canonical path:

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

There should be no normal SDK implementation imports that use
`mainsequence.client.models_vam`.

## Risks

- Generated docs or CLI model references can keep old module names.
- The moved modules can accidentally introduce relative imports that make future
  moves harder.
- External callers who imported `mainsequence.client.models_vam` directly must
  migrate to `mainsequence.client.markets.models` or `mainsequence.client`.

## Open Questions

- Should `mainsequence.client.markets.models.__init__` re-export all core
  symbols, or should it define an explicit public `__all__`?
