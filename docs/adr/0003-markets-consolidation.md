# ADR 0003: Move Market Folders Under `mainsequence.markets`

Date: 2026-05-20

Status: Proposed

## Context

The SDK currently keeps these two market-related folders directly under
`mainsequence`:

```text
mainsequence/
  instruments/
  virtualfundbuilder/
```

The requested refactor is to add a `markets` module under `mainsequence` and
move both folders inside it:

```text
mainsequence/
  markets/
    instruments/
    virtualfundbuilder/
```

This ADR is only about the package move. It does not merge the two modules,
change their responsibilities, change runtime behavior, or redesign their APIs.

## Decision

Move:

```text
mainsequence/instruments/
```

to:

```text
mainsequence/markets/instruments/
```

Move:

```text
mainsequence/virtualfundbuilder/
```

to:

```text
mainsequence/markets/virtualfundbuilder/
```

Add:

```text
mainsequence/markets/__init__.py
```

The canonical import paths after the move are:

```python
import mainsequence.markets.instruments
import mainsequence.markets.virtualfundbuilder
```

Do not keep backwards-compatible shim packages at the old paths. The canonical
`mainsequence.markets.*` imports are the only supported SDK paths for these
packages.

## Non-Goals

This refactor must not:

- combine `instruments` and `virtualfundbuilder` into one package
- rename internal classes or public APIs
- change pricing behavior
- change portfolio construction behavior
- change TDAG behavior
- change CLI release behavior
- introduce dashboard or UI decisions

## Implementation Plan

### Phase 1: Create Target Package

1. Create `mainsequence/markets/`.
2. Add `mainsequence/markets/__init__.py`.
3. Move `mainsequence/instruments/` into `mainsequence/markets/instruments/`.
4. Move `mainsequence/virtualfundbuilder/` into
   `mainsequence/markets/virtualfundbuilder/`.

This phase should be a mechanical filesystem move.

### Phase 2: Update Internal Imports

Do an initial import sweep after the mechanical move and fix imports that would
break immediately.

Search:

```bash
rg "mainsequence\.instruments"
rg "mainsequence\.virtualfundbuilder"
```

Replace with:

```python
mainsequence.markets.instruments
mainsequence.markets.virtualfundbuilder
```

Also check relative imports inside moved packages. Prefer preserving relative
imports where they still work after the move.

### Phase 3: Remove Compatibility Shims

Remove the deprecated top-level packages:

```text
mainsequence/instruments/
mainsequence/virtualfundbuilder/
```

Users must import the canonical packages under `mainsequence.markets`.

### Phase 4: Update Docs And Tests

Update references in:

- `README.md`
- `docs/index.md`
- `docs/knowledge/instruments/`
- `docs/knowledge/virtualfundbuilder/`
- `docs/reference/`
- `agent_scaffold/skills/`
- tests that import either package

The docs should describe the new folder path and import path. They should not
claim any behavioral change.

### Phase 5: Verify

Run targeted checks:

```bash
python -m py_compile mainsequence/markets/**/*.py
pytest tests/test_instruments.py
```

Then run import checks for the new canonical paths:

```bash
python - <<'PY'
import mainsequence.markets.instruments
import mainsequence.markets.virtualfundbuilder
print("markets imports ok")
PY
```

Verify the old top-level compatibility paths are gone:

```bash
python - <<'PY'
import importlib.util

assert importlib.util.find_spec("mainsequence.instruments") is None
assert importlib.util.find_spec("mainsequence.virtualfundbuilder") is None
print("compat imports removed")
PY
```

### Phase 6: Correct Internal Imports To Canonical Paths

After the basic import checks are in place, update SDK
source code to use the proper canonical package paths internally.

Internal SDK code should import:

```python
mainsequence.markets.instruments
mainsequence.markets.virtualfundbuilder
```

It should not rely on removed compatibility paths:

```python
mainsequence.instruments
mainsequence.virtualfundbuilder
```

Run a final search:

```bash
rg "mainsequence\.instruments|mainsequence\.virtualfundbuilder" mainsequence tests docs agent_scaffold
```

Every remaining old-path reference should be migration documentation that
intentionally mentions the old path.

All normal SDK implementation imports should use `mainsequence.markets.*`.

## Risks

- Internal imports may still point to the old top-level paths.
- Tests, docs, generated reference pages, or agent scaffold skills may keep old
  paths.
- Package data rules in `pyproject.toml` may need adjustment after the move.
- Removing compatibility shims is a breaking import-path change for users still
  importing the old top-level packages.

## Open Questions

- Should docs immediately teach only `mainsequence.markets.*`, or mention both
  old and new paths during the transition?
- What release should remove the compatibility shims?
