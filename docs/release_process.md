# Release Process

This page is the branch and release standard for the `mainsequence` SDK. It
describes where work lands, what each branch publishes, and how the version of a
development release is decided.

## Branches

| Branch | Role | Publishes to PyPI |
| --- | --- | --- |
| `feat/*` | Work in progress. | Nothing. |
| `development` | Where features land, by merge or by direct push. Nobody tags here. | `X.Y.Z.devN`, automatically, on every push. |
| `main` | Receives `development` when a release is decided. `vX.Y.Z` is tagged here. | `X.Y.Z`, when the tag is pushed. |

**A release is a plain tag `vX.Y.Z` on `main`.** There is nothing else to it: no
release branch, no manual upload step. The publish job refuses a tag whose
commit is not contained in `main`, so a tag pushed from any other branch
publishes nothing.

## Development releases

Every push to `development` publishes one PEP 440 development release. PyPI does
not accept a form like `1.2.x-dev`; the form is `1.2.6.dev41`.

A development release sorts **before** the final release of the same number:

```text
1.2.5  <  1.2.6.dev41  <  1.2.6
```

so it carries the number of the *next* release, not the last one.

### Users are not affected

`pip` and `uv` ignore development releases when resolving a requirement. A plain
`pip install mainsequence` can never land on one. To get a development release
you have to ask for it explicitly:

```bash
pip install "mainsequence==8.1.20.dev41"
```

```bash
pip install --pre mainsequence
```

### How the version is computed

Nothing is edited or tagged by hand.

* **Base** — the newest final release on PyPI, with its patch number raised by
  one. If PyPI's newest final release is `8.1.19`, the base is `8.1.20`.
* **Serial `N`** — the publishing workflow's run number. One push is one
  development release, however many commits that push carries.

The logic lives in [`scripts/dev_release_version.py`][script] and is covered by
`tests/test_dev_release_version.py`.

Git tags are deliberately **not** used as the base. This repository's tag
history contains tags that do not correspond to anything that was released — for
example a `v9.0.0` tag that exists while PyPI's newest final release is `8.1.19`
— so PyPI is the only trustworthy record of what the newest final release is.
For the same reason, a version declared in `pyproject.toml` that is ahead of the
computed base does not change the result; the workflow log notes it and counts
from PyPI anyway.

### Guards

* The publish job runs the test suite first and publishes nothing when it fails.
  It runs the very same reusable workflow (`.github/workflows/tests.yml`) that
  gates pull requests, so the guard cannot drift from the check.
* A newer push to `development` cancels a build that is still running, so the
  newest commit is the one that reaches PyPI.

## Cutting a release

1. Merge `development` into `main` with a **merge commit** (or a fast-forward).
2. Tag the merge commit on `main` as `vX.Y.Z` and push the tag.
3. Fast-forward `development` back to `main` so both branches start the next
   cycle equal:

   ```bash
   git push origin origin/main:refs/heads/development
   ```

### Never squash and never rebase a release merge

Both create new commits, so `main` and `development` stop sharing history. Once
that happens the next release conflicts with itself, and a tag made on one
branch is not contained in the other — which the publish guard will (correctly)
refuse.

## Tests

The suite runs offline. Tests that need a live Main Sequence backend and
credentials carry the `live` marker and are deselected by default, through
`addopts` in `pyproject.toml`:

```bash
pytest            # the offline suite; what CI runs
pytest -m live    # only the live-backend tests; needs credentials
```

Mark a new backend-driven test with `@pytest.mark.live`, or a whole module with
`pytestmark = pytest.mark.live`.

## Repository setup this depends on

These are configured outside the repository and are listed here so they are not
lost:

* **PyPI trusted publishers.** The project needs one entry per publishing
  workflow, since an entry is keyed by workflow filename *and* environment. Both
  are configured:

  | Workflow | Publishes | Environment |
  | --- | --- | --- |
  | `publish-to-pypi.yml` | final releases | `pypi` |
  | `publish-dev-to-pypi.yml` | development releases | `pypi-development` |

  The `environment:` in a publishing workflow has to match its entry exactly, or
  PyPI rejects the OIDC token and the upload fails.
* **Branch `development` exists and shares history with `main`.** After the
  one-time cleanup of September 2026 both branches point at the same commit.

[script]: https://github.com/mainsequence-sdk/mainsequence-sdk/blob/main/scripts/dev_release_version.py
