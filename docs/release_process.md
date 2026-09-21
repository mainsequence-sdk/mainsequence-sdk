# Release Process

This page is the branch and release standard for the `mainsequence` SDK. It
describes where work lands, what each branch publishes, and how the version of a
development release is decided.

## Branches

| Branch | Role | Publishes to PyPI |
| --- | --- | --- |
| `feat/*` | Work in progress. | Nothing. |
| `development` | Where features land, by merge or by direct push. Nobody tags here. | `X.Y.Z.devN`, automatically, on every push. |
| `main` | Receives `development` when a release is decided, through a pull request. | `X.Y.Z`, automatically, when the pull request is merged. |

**A merge to `main` is the release.** There is nothing else to it: no release
branch, no manual upload step, and no tag to push. The release workflow
publishes the version `pyproject.toml` declares, then creates the tag `vX.Y.Z`
and the GitHub release itself. A tag pushed by hand publishes nothing.

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

`pyproject.toml` is the only source of the version.

* **Base**: the version `pyproject.toml` declares on `development`, which is the
  release being worked toward. While it says `8.1.20`, development releases are
  `8.1.20.devN` and the merge to `main` publishes `8.1.20`.
* **Serial `N`**: the publishing workflow's run number. One push is one
  development release, however many commits that push carries.
* **Guard**: PyPI is read only to refuse a declared version that is already
  released. The build then fails with "development must declare the next
  release" instead of publishing under a number the repository does not show.
* **After a final release** the release workflow raises the patch number on
  `development` by itself, so once `8.1.20` is released the next development
  release is `8.1.21.devN` and there is no further `8.1.20.devN`. A minor or
  major release is declared by hand, by writing that version on `development`.

The logic lives in [`scripts/dev_release_version.py`][script] and is covered by
`tests/test_dev_release_version.py`.

Git tags are not used: this repository's tag history contains tags that do not
correspond to anything that was released, for example a `v9.0.0` tag.
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

Merge the pull request from `development` into `main` with a **merge commit**.
That is the whole release. [`publish-to-pypi.yml`][release-workflow] then:

1. reads the version `pyproject.toml` declares and stops when PyPI already has
   it ("A merge to main is a release and must carry the next version");
2. stops when a tag `vX.Y.Z` already exists on another commit;
3. builds and publishes `X.Y.Z` to PyPI;
4. creates the tag `vX.Y.Z` and the GitHub release on the merge commit, after
   the upload, so a tag always names code that is on PyPI;
5. deploys the documentation site from the released commit;
6. merges the release commit into `development`, raises the patch number there
   and pushes both in one push.

Do not push `main` back to `development` by hand. The workflow's push is made
with the workflow token and starts no development release; a push by hand
before the patch number is raised would publish one more `X.Y.Z.devN` of a
version that is already final.

`main` has no other way in: its ruleset accepts pull requests only. A pull
request into `main` that does not raise the version, a hotfix for example,
fails at step 1 and publishes nothing; raise the version in it.

### Never squash and never rebase a release merge

Both create new commits, so `main` and `development` stop sharing history. Once
that happens the next release conflicts with itself, and the workflow can no
longer merge the release commit back into `development`.

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
* **The tag ruleset "release tags v\*: admins only" lists GitHub Actions as a
  bypass actor.** The release workflow creates the tag with the workflow token.
  Without the bypass the `tag` job fails after the upload: the release is on
  PyPI, and the tag and the GitHub release are missing until the job is re-run.
* **The `github-pages` environment accepts the branch `main`**, because the
  documentation is deployed from the release run on `main`.
* **Branch `development` exists and shares history with `main`.** After the
  one-time cleanup of September 2026 both branches point at the same commit.

[script]: https://github.com/mainsequence-sdk/mainsequence-sdk/blob/main/scripts/dev_release_version.py
[release-workflow]: https://github.com/mainsequence-sdk/mainsequence-sdk/blob/main/.github/workflows/publish-to-pypi.yml
