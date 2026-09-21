"""Compute and apply the PEP 440 development version for a `development` build.

The release standard (see `docs/release_process.md`) is:

* `pyproject.toml` is the only source of the version. On `development` it
  declares the release being worked toward, `X.Y.Z`; development releases are
  `X.Y.Z.devN` and the final release `X.Y.Z` is published when `development`
  is merged into `main`;
* the serial is the publishing workflow's run number, so one push produces one
  development release however many commits it holds;
* PyPI is read only as a guard. A declared version that is already released
  means the bump that follows every release is missing, and the build stops
  instead of publishing under a number the repository does not show.

Git tags are not consulted: this repository's tag history contains tags that
were never released.

Used by `.github/workflows/publish-dev-to-pypi.yml`::

    python scripts/dev_release_version.py --run-number "$GITHUB_RUN_NUMBER" --write

and by `.github/workflows/publish-to-pypi.yml`, before a final release and
after it::

    python scripts/dev_release_version.py --final
    python scripts/dev_release_version.py --bump-patch

"""

from __future__ import annotations

import argparse
import json
import pathlib
import re
import sys
import urllib.error
import urllib.request
from collections.abc import Iterable, Mapping
from typing import Any

from packaging.version import InvalidVersion, Version

PACKAGE_NAME = "mainsequence"
PYPI_JSON_URL = f"https://pypi.org/pypi/{PACKAGE_NAME}/json"
PYPROJECT = pathlib.Path(__file__).resolve().parents[1] / "pyproject.toml"

# The `version = "..."` assignment of the `[project]` table. Anchored to the
# start of a line so a `version` key nested in another table cannot match.
_PROJECT_VERSION_RE = re.compile(r'^version = "[^"]*"$', re.MULTILINE)


class DevVersionError(RuntimeError):
    """Raised when a development version cannot be computed or applied."""


def iter_final_versions(releases: Mapping[str, Any]) -> Iterable[Version]:
    """Yield the final releases of a PyPI ``releases`` mapping.

    Development releases, pre-releases and post-releases are skipped, as are
    versions with no file left on the index and versions whose every file has
    been yanked: neither can be installed, so neither is a release users can be
    on.
    """
    for raw_version, files in releases.items():
        try:
            version = Version(raw_version)
        except InvalidVersion:
            continue
        if version.is_devrelease or version.is_prerelease or version.is_postrelease:
            continue
        if not files or all(file.get("yanked") for file in files):
            continue
        yield version


def latest_final_version(releases: Mapping[str, Any]) -> Version:
    """Return the newest installable final release."""
    finals = sorted(iter_final_versions(releases))
    if not finals:
        raise DevVersionError(
            f"PyPI lists no installable final release of {PACKAGE_NAME!r}, so the "
            "development version has no base to count from."
        )
    return finals[-1]


def next_patch(version: Version) -> str:
    """Return the version `development` declares once ``version`` is released."""
    return f"{version.major}.{version.minor}.{version.micro + 1}"


def declared_release(declared: str | None) -> Version:
    """Return the final ``X.Y.Z`` release that ``pyproject.toml`` declares."""
    if declared is None:
        raise DevVersionError("pyproject.toml declares no project version.")
    try:
        version = Version(declared)
    except InvalidVersion as exc:
        raise DevVersionError(f"pyproject.toml declares an invalid version {declared!r}.") from exc
    if version.is_devrelease or version.is_prerelease or version.is_postrelease:
        raise DevVersionError(
            f"pyproject.toml must declare a final X.Y.Z version, found {declared!r}."
        )
    return version


def dev_version(declared: Version, latest: Version | None, run_number: int) -> str:
    """Return the PEP 440 development version of the declared release.

    ``latest`` is the newest final release on PyPI, or ``None`` when there is
    none. The declared release must be ahead of it.
    """
    if run_number < 0:
        raise DevVersionError(f"Run number must not be negative, got {run_number}.")
    if latest is not None and declared <= latest:
        raise DevVersionError(
            f"pyproject.toml declares {declared}, and {latest} is already released. "
            "development must declare the next release."
        )
    return f"{declared.major}.{declared.minor}.{declared.micro}.dev{run_number}"


def final_version(declared: Version, latest: Version | None) -> str:
    """Return the version a merge to `main` releases.

    ``latest`` is the newest final release on PyPI, or ``None`` when there is
    none. The declared release must be ahead of it: a merge to `main` is the
    release, and PyPI accepts a version once.
    """
    if latest is not None and declared <= latest:
        raise DevVersionError(
            f"pyproject.toml declares {declared}, and {latest} is already released. "
            "A merge to main is a release and must carry the next version."
        )
    return f"{declared.major}.{declared.minor}.{declared.micro}"


def fetch_releases(url: str = PYPI_JSON_URL, timeout: float = 30.0) -> Mapping[str, Any]:
    """Fetch the package's release index from PyPI."""
    try:
        with urllib.request.urlopen(url, timeout=timeout) as response:  # noqa: S310
            payload = json.load(response)
    except (urllib.error.URLError, TimeoutError, json.JSONDecodeError) as exc:
        raise DevVersionError(f"Could not read the release index from {url}: {exc}") from exc
    releases = payload.get("releases")
    if not isinstance(releases, dict):
        raise DevVersionError(f"The release index at {url} carries no 'releases' mapping.")
    return releases


def read_declared_version(pyproject_text: str) -> str | None:
    """Return the version currently declared in ``pyproject.toml``."""
    match = _PROJECT_VERSION_RE.search(pyproject_text)
    return None if match is None else match.group(0).split('"')[1]


def apply_version(pyproject_text: str, version: str) -> str:
    """Return ``pyproject_text`` with the project version replaced."""
    replaced, count = _PROJECT_VERSION_RE.subn(f'version = "{version}"', pyproject_text, count=1)
    if count != 1:
        raise DevVersionError(
            "pyproject.toml has no top-level 'version = \"...\"' assignment to rewrite."
        )
    return replaced


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--run-number",
        type=int,
        help="Serial for this development release; the publishing workflow's run number.",
    )
    parser.add_argument(
        "--write",
        action="store_true",
        help="Rewrite pyproject.toml's project version in place with the computed version.",
    )
    parser.add_argument(
        "--bump-patch",
        action="store_true",
        help="Write the next patch version to pyproject.toml; run on development after a release.",
    )
    parser.add_argument(
        "--final",
        action="store_true",
        help="Print the version a merge to main releases; fails when PyPI already has it.",
    )
    args = parser.parse_args(argv)

    pyproject_text = PYPROJECT.read_text(encoding="utf-8")
    declared = declared_release(read_declared_version(pyproject_text))

    if args.bump_patch:
        bumped = next_patch(declared)
        PYPROJECT.write_text(apply_version(pyproject_text, bumped), encoding="utf-8")
        print(bumped)
        return 0

    if args.final:
        latest = latest_final_version(fetch_releases())
        version = final_version(declared, latest)
        print(f"Declared in pyproject.toml:   {declared}", file=sys.stderr)
        print(f"Newest final release on PyPI: {latest}", file=sys.stderr)
        print(version)
        return 0

    if args.run_number is None:
        raise DevVersionError("--run-number is required.")
    latest = latest_final_version(fetch_releases())
    version = dev_version(declared, latest, args.run_number)

    print(f"Declared in pyproject.toml:   {declared}", file=sys.stderr)
    print(f"Newest final release on PyPI: {latest}", file=sys.stderr)
    print(f"Development version:          {version}", file=sys.stderr)

    if args.write:
        PYPROJECT.write_text(apply_version(pyproject_text, version), encoding="utf-8")
        print(f"Wrote version {version} to {PYPROJECT}", file=sys.stderr)

    print(version)
    return 0


if __name__ == "__main__":  # pragma: no cover
    try:
        raise SystemExit(main())
    except DevVersionError as error:
        print(f"error: {error}", file=sys.stderr)
        raise SystemExit(1) from error
