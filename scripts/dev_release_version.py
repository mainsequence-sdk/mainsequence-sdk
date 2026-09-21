"""Compute and apply the PEP 440 development version for a `development` build.

The release standard (see `docs/knowledge/release_process.md`) is:

* the base is the newest **final** release on PyPI with its patch number raised
  by one, so the development release carries the number of the *next* release;
* the serial is the publishing workflow's run number, so one push produces one
  development release however many commits it holds.

Git tags are deliberately not consulted: this repository's tag history contains
tags that were never released, so PyPI is the only trustworthy record of what
the newest final release actually is.

Used by `.github/workflows/publish-dev-to-pypi.yml`::

    python scripts/dev_release_version.py --run-number "$GITHUB_RUN_NUMBER" --write

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


def next_patch_base(latest: Version) -> str:
    """Return the version a development release counts towards."""
    return f"{latest.major}.{latest.minor}.{latest.micro + 1}"


def dev_version(latest: Version, run_number: int) -> str:
    """Return the PEP 440 development version for ``run_number``."""
    if run_number < 0:
        raise DevVersionError(f"Run number must not be negative, got {run_number}.")
    return f"{next_patch_base(latest)}.dev{run_number}"


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
        required=True,
        help="Serial for this development release; the publishing workflow's run number.",
    )
    parser.add_argument(
        "--write",
        action="store_true",
        help="Rewrite pyproject.toml's project version in place with the computed version.",
    )
    args = parser.parse_args(argv)

    latest = latest_final_version(fetch_releases())
    version = dev_version(latest, args.run_number)

    pyproject_text = PYPROJECT.read_text(encoding="utf-8")
    declared = read_declared_version(pyproject_text)

    print(f"Newest final release on PyPI: {latest}", file=sys.stderr)
    print(f"Development version:          {version}", file=sys.stderr)
    if declared is not None:
        print(f"Declared in pyproject.toml:   {declared}", file=sys.stderr)
        try:
            if Version(declared) > Version(next_patch_base(latest)):
                # Not an error: the standard bases the number on PyPI precisely
                # because pyproject.toml and the tag history cannot be trusted.
                # Worth saying out loud, though, since the development release
                # will then sort below the release being prepared.
                print(
                    f"Note: pyproject.toml declares {declared}, which is ahead of the computed "
                    f"base {next_patch_base(latest)}. The development release still counts from "
                    "PyPI, as the release standard requires.",
                    file=sys.stderr,
                )
        except InvalidVersion:
            pass

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
