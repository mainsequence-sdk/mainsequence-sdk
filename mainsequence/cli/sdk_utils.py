from __future__ import annotations

"""
mainsequence.cli.sdk_utils
==========================

SDK version helpers aligned with VS Code extension behavior:
- Fetch latest GitHub release tag (fallback to latest tag)
- Read local mainsequence version from requirements.txt
"""

import pathlib
import re

import requests

GITHUB_REPO = "mainsequence-sdk/mainsequence-sdk"
RELEASES_LATEST = f"https://api.github.com/repos/{GITHUB_REPO}/releases/latest"
TAGS_LATEST = f"https://api.github.com/repos/{GITHUB_REPO}/tags?per_page=1"


def fetch_latest_sdk_version(timeout_s: float = 8.0) -> str | None:
    """
    Fetch latest SDK version from GitHub.

    Mirrors extension logic:
      1) releases/latest -> tag_name or name
      2) tags?per_page=1 -> first tag name

    Returns:
        str | None
    """
    headers = {
        "User-Agent": "mainsequence-cli",
        "Accept": "application/vnd.github+json",
    }
    r = requests.get(RELEASES_LATEST, headers=headers, timeout=timeout_s)
    if r.status_code == 200:
        data = r.json() or {}
        tag = data.get("tag_name") or data.get("name")
        if isinstance(tag, str) and tag.strip():
            return tag.strip()
    if r.status_code not in (200, 404):
        raise RuntimeError(f"GitHub releases returned {r.status_code}")

    r = requests.get(TAGS_LATEST, headers=headers, timeout=timeout_s)
    if r.status_code == 200:
        data = r.json()
        if isinstance(data, list) and data and data[0].get("name"):
            return str(data[0]["name"]).strip()
        return None
    raise RuntimeError(f"GitHub tags returned {r.status_code}")


def normalize_version(v: str) -> str:
    """Normalize versions by stripping leading 'v' and whitespace."""
    return (v or "").strip().lstrip("v").lstrip("V")


def read_local_sdk_version(requirements_txt: pathlib.Path) -> str | None:
    """
    Parse requirements.txt and extract the mainsequence package version.

    Returns:
        - a version string like "1.2.3"
        - "unversioned" if mainsequence is present but unpinned
        - None if not found or file missing
    """
    if not requirements_txt.exists():
        return None
    try:
        raw = requirements_txt.read_text(encoding="utf-8", errors="replace")
    except Exception:
        return None

    for line in raw.splitlines():
        t = line.strip()
        if not t or t.startswith("#"):
            continue
        m = re.match(
            r"^mainsequence(?:\[[^\]]+\])?\s*(?:==|===|>=|<=|!=|~=|>|<)?\s*([A-Za-z0-9._+-]+)?",
            t,
            flags=re.I,
        )
        if m:
            ver = m.group(1)
            return ver.strip() if ver else "unversioned"
    return None
