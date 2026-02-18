from __future__ import annotations

"""
mainsequence.cli.docker_utils
=============================

Docker environment helpers aligned with VS Code extension behavior:

- If DEFAULT_BASE_IMAGE is present in env text:
    - resolve it into a full image reference
    - if "none" / empty: fetch latest tag from GHCR for PodDeploymentOrchestrator
- Create Dockerfile + .dockerignore scaffolding if missing
- Create/update .devcontainer/devcontainer.json
- Build docker image via `docker buildx build ...`

The VS Code extension ships template files; the CLI embeds templates as strings so it
works out of the box.
"""

import datetime as _dt
import json
import pathlib
import re
import subprocess
from dataclasses import dataclass

import requests

POD_DEPLOYMENT_GHCR_OWNER = "main-sequence-server-side"
POD_DEPLOYMENT_BASE_REPO = "poddeploymentorchestrator"
POD_DEPLOYMENT_GHCR_REPO = f"{POD_DEPLOYMENT_GHCR_OWNER}/{POD_DEPLOYMENT_BASE_REPO}"
POD_DEPLOYMENT_IMAGE = f"ghcr.io/{POD_DEPLOYMENT_GHCR_REPO}"


DOCKERFILE_TEMPLATE = """\
# syntax=docker/dockerfile:1
#
# MainSequence scaffold Dockerfile
# Base image resolved from DEFAULT_BASE_IMAGE or platform defaults.
#
FROM __BASE_IMAGE__

WORKDIR /app

# Copy the project into the image
COPY . /app

# NOTE:
# This is a scaffold. Your base image may already contain tooling.
# Customize as needed (install deps, set entrypoints, etc.).
"""

DOCKERIGNORE_TEMPLATE = """\
# MainSequence scaffold .dockerignore
.git
.gitignore
**/__pycache__/
**/*.pyc
.venv
.env
.devcontainer
.vscode
.DS_Store
*.log
dist
build
"""


@dataclass(frozen=True)
class GhcrImage:
    repo: str
    tag: str


def extract_env_value(env_text: str, key: str) -> str | None:
    """
    Extract KEY=value from env_text, supporting optional `export` prefix and quoting.
    """
    if not env_text:
        return None
    pat = re.compile(rf"^\s*(?:export\s+)?{re.escape(key)}\s*=\s*(.*)\s*$", flags=re.M)
    for line in env_text.splitlines():
        m = pat.match(line)
        if not m:
            continue
        val = m.group(1).strip()
        if (val.startswith('"') and val.endswith('"')) or (val.startswith("'") and val.endswith("'")):
            val = val[1:-1]
        return val
    return None


def is_none_value(value: str | None) -> bool:
    """Return True if value is None/empty/'none' (case-insensitive)."""
    if value is None:
        return True
    v = value.strip().lower()
    return v == "" or v == "none"


def _fetch_ghcr_tags(repo: str, timeout_s: float = 6.0) -> list[str] | None:
    """
    Fetch GHCR tags for a repo using the public GHCR registry API.

    Returns:
        list[str] | None
    """
    url = f"https://ghcr.io/v2/{repo}/tags/list"
    r = requests.get(url, timeout=timeout_s)
    if r.status_code == 404:
        return None
    if r.status_code != 200:
        return None
    try:
        data = r.json()
    except Exception:
        return None
    tags = data.get("tags")
    return tags if isinstance(tags, list) else None


def _parse_semver_tag(tag: str) -> tuple[int, int, int, str] | None:
    """
    Parse tag like 'v1.2.3' or '1.2.3' -> (major, minor, patch, original_tag).
    """
    t = tag[1:] if tag.startswith("v") else tag
    m = re.match(r"^(\d+)\.(\d+)\.(\d+)$", t)
    if not m:
        return None
    return int(m.group(1)), int(m.group(2)), int(m.group(3)), tag


def _pick_highest_semver_tag(tags: list[str]) -> str | None:
    parsed = [_parse_semver_tag(t) for t in tags]
    parsed = [p for p in parsed if p is not None]
    if not parsed:
        return None
    parsed.sort(key=lambda x: (x[0], x[1], x[2]), reverse=True)
    return parsed[0][3]


def fetch_latest_pod_deployment_image() -> str:
    """
    Resolve the default PodDeploymentOrchestrator image tag.

    Mirrors VS Code extension behavior:
      - Prefer 'latest'
      - Else pick highest semver
      - Else first available tag
      - Else fallback to ':latest'
    """
    fallback = f"{POD_DEPLOYMENT_IMAGE}:latest"
    tags = _fetch_ghcr_tags(POD_DEPLOYMENT_GHCR_REPO)
    if not tags:
        return fallback
    if "latest" in tags:
        return f"{POD_DEPLOYMENT_IMAGE}:latest"
    best = _pick_highest_semver_tag(tags)
    return f"{POD_DEPLOYMENT_IMAGE}:{best or tags[0]}"


def resolve_named_base_image(name: str) -> str:
    """
    Resolve shorthand DEFAULT_BASE_IMAGE names into full GHCR references.

    Mirrors VS Code extension logic:
      - If starts with 'ghcr.io/' => return as-is
      - If contains '/' => treat as fully qualified repository/image
      - Else treat as '<variant>[:tag]' and map to:
            ghcr.io/main-sequence-server-side/poddeploymentorchestrator-<variant>:<tag-or-latest>
    """
    trimmed = (name or "").strip()
    if not trimmed:
        return trimmed
    if trimmed.startswith("ghcr.io/"):
        return trimmed
    if "/" in trimmed:
        return trimmed

    image_name, tag = (trimmed.split(":", 1) + [""])[:2]
    if image_name.startswith(f"{POD_DEPLOYMENT_BASE_REPO}-"):
        repo_name = image_name
    else:
        repo_name = f"{POD_DEPLOYMENT_BASE_REPO}-{image_name}"
    final_tag = tag or "latest"
    return f"ghcr.io/{POD_DEPLOYMENT_GHCR_OWNER}/{repo_name}:{final_tag}"


def parse_ghcr_image(image: str) -> GhcrImage | None:
    """Parse ghcr.io/<repo>:<tag> into (repo, tag)."""
    if not image.startswith("ghcr.io/"):
        return None
    without = image[len("ghcr.io/") :]
    idx = without.rfind(":")
    if idx == -1:
        return GhcrImage(repo=without, tag="latest")
    repo = without[:idx]
    tag = without[idx + 1 :] or "latest"
    return GhcrImage(repo=repo, tag=tag)


def warn_if_ghcr_image_missing(image: str) -> str | None:
    """
    Return a warning message if the GHCR image tag does not exist, else None.

    We do best-effort checks and never raise.
    """
    parsed = parse_ghcr_image(image)
    if not parsed:
        return None
    tags = _fetch_ghcr_tags(parsed.repo)
    if not tags:
        return f"Could not verify GHCR tags for {image}. Proceeding anyway."
    if parsed.tag not in tags:
        return f'GHCR tag "{parsed.tag}" not found for {image}. Proceeding anyway.'
    return None


def resolve_base_image(default_base_image: str) -> tuple[str, list[str]]:
    """
    Resolve DEFAULT_BASE_IMAGE string to a usable image reference.

    Returns:
        (image_ref, warnings)
    """
    warnings: list[str] = []
    if is_none_value(default_base_image):
        img = fetch_latest_pod_deployment_image()
        return img, warnings

    resolved = resolve_named_base_image(default_base_image)
    w = warn_if_ghcr_image_missing(resolved)
    if w:
        warnings.append(w)
    return resolved, warnings


def ensure_docker_scaffold(target_dir: pathlib.Path, base_image: str) -> tuple[bool, list[str]]:
    """
    Ensure Dockerfile and .dockerignore exist in target_dir, creating them if missing.

    Returns:
        (changed, messages)
    """
    msgs: list[str] = []
    changed = False
    target_dir.mkdir(parents=True, exist_ok=True)

    dockerfile = target_dir / "Dockerfile"
    if not dockerfile.exists():
        dockerfile.write_text(DOCKERFILE_TEMPLATE.replace("__BASE_IMAGE__", base_image), encoding="utf-8")
        msgs.append(f"Created Dockerfile (base image: {base_image})")
        changed = True
    else:
        msgs.append("Dockerfile already exists (skipped).")

    dockerignore = target_dir / ".dockerignore"
    if not dockerignore.exists():
        dockerignore.write_text(DOCKERIGNORE_TEMPLATE, encoding="utf-8")
        msgs.append("Created .dockerignore")
        changed = True
    else:
        msgs.append(".dockerignore already exists (skipped).")

    return changed, msgs


def _git_short_hash(project_dir: pathlib.Path) -> str | None:
    try:
        r = subprocess.run(
            ["git", "-C", str(project_dir), "rev-parse", "--short", "HEAD"],
            capture_output=True,
            text=True,
        )
        if r.returncode == 0:
            out = (r.stdout or "").strip().split()
            return out[0] if out else None
    except Exception:
        return None
    return None


def _timestamp_tag() -> str:
    now = _dt.datetime.now()
    return now.strftime("%Y%m%d%H%M%S")


def compute_docker_image_ref(project_dir: pathlib.Path) -> str:
    """
    Create an image ref similar to the extension:
      <safe-folder>-img:<git-short-hash-or-timestamp>
    """
    base = project_dir.name
    safe = re.sub(r"[^a-z0-9_.-]+", "", base.lower())
    image_name = f"{safe or 'project'}-img"
    tag = _git_short_hash(project_dir) or _timestamp_tag()
    return f"{image_name}:{tag}"


def write_devcontainer_config(project_dir: pathlib.Path, image_ref: str) -> pathlib.Path:
    """
    Create/update .devcontainer/devcontainer.json with an image reference.
    """
    dev_dir = project_dir / ".devcontainer"
    path = dev_dir / "devcontainer.json"
    config: dict | None = None

    if path.exists():
        try:
            config = json.loads(path.read_text(encoding="utf-8"))
        except Exception as e:
            raise RuntimeError(f"Failed to parse existing devcontainer.json: {e}") from e

    if not config:
        name = project_dir.name
        config = {
            "name": f"{name} (MainSequence)" if name else "MainSequence Project",
            "image": image_ref,
            "workspaceFolder": "/app",
            "workspaceMount": "source=${localWorkspaceFolder},target=/app,type=bind,consistency=cached",
        }
    else:
        config["image"] = image_ref
        config.setdefault("workspaceFolder", "/app")
        config.setdefault(
            "workspaceMount",
            "source=${localWorkspaceFolder},target=/app,type=bind,consistency=cached",
        )

    dev_dir.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(config, indent=2) + "\n", encoding="utf-8")
    return path


def build_docker_environment(project_dir: pathlib.Path, image_ref: str) -> int:
    """
    Run docker buildx build, streaming output to the current terminal.

    Returns:
        int: process return code
    """
    dockerfile = project_dir / "Dockerfile"
    if not dockerfile.exists():
        raise RuntimeError("Dockerfile not found in the project root.")

    cmd = [
        "docker",
        "buildx",
        "build",
        "--platform",
        "linux/amd64",
        "--load",
        "-t",
        image_ref,
        "-f",
        "Dockerfile",
        ".",
    ]
    p = subprocess.run(cmd, cwd=str(project_dir))
    return p.returncode
