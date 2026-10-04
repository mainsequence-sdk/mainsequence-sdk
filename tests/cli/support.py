from __future__ import annotations

import hashlib
import importlib
import pathlib
import subprocess
import sys
import types

from tests._support import REPOSITORY_ROOT

USER_UID = "8f5d6b54-2f5e-4a8b-bb10-0b17f3f4c123"


TEAM_UID = "3f1cc452-43ec-49cb-b2ba-87dbac164d29"


_REMOVED_DOMAIN_TOKEN = "PRO" + "JECT"


_UNSUPPORTED_REPOSITORY_UID_ENV = "MAIN_SEQUENCE_" + _REMOVED_DOMAIN_TOKEN + "_UID"


_UNSUPPORTED_REPOSITORY_BRANCH_UID_ENV = "MAIN_SEQUENCE_" + _REMOVED_DOMAIN_TOKEN + "_BRANCH_UID"


_UNSUPPORTED_REPOSITORY_NUMERIC_ID_ENV = "MAIN_SEQUENCE_" + _REMOVED_DOMAIN_TOKEN + "_ID"


def _load_cli_module():
    """
    Import mainsequence.cli.cli without requiring mainsequence/__init__.py side effects.

    In this repo, importing `mainsequence` can trigger runtime logging bootstrap that expects
    TDAG-specific env vars and network access. For CLI tests we only need the CLI package.
    """
    try:
        return importlib.import_module("mainsequence.cli.cli")
    except Exception:
        repo_root = REPOSITORY_ROOT
        pkg_root = repo_root / "mainsequence"
        cli_root = pkg_root / "cli"

        for name in ("mainsequence.cli.cli", "mainsequence.cli", "mainsequence"):
            sys.modules.pop(name, None)

        pkg = types.ModuleType("mainsequence")
        pkg.__path__ = [str(pkg_root)]
        sys.modules["mainsequence"] = pkg

        subpkg = types.ModuleType("mainsequence.cli")
        subpkg.__path__ = [str(cli_root)]
        sys.modules["mainsequence.cli"] = subpkg
        return importlib.import_module("mainsequence.cli.cli")


def _session_report(**overrides):
    report = {
        "endpoint": "https://backend.test",
        "authenticated": True,
        "checked_with_backend": False,
        "auth_mode": "jwt",
        "username": "u@example.com",
        "source": "store",
        "storage": "test store",
        "store_error": None,
        "session_expires_at": 1_900_000_000,
        "access_expires_at": 1_899_000_000,
    }
    report.update(overrides)
    return report


def _sync_project(root: pathlib.Path) -> tuple[pathlib.Path, pathlib.Path]:
    """Create a project root with pyproject.toml and a `.venv` holding `uv`."""
    project = root / "code-repository"
    bin_dir = project / ".venv" / "bin"
    bin_dir.mkdir(parents=True)
    (bin_dir / "python").write_text("", encoding="utf-8")
    uv_path = bin_dir / "uv"
    uv_path.write_text("", encoding="utf-8")
    (project / "pyproject.toml").write_text(
        '[project]\nname = "demo"\nversion = "1.2.3"\n',
        encoding="utf-8",
    )
    return project, uv_path


def _record_sync_side_effects(monkeypatch, cli_mod) -> dict[str, list]:
    """Record every process `sync` starts and every network access it attempts.

    `subprocess.run` is replaced for the whole process, so a Git command run
    through any helper is recorded too. HTTP is refused at the `requests`
    session and at the socket, below every SDK client.
    """
    import socket

    import requests

    api_mod = importlib.import_module("mainsequence.cli.api")
    recorded: dict[str, list] = {"run": [], "popen": [], "network": []}

    def _run(cmd, *args, **kwargs):
        cwd = kwargs.get("cwd")
        recorded["run"].append((list(cmd), pathlib.Path(cwd) if cwd else None))
        return subprocess.CompletedProcess(cmd, 0, "", "")

    def _popen(cmd, *args, **kwargs):
        recorded["popen"].append(list(cmd))
        raise OSError("sync must not start a process outside subprocess.run")

    def _network(*args, **kwargs):
        recorded["network"].append(args)
        raise OSError("sync must not use the network")

    monkeypatch.setattr(subprocess, "run", _run)
    monkeypatch.setattr(subprocess, "Popen", _popen)
    monkeypatch.setattr(requests.sessions.Session, "request", _network)
    monkeypatch.setattr(socket.socket, "connect", _network)
    monkeypatch.setattr(api_mod, "authed", _network)
    return recorded


def _write_installed_agent_scaffold_bundle(bundle_dir: pathlib.Path) -> pathlib.Path:
    bundle_dir.mkdir(parents=True, exist_ok=True)
    agents_md = bundle_dir / "AGENTS.md"
    agents_md.write_text(
        "\n".join(
            [
                "# AGENTS.md",
                "",
                "Installed scaffold preface.",
                "",
                "Do not remove the `<!-- mainsequence-agent-scaffold:start schema=1 "
                "source=agent_scaffold -->`",
                "or `<!-- mainsequence-agent-scaffold:end -->` markers.",
                "",
                "<!-- mainsequence-agent-scaffold:start schema=1 source=agent_scaffold -->",
                "## Main Sequence Instructions",
                "",
                "Installed managed block.",
                "",
                "<!-- mainsequence-agent-scaffold:end -->",
                "",
                "Installed scaffold footer.",
                "",
            ]
        ),
        encoding="utf-8",
    )
    return agents_md


def _cli_platform_skill_catalog():
    from pathlib import PurePosixPath

    from mainsequence.code_repository_skills import (
        PLATFORM_ONTOLOGY_URI,
        PLATFORM_SKILL_URI_PREFIX,
        PlatformCodeRepositoryResource,
        PlatformCodeRepositorySkill,
        PlatformCodeRepositorySkillCatalog,
    )

    ontology_content = '{"title":"Main Sequence","description":"Ontology"}\n'
    ontology = PlatformCodeRepositoryResource(
        name="ontology",
        uri=PLATFORM_ONTOLOGY_URI,
        resource_path=PurePosixPath("ontology/platform.json"),
        content=ontology_content,
        content_sha256=hashlib.sha256(ontology_content.encode("utf-8")).hexdigest(),
        content_mime_type="application/json",
        content_size=len(ontology_content.encode("utf-8")),
    )
    skills = []
    for name in (
        "a2a_communication",
        "code_repository_design",
        "code_repository_to_agent",
    ):
        front_matter_name = name.replace("_", "-")
        uri = f"{PLATFORM_SKILL_URI_PREFIX}{front_matter_name}"
        content = f"---\nname: {front_matter_name}\ndescription: Platform {name}\n---\n"
        skills.append(
            PlatformCodeRepositorySkill(
                name=name,
                uri=uri,
                resource_path=PurePosixPath(
                    "skills",
                    name,
                    "SKILL.md",
                ),
                relative_path=PurePosixPath(name, "SKILL.md"),
                content=content,
                content_sha256=hashlib.sha256(content.encode("utf-8")).hexdigest(),
                content_mime_type="text/markdown",
                content_size=len(content.encode("utf-8")),
            )
        )
    return PlatformCodeRepositorySkillCatalog(
        source_url="https://platform.example.test/mcp",
        manifest_version=2,
        manifest_sha256="a" * 64,
        ontology=ontology,
        skills=tuple(skills),
    )
