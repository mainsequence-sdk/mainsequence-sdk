from __future__ import annotations

import re
from pathlib import Path

import pytest

REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
DOCUMENTATION_ROOTS = (
    REPOSITORY_ROOT / "docs",
    REPOSITORY_ROOT / "agent_scaffold",
)
HISTORICAL_IDENTIFIER_ALLOWLIST = {
    Path("CHANGELOG.md"): frozenset({"project-context", "runtime_project_context"}),
}
STREAMLIT_RECORD_PATHS = {
    Path("CHANGELOG.md"),
    Path("docs/knowledge/dashboards/streamlit/index.md"),
    Path("docs/migrations/streamlit-dashboard-removal.md"),
}
STREAMLIT_REMOVAL_LINK_PATHS = {
    Path("README.md"),
    Path("docs/SUMMARY.md"),
    Path("docs/index.md"),
}
RETIRED_PUBLIC_IDENTIFIERS = {
    "project_context": re.compile(r"(?<![A-Za-z0-9_])project_context(?![A-Za-z0-9_])"),
    "project-context": re.compile(r"(?<![A-Za-z0-9_])project-context(?![A-Za-z0-9_])"),
    "runtime_project_context": re.compile(
        r"(?<![A-Za-z0-9_])runtime_project_context(?![A-Za-z0-9_])"
    ),
    "get_project_runtime_context": re.compile(
        r"(?<![A-Za-z0-9_])get_project_runtime_context(?![A-Za-z0-9_])"
    ),
    "ProjectAlembicVersion": re.compile(r"\bProjectAlembicVersion\b"),
    "ProjectBranch": re.compile(r"\bProjectBranch\b"),
    "MAIN_SEQUENCE_PROJECT": re.compile(r"\bMAIN_SEQUENCE_PROJECT[A-Z0-9_]*\b"),
    "MAINSEQUENCE_PROJECT": re.compile(r"\bMAINSEQUENCE_PROJECT[A-Z0-9_]*\b"),
    "organization_project_environment": re.compile(
        r"\borganization_project_environment(?:_uid|_name)?\b"
    ),
    "project_to_agent": re.compile(r"\bproject_to_agent\b"),
    "project-design": re.compile(r"\bproject-design\b"),
    "project-code": re.compile(r"\bproject-code\b"),
    "project-facing": re.compile(r"\bproject-facing\b"),
}


def _documentation_files() -> tuple[Path, ...]:
    files = {REPOSITORY_ROOT / "README.md", REPOSITORY_ROOT / "CHANGELOG.md"}
    for root in DOCUMENTATION_ROOTS:
        files.update(root.rglob("*.md"))
    return tuple(sorted(files))


@pytest.mark.parametrize(
    "path",
    _documentation_files(),
    ids=lambda path: str(path.relative_to(REPOSITORY_ROOT)),
)
def test_current_documentation_excludes_retired_project_identifiers(path: Path) -> None:
    relative_path = path.relative_to(REPOSITORY_ROOT)
    allowed_identifiers = HISTORICAL_IDENTIFIER_ALLOWLIST.get(relative_path, frozenset())
    text = path.read_text(encoding="utf-8")

    unexpected = sorted(
        identifier
        for identifier, pattern in RETIRED_PUBLIC_IDENTIFIERS.items()
        if identifier not in allowed_identifiers and pattern.search(text)
    )

    assert not unexpected, f"{relative_path} contains retired public identifiers: {unexpected}"


def test_code_repository_context_adr_describes_the_current_cutover() -> None:
    adr = (
        REPOSITORY_ROOT
        / "docs"
        / "adr"
        / "0031-process-lifetime-code-repository-branch-context.md"
    ).read_text(encoding="utf-8")

    assert "get_code_repository_context()" in adr
    assert "are the canonical public names" in adr
    assert "names below describe the pre-cutover contract" not in adr


@pytest.mark.parametrize(
    "path",
    _documentation_files(),
    ids=lambda path: str(path.relative_to(REPOSITORY_ROOT)),
)
def test_current_documentation_does_not_advertise_streamlit_support(path: Path) -> None:
    relative_path = path.relative_to(REPOSITORY_ROOT)
    text = path.read_text(encoding="utf-8")

    if relative_path in STREAMLIT_RECORD_PATHS:
        return
    if relative_path in STREAMLIT_REMOVAL_LINK_PATHS:
        streamlit_lines = [line for line in text.splitlines() if "streamlit" in line.lower()]
        assert all("remov" in line.lower() for line in streamlit_lines)
        return

    assert "streamlit" not in text.lower()
    assert "create_dashboard" not in text
    assert "delete_dashboard" not in text
