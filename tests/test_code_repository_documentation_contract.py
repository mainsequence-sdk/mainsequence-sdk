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
SKILL_ROOT = REPOSITORY_ROOT / "agent_scaffold" / "skills"
DOCUMENTATION_SITE_ROOT = "https://mainsequence-sdk.github.io/mainsequence-sdk"
# The wheel ships `agent_scaffold/skills` and no `docs/`, and the CLI copies those
# skills into a user's CodeRepository under `.agents/skills/<namespace>/`. There,
# `docs/` is that repository's own directory, so a skill that names an SDK
# documentation page by repository-relative path names a file the agent reading it
# cannot open. Skills cite the published documentation site instead.
SKILL_REPOSITORY_DOCUMENTATION_PATH = re.compile(r"`(docs/[A-Za-z0-9_./-]+\.md)`")
SKILL_DOCUMENTATION_URL = re.compile(re.escape(DOCUMENTATION_SITE_ROOT) + r"(/[A-Za-z0-9_./-]*)")
# Skills are copied into a CodeRepository under `.agents/skills/<namespace>/`, so
# a cross-skill reference maps back to a shipped `agent_scaffold/skills` path.
SKILL_CROSS_REFERENCE = re.compile(r"`\.agents/skills/[A-Za-z0-9_]+/([A-Za-z0-9_./-]+/SKILL\.md)`")
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


def _shipped_scaffold_documents() -> tuple[Path, ...]:
    """Every Markdown file the wheel ships under `agent_scaffold`.

    `AGENTS.md` ships beside the skills and several skills make it their first
    mandatory read, so it carries the same reference obligations they do.
    """

    return tuple(sorted((REPOSITORY_ROOT / "agent_scaffold").rglob("*.md")))


def _documentation_page_for_url_path(url_path: str) -> Path | None:
    """Resolve a documentation-site URL path to the `docs/` page that renders it.

    MkDocs serves directory URLs, so `/knowledge/meta_tables/migrations/` renders
    `docs/knowledge/meta_tables/migrations.md` and `/knowledge/meta_tables/`
    renders that directory's `index.md`. The transform is pure, so a skill's
    documentation links are checkable without reaching the network.
    """

    relative = url_path.strip("/")
    if not relative:
        return REPOSITORY_ROOT / "docs" / "index.md"
    candidates = (
        REPOSITORY_ROOT / "docs" / f"{relative}.md",
        REPOSITORY_ROOT / "docs" / relative / "index.md",
    )
    return next((candidate for candidate in candidates if candidate.is_file()), None)


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
        REPOSITORY_ROOT / "docs" / "adr" / "0031-process-lifetime-code-repository-branch-context.md"
    ).read_text(encoding="utf-8")

    assert "get_code_repository_context()" in adr
    assert "are the canonical public names" in adr
    assert "names below describe the pre-cutover contract" not in adr


def test_api_surface_skill_separates_deployed_and_local_readiness() -> None:
    skill = (
        REPOSITORY_ROOT
        / "agent_scaffold"
        / "skills"
        / "application_surfaces"
        / "api_surfaces"
        / "SKILL.md"
    ).read_text(encoding="utf-8")
    normalized_skill = " ".join(skill.split())

    assert "Request runtime access" in normalized_skill
    assert "through Django" in normalized_skill
    assert "wait for Django's ready result" in normalized_skill
    assert "wait for its lifespan startup to complete" in normalized_skill


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


@pytest.mark.parametrize(
    "path",
    _shipped_scaffold_documents(),
    ids=lambda path: str(path.relative_to(REPOSITORY_ROOT)),
)
def test_skills_do_not_cite_repository_relative_documentation_paths(path: Path) -> None:
    relative_path = path.relative_to(REPOSITORY_ROOT)
    text = path.read_text(encoding="utf-8")

    repository_paths = sorted(set(SKILL_REPOSITORY_DOCUMENTATION_PATH.findall(text)))

    assert not repository_paths, (
        f"{relative_path} cites SDK documentation by repository-relative path: "
        f"{repository_paths}. The wheel ships no `docs/`, so cite "
        f"{DOCUMENTATION_SITE_ROOT}/... instead."
    )


@pytest.mark.parametrize(
    "path",
    _shipped_scaffold_documents(),
    ids=lambda path: str(path.relative_to(REPOSITORY_ROOT)),
)
def test_skill_documentation_urls_render_an_existing_page(path: Path) -> None:
    relative_path = path.relative_to(REPOSITORY_ROOT)
    text = path.read_text(encoding="utf-8")

    unrendered = sorted(
        {
            url_path
            for url_path in SKILL_DOCUMENTATION_URL.findall(text)
            if _documentation_page_for_url_path(url_path) is None
        }
    )

    assert not unrendered, (
        f"{relative_path} links documentation-site paths that no `docs/` page "
        f"renders: {unrendered}"
    )


@pytest.mark.parametrize(
    "path",
    _shipped_scaffold_documents(),
    ids=lambda path: str(path.relative_to(REPOSITORY_ROOT)),
)
def test_skill_cross_references_resolve(path: Path) -> None:
    relative_path = path.relative_to(REPOSITORY_ROOT)
    text = path.read_text(encoding="utf-8")

    missing = sorted(
        {
            reference
            for reference in SKILL_CROSS_REFERENCE.findall(text)
            if not (SKILL_ROOT / reference).is_file()
        }
    )

    assert not missing, f"{relative_path} references skills that are not shipped: {missing}"
