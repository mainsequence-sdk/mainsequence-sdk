import pathlib
from types import SimpleNamespace

import pytest

import mainsequence.code_repository_context as code_repository_context
from tests.client.support import CODE_REPOSITORY_BRANCH_UID, CODE_REPOSITORY_UID, ENVIRONMENT_UID


@pytest.fixture(autouse=True)
def _resolved_code_repository_context(monkeypatch):
    code_repository_context._reset_code_repository_context()
    source = code_repository_context.GitCodeRepositorySourceContext(
        repository_root=pathlib.Path.cwd().resolve(),
        canonical_repository_identity="github.com/mainsequence-sdk/filters",
        repository_branch="main",
        repository_ref="refs/heads/main",
        commit_sha="a" * 40,
    )
    monkeypatch.setattr(
        code_repository_context,
        "_resolve_git_source_context",
        lambda code_repository_dir: source,
    )
    code_repository_context.get_code_repository_context(
        _code_repository_branch_context_loader=lambda resolved_source: SimpleNamespace(
            canonical_repository_identity=(resolved_source.canonical_repository_identity),
            repository_branch=resolved_source.repository_branch,
            repository_ref=resolved_source.repository_ref,
            commit_sha=resolved_source.commit_sha,
            code_repository_branch=SimpleNamespace(
                uid=CODE_REPOSITORY_BRANCH_UID,
                code_repository_uid=CODE_REPOSITORY_UID,
                repository_branch=resolved_source.repository_branch,
                organization_environment_uid=ENVIRONMENT_UID,
                metatables_data_source=None,
            ),
        ),
    )
    yield
    code_repository_context._reset_code_repository_context()
