from __future__ import annotations

from types import SimpleNamespace

import pytest
from pydantic import ValidationError

from mainsequence.cli import cli as cli_mod
from mainsequence.client import models_foundry

CODE_REPOSITORY_UID = "11111111-1111-4111-8111-111111111111"
CODE_REPOSITORY_BRANCH_UID = "81111111-1111-4111-8111-111111111111"
REPOSITORY_UID = "71111111-1111-4111-8111-111111111111"


def logical_code_repository_payload() -> dict:
    return {
        "uid": CODE_REPOSITORY_UID,
        "code_repository_name": "Analytics",
        "code_repository_type": "python",
        "primary_language": "python",
        "framework": "mainsequence",
        "github_repository_binding_uid": REPOSITORY_UID,
        "archived": False,
        "created_by": "owner@example.com",
        "labels": ["analytics"],
        "branches": [
            {"uid": CODE_REPOSITORY_BRANCH_UID, "repository_branch": "main"},
        ],
    }


def code_repository_branch_payload() -> dict:
    return {
        "uid": CODE_REPOSITORY_BRANCH_UID,
        "code_repository_uid": CODE_REPOSITORY_UID,
        "code_repository_name": "Analytics",
        "repository_branch": "main",
        "metatables_data_source": None,
        "metatables_data_source_uid": None,
        "default_base_image": {
            "uid": "61111111-1111-4111-8111-111111111111",
            "title": "Python",
            "description": "Python runtime",
        },
        "sdks": [],
        "github_repository_binding_uid": REPOSITORY_UID,
        "latest_git_version": "1.0.0",
        "is_initialized": True,
        "created_by": "owner@example.com",
    }


class FakeResponse:
    status_code = 200

    def __init__(self, payload: object):
        self._payload = payload
        self.content = b"{}"

    def json(self):
        return self._payload


def test_code_repository_is_only_the_logical_aggregate_contract():
    code_repository = models_foundry.CodeRepository(**logical_code_repository_payload())

    assert code_repository.uid == CODE_REPOSITORY_UID
    assert code_repository.code_repository_type == "python"
    assert code_repository.primary_language == "python"
    assert code_repository.framework == "mainsequence"
    assert code_repository.branches[0].uid == CODE_REPOSITORY_BRANCH_UID
    assert not hasattr(code_repository, "repository_branch")

    with pytest.raises(ValidationError):
        models_foundry.CodeRepository(
            **logical_code_repository_payload(),
            repository_branch="main",
        )


def test_code_repository_requires_canonical_type_and_derived_technology():
    for missing_field in ("code_repository_type", "primary_language", "framework"):
        payload = logical_code_repository_payload()
        payload.pop(missing_field)

        with pytest.raises(ValidationError):
            models_foundry.CodeRepository(**payload)


def test_code_repository_quick_search_requires_code_repository_type():
    result = models_foundry.CodeRepositoryQuickSearchResult(
        uid=CODE_REPOSITORY_UID,
        code_repository_name="Analytics",
        code_repository_type="python",
    )

    assert result.code_repository_type == "python"
    with pytest.raises(ValidationError):
        models_foundry.CodeRepositoryQuickSearchResult(
            uid=CODE_REPOSITORY_UID,
            code_repository_name="Analytics",
        )


def test_code_repository_branch_owns_branch_configuration():
    branch = models_foundry.CodeRepositoryBranch(**code_repository_branch_payload())

    assert branch.code_repository_uid == CODE_REPOSITORY_UID
    assert branch.repository_branch == "main"
    assert branch.default_base_image.uid == "61111111-1111-4111-8111-111111111111"
    assert branch.organization_environment_uid is None
    assert branch.organization_environment_name is None
    assert not hasattr(branch, "code_repository_type")
    assert not hasattr(branch, "primary_language")
    assert not hasattr(branch, "framework")
    assert not hasattr(branch, "git_ssh_url")
    assert not hasattr(branch, "labels")


def test_github_repository_binding_is_a_separate_registry_contract():
    repository = models_foundry.GitHubRepositoryBinding(
        uid=REPOSITORY_UID,
        code_repository_uid=CODE_REPOSITORY_UID,
        code_repository_name="Analytics",
        repo_name="analytics",
        github_full_name="mainsequence/analytics",
        git_ssh_url="git@github.com:mainsequence/analytics.git",
        git_repo_url="https://github.com/mainsequence/analytics.git",
        created_by="owner@example.com",
    )

    assert repository.code_repository_uid == CODE_REPOSITORY_UID
    assert repository.uid == REPOSITORY_UID


def test_code_repository_create_sends_only_canonical_fields(monkeypatch):
    captured = {}

    def fake_make_request(**kwargs):
        captured.update(kwargs)
        response = FakeResponse(logical_code_repository_payload())
        response.status_code = 201
        return response

    monkeypatch.setattr(
        models_foundry.CodeRepository,
        "get_object_url",
        classmethod(lambda cls: "https://api/code-repositories"),
    )
    monkeypatch.setattr(
        models_foundry.CodeRepository,
        "build_session",
        classmethod(lambda cls: object()),
    )
    monkeypatch.setattr(models_foundry, "make_request", fake_make_request)

    created = models_foundry.CodeRepository.create(
        code_repository_name="Analytics",
        code_repository_type="python",
        default_base_image_uid="61111111-1111-4111-8111-111111111111",
        github_org_uid="41111111-1111-4111-8111-111111111111",
        env_vars={"FOO": "bar"},
        labels=["analytics"],
    )

    assert created.uid == CODE_REPOSITORY_UID
    assert captured["payload"] == {
        "json": {
            "code_repository_name": "Analytics",
            "code_repository_type": "python",
            "default_base_image_uid": "61111111-1111-4111-8111-111111111111",
            "github_org_uid": "41111111-1111-4111-8111-111111111111",
            "env_vars": [{"name": "FOO", "value": "bar"}],
            "labels": ["analytics"],
        }
    }


def test_code_repository_bulk_delete_uses_selection_and_options(monkeypatch):
    captured = {}

    def fake_make_request(**kwargs):
        captured.update(kwargs)
        return FakeResponse({"detail": "Projects deleted.", "matched_count": 1, "deleted_count": 1})

    monkeypatch.setattr(
        models_foundry.CodeRepository,
        "get_object_url",
        classmethod(lambda cls: "https://api/code-repositories"),
    )
    monkeypatch.setattr(
        models_foundry.CodeRepository,
        "build_session",
        classmethod(lambda cls: object()),
    )
    monkeypatch.setattr(models_foundry, "make_request", fake_make_request)

    result = models_foundry.CodeRepository.bulk_delete(
        uids=[CODE_REPOSITORY_UID],
        delete_repositories=True,
    )

    assert result.deleted_count == 1
    assert captured["payload"] == {
        "json": {
            "selection": {"mode": "explicit", "uids": [CODE_REPOSITORY_UID]},
            "options": {"delete_repositories": True},
        }
    }


def test_code_repository_add_deploy_key_uses_code_repository_route(monkeypatch):
    captured = {}
    code_repository = models_foundry.CodeRepository(**logical_code_repository_payload())

    def fake_make_request(**kwargs):
        captured.update(kwargs)
        return FakeResponse(None)

    monkeypatch.setattr(
        models_foundry.CodeRepository,
        "get_object_url",
        classmethod(lambda cls: "https://api/code-repositories"),
    )
    monkeypatch.setattr(
        models_foundry.CodeRepository,
        "build_session",
        classmethod(lambda cls: object()),
    )
    monkeypatch.setattr(models_foundry, "make_request", fake_make_request)

    code_repository.add_deploy_key(key_title="workstation", public_key="ssh-ed25519 AAA test")

    assert captured["r_type"] == "POST"
    assert captured["url"] == f"https://api/code-repositories/{CODE_REPOSITORY_UID}/add-deploy-key/"
    assert captured["payload"] == {
        "json": {"key_title": "workstation", "public_key": "ssh-ed25519 AAA test"}
    }
    assert not hasattr(models_foundry.CodeRepositoryBranch, "add_deploy_key")


def test_code_repository_branch_summary_uses_branch_route(monkeypatch):
    captured = {}
    branch = models_foundry.CodeRepositoryBranch(**code_repository_branch_payload())

    def fake_make_request(**kwargs):
        captured.update(kwargs)
        return FakeResponse({"uid": CODE_REPOSITORY_BRANCH_UID})

    monkeypatch.setattr(
        models_foundry.CodeRepositoryBranch,
        "get_object_url",
        classmethod(lambda cls: "https://api/code-repository-branches"),
    )
    monkeypatch.setattr(
        models_foundry.CodeRepositoryBranch,
        "build_session",
        classmethod(lambda cls: object()),
    )
    monkeypatch.setattr(models_foundry, "make_request", fake_make_request)

    assert branch.summary() == {"uid": CODE_REPOSITORY_BRANCH_UID}
    assert captured["url"] == (f"https://api/code-repository-branches/{CODE_REPOSITORY_BRANCH_UID}/summary/")


def test_local_branch_resolution_does_not_infer_the_only_branch(monkeypatch):
    monkeypatch.setattr(
        cli_mod,
        "get_code_repository_branch",
        lambda uid: SimpleNamespace(uid=uid),
    )

    with pytest.raises(cli_mod.ApiError, match="No repository branch was selected"):
        cli_mod._resolve_code_repository_branch(
            logical_code_repository_payload(),
            prompt_if_ambiguous=False,
        )
