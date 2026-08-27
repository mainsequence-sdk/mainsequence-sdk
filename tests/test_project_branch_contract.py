from __future__ import annotations

from types import SimpleNamespace

import pytest
from pydantic import ValidationError

from mainsequence.cli import cli as cli_mod
from mainsequence.client import models_foundry

PROJECT_UID = "11111111-1111-4111-8111-111111111111"
PROJECT_BRANCH_UID = "81111111-1111-4111-8111-111111111111"
REPOSITORY_UID = "71111111-1111-4111-8111-111111111111"


def logical_project_payload() -> dict:
    return {
        "uid": PROJECT_UID,
        "project_name": "Analytics",
        "project_type": "python",
        "primary_language": "python",
        "framework": "mainsequence",
        "git_repository_uid": REPOSITORY_UID,
        "archived": False,
        "created_by": "owner@example.com",
        "labels": ["analytics"],
        "branches": [
            {"uid": PROJECT_BRANCH_UID, "repository_branch": "main"},
        ],
    }


def project_branch_payload() -> dict:
    return {
        "uid": PROJECT_BRANCH_UID,
        "project_uid": PROJECT_UID,
        "project_name": "Analytics",
        "repository_branch": "main",
        "metatables_data_source": None,
        "metatables_data_source_uid": None,
        "default_base_image": {
            "uid": "61111111-1111-4111-8111-111111111111",
            "title": "Python",
            "description": "Python runtime",
        },
        "sdks": [],
        "git_repository_uid": REPOSITORY_UID,
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


def test_project_is_only_the_logical_aggregate_contract():
    project = models_foundry.Project(**logical_project_payload())

    assert project.uid == PROJECT_UID
    assert project.project_type == "python"
    assert project.primary_language == "python"
    assert project.framework == "mainsequence"
    assert project.branches[0].uid == PROJECT_BRANCH_UID
    assert not hasattr(project, "repository_branch")

    with pytest.raises(ValidationError):
        models_foundry.Project(
            **logical_project_payload(),
            repository_branch="main",
        )


def test_project_requires_canonical_type_and_derived_technology():
    for missing_field in ("project_type", "primary_language", "framework"):
        payload = logical_project_payload()
        payload.pop(missing_field)

        with pytest.raises(ValidationError):
            models_foundry.Project(**payload)


def test_project_quick_search_requires_project_type():
    result = models_foundry.ProjectQuickSearchResult(
        uid=PROJECT_UID,
        project_name="Analytics",
        project_type="python",
    )

    assert result.project_type == "python"
    with pytest.raises(ValidationError):
        models_foundry.ProjectQuickSearchResult(
            uid=PROJECT_UID,
            project_name="Analytics",
        )


def test_project_branch_owns_branch_configuration():
    branch = models_foundry.ProjectBranch(**project_branch_payload())

    assert branch.project_uid == PROJECT_UID
    assert branch.repository_branch == "main"
    assert branch.default_base_image.uid == "61111111-1111-4111-8111-111111111111"
    assert branch.organization_environment_uid is None
    assert branch.organization_environment_name is None
    assert not hasattr(branch, "project_type")
    assert not hasattr(branch, "primary_language")
    assert not hasattr(branch, "framework")
    assert not hasattr(branch, "git_ssh_url")
    assert not hasattr(branch, "labels")


def test_git_repository_is_a_separate_registry_contract():
    repository = models_foundry.GitRepository(
        uid=REPOSITORY_UID,
        project_uid=PROJECT_UID,
        project_name="Analytics",
        repo_name="analytics",
        github_full_name="mainsequence/analytics",
        git_ssh_url="git@github.com:mainsequence/analytics.git",
        git_repo_url="https://github.com/mainsequence/analytics.git",
        created_by="owner@example.com",
    )

    assert repository.project_uid == PROJECT_UID
    assert repository.uid == REPOSITORY_UID


def test_project_create_sends_only_canonical_fields(monkeypatch):
    captured = {}

    def fake_make_request(**kwargs):
        captured.update(kwargs)
        response = FakeResponse(logical_project_payload())
        response.status_code = 201
        return response

    monkeypatch.setattr(
        models_foundry.Project,
        "get_object_url",
        classmethod(lambda cls: "https://api/projects"),
    )
    monkeypatch.setattr(
        models_foundry.Project,
        "build_session",
        classmethod(lambda cls: object()),
    )
    monkeypatch.setattr(models_foundry, "make_request", fake_make_request)

    created = models_foundry.Project.create(
        project_name="Analytics",
        project_type="python",
        default_base_image_uid="61111111-1111-4111-8111-111111111111",
        github_org_uid="41111111-1111-4111-8111-111111111111",
        env_vars={"FOO": "bar"},
        labels=["analytics"],
    )

    assert created.uid == PROJECT_UID
    assert captured["payload"] == {
        "json": {
            "project_name": "Analytics",
            "project_type": "python",
            "default_base_image_uid": "61111111-1111-4111-8111-111111111111",
            "github_org_uid": "41111111-1111-4111-8111-111111111111",
            "env_vars": [{"name": "FOO", "value": "bar"}],
            "labels": ["analytics"],
        }
    }


def test_project_bulk_delete_uses_selection_and_options(monkeypatch):
    captured = {}

    def fake_make_request(**kwargs):
        captured.update(kwargs)
        return FakeResponse({"detail": "Projects deleted.", "matched_count": 1, "deleted_count": 1})

    monkeypatch.setattr(
        models_foundry.Project,
        "get_object_url",
        classmethod(lambda cls: "https://api/projects"),
    )
    monkeypatch.setattr(
        models_foundry.Project,
        "build_session",
        classmethod(lambda cls: object()),
    )
    monkeypatch.setattr(models_foundry, "make_request", fake_make_request)

    result = models_foundry.Project.bulk_delete(
        uids=[PROJECT_UID],
        delete_repositories=True,
    )

    assert result.deleted_count == 1
    assert captured["payload"] == {
        "json": {
            "selection": {"mode": "explicit", "uids": [PROJECT_UID]},
            "options": {"delete_repositories": True},
        }
    }


def test_project_add_deploy_key_uses_project_route(monkeypatch):
    captured = {}
    project = models_foundry.Project(**logical_project_payload())

    def fake_make_request(**kwargs):
        captured.update(kwargs)
        return FakeResponse(None)

    monkeypatch.setattr(
        models_foundry.Project,
        "get_object_url",
        classmethod(lambda cls: "https://api/projects"),
    )
    monkeypatch.setattr(
        models_foundry.Project,
        "build_session",
        classmethod(lambda cls: object()),
    )
    monkeypatch.setattr(models_foundry, "make_request", fake_make_request)

    project.add_deploy_key(key_title="workstation", public_key="ssh-ed25519 AAA test")

    assert captured["r_type"] == "POST"
    assert captured["url"] == f"https://api/projects/{PROJECT_UID}/add-deploy-key/"
    assert captured["payload"] == {
        "json": {"key_title": "workstation", "public_key": "ssh-ed25519 AAA test"}
    }
    assert not hasattr(models_foundry.ProjectBranch, "add_deploy_key")


def test_project_branch_summary_uses_branch_route(monkeypatch):
    captured = {}
    branch = models_foundry.ProjectBranch(**project_branch_payload())

    def fake_make_request(**kwargs):
        captured.update(kwargs)
        return FakeResponse({"uid": PROJECT_BRANCH_UID})

    monkeypatch.setattr(
        models_foundry.ProjectBranch,
        "get_object_url",
        classmethod(lambda cls: "https://api/project-branches"),
    )
    monkeypatch.setattr(
        models_foundry.ProjectBranch,
        "build_session",
        classmethod(lambda cls: object()),
    )
    monkeypatch.setattr(models_foundry, "make_request", fake_make_request)

    assert branch.summary() == {"uid": PROJECT_BRANCH_UID}
    assert captured["url"] == (f"https://api/project-branches/{PROJECT_BRANCH_UID}/summary/")


def test_local_branch_resolution_does_not_infer_the_only_branch(monkeypatch):
    monkeypatch.setattr(
        cli_mod,
        "get_project_branch",
        lambda uid: SimpleNamespace(uid=uid),
    )

    with pytest.raises(cli_mod.ApiError, match="No repository branch was selected"):
        cli_mod._resolve_project_branch(
            logical_project_payload(),
            prompt_if_ambiguous=False,
        )
