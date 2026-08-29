import time

import pytest

import mainsequence.client as msc


def test_create_code_repository():
    ds = msc.DataSource.filter(status=msc.DataSource.STATUS_AVAILABLE)[0]
    img = msc.CodeRepositoryBaseImage.filter()[0]
    org = msc.GitHubOrganization.filter()[0]

    code_repository = msc.CodeRepository.filter(id=124)

    # todo:loop unitl is_initialized == True
    code_repository = msc.CodeRepository.create(
        code_repository_name="demo-project-002",
        data_source=ds,  # <-- pydantic obj with .id
        default_base_image=img,  # <-- pydantic obj with .id (or None)
        github_org=org,  # <-- pydantic obj with .id (or None)
        repository_branch="main",
        env_vars={"FOO": "bar"},
    )
    print(code_repository)


def test_code_repository_time_index_table_updates():
    code_repository_branch = msc.CodeRepositoryBranch.filter()[0]

    updates = []
    poll_interval_s = 2
    timeout_s = 120
    deadline = time.time() + timeout_s

    while not updates and time.time() < deadline:
        updates = code_repository_branch.get_time_index_table_updates()
        if not updates:
            remaining = max(0, int(deadline - time.time()))
            print(
                f"No time-index table updates yet for project branch {code_repository_branch.uid}. "
                f"Retrying in {poll_interval_s}s (remaining: {remaining}s)..."
            )
            time.sleep(poll_interval_s)

    assert updates, (
        "No time-index table updates found for project branch "
        f"{code_repository_branch.uid} within {timeout_s}s."
    )

    for table_update in updates:
        print(table_update)


def test_code_repository_image_filter():
    images = msc.CodeRepositoryImage.filter()
    if not images:
        pytest.skip("No project images available for filter test.")

    image = images[0]
    code_repository_id = (
        image.related_code_repository.id if hasattr(image.related_code_repository, "id") else image.related_code_repository
    )
    repo_hash = image.code_repository_commit_hash

    filtered_by_code_repository = msc.CodeRepositoryImage.filter(related_code_repository__id__in=[code_repository_id])
    assert any(img.id == image.id for img in filtered_by_code_repository)

    filtered_by_hash = msc.CodeRepositoryImage.filter(code_repository_commit_hash=repo_hash)
    assert any(img.id == image.id for img in filtered_by_hash)

    filtered_by_hash_in = msc.CodeRepositoryImage.filter(code_repository_commit_hash__in=[repo_hash])
    assert any(img.id == image.id for img in filtered_by_hash_in)

    with pytest.raises(ValueError):
        msc.CodeRepositoryImage.filter(related_code_repository=code_repository_id)


def test_code_repository_resource_filter():
    resources = msc.CodeRepositoryResource.filter()
    if not resources:
        pytest.skip("No project resources available for filter test.")

    resource = next(
        (item for item in resources if item.id is not None and item.code_repository is not None),
        None,
    )
    if resource is None:
        pytest.skip("No project resource with id and project available for filter test.")

    code_repository_ref = resource.code_repository
    code_repository_id = code_repository_ref.id if hasattr(code_repository_ref, "id") else code_repository_ref

    filtered_by_code_repository = msc.CodeRepositoryResource.filter(code_repository__id=code_repository_id)
    assert any(item.id == resource.id for item in filtered_by_code_repository)

    filtered_by_id = msc.CodeRepositoryResource.filter(id=resource.id)
    assert any(item.id == resource.id for item in filtered_by_id)

    filtered_by_id_in = msc.CodeRepositoryResource.filter(id__in=[resource.id])
    assert any(item.id == resource.id for item in filtered_by_id_in)

    if resource.repo_commit_sha:
        filtered_by_repo_commit = msc.CodeRepositoryResource.filter(
            repo_commit_sha=resource.repo_commit_sha
        )
        assert any(item.id == resource.id for item in filtered_by_repo_commit)

    if resource.resource_type:
        filtered_by_resource_type = msc.CodeRepositoryResource.filter(resource_type=resource.resource_type)
        assert any(item.id == resource.id for item in filtered_by_resource_type)

    with pytest.raises(ValueError):
        msc.CodeRepositoryResource.filter(code_repository=code_repository_id)
