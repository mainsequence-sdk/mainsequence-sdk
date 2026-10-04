import pytest

import mainsequence.client as msc

# Every test in this module drives a real backend over the network and needs
# SDK credentials, so none of them can run in CI. See `[tool.pytest.ini_options]`
# in pyproject.toml: `pytest` deselects them, `pytest -m live` runs them.
pytestmark = pytest.mark.live


def test_code_repository_image_filter():
    images = msc.CodeRepositoryImage.filter()
    if not images:
        pytest.skip("No CodeRepository images available for filter test.")

    image = images[0]
    repo_hash = image.code_repository_commit_hash

    filtered_by_hash = msc.CodeRepositoryImage.filter(code_repository_commit_hash=repo_hash)
    assert any(img.uid == image.uid for img in filtered_by_hash)

    filtered_by_hash_in = msc.CodeRepositoryImage.filter(
        code_repository_commit_hash__in=[repo_hash]
    )
    assert any(img.uid == image.uid for img in filtered_by_hash_in)


def test_code_repository_resource_filter():
    resources = msc.CodeRepositoryResource.filter()
    if not resources:
        pytest.skip("No CodeRepository resources available for filter test.")

    resource = resources[0]
    filtered_by_uid = msc.CodeRepositoryResource.filter(uid=resource.uid)
    assert any(item.uid == resource.uid for item in filtered_by_uid)

    filtered_by_uid_in = msc.CodeRepositoryResource.filter(uid__in=[resource.uid])
    assert any(item.uid == resource.uid for item in filtered_by_uid_in)

    if resource.repo_commit_sha:
        filtered_by_repo_commit = msc.CodeRepositoryResource.filter(
            repo_commit_sha=resource.repo_commit_sha
        )
        assert any(item.uid == resource.uid for item in filtered_by_repo_commit)

    if resource.resource_type:
        filtered_by_resource_type = msc.CodeRepositoryResource.filter(
            resource_type=resource.resource_type
        )
        assert any(item.uid == resource.uid for item in filtered_by_resource_type)
