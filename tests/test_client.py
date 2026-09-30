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
        pytest.skip("No CodeRepository resources available for filter test.")

    resource = next(
        (item for item in resources if item.id is not None and item.code_repository is not None),
        None,
    )
    if resource is None:
        pytest.skip("No CodeRepository resource with an ID and owner available for filter test.")

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
