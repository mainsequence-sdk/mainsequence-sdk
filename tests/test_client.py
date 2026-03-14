import time

import pytest

import mainsequence.client as msc


def test_create_project():

    ds = msc.DynamicTableDataSource.filter(related_resource__status=msc.DataSource.STATUS_AVAILABLE)[0]
    img = msc.ProjectBaseImage.filter()[0]
    org = msc.GithubOrganization.filter()[0]


    project=msc.Project.filter(id=124)

    #todo:loop unitl is_initialized == True
    project = msc.Project.create(
        project_name="demo-project-002",
        data_source=ds,                 # <-- pydantic obj with .id
        default_base_image=img,         # <-- pydantic obj with .id (or None)
        github_org=org,                 # <-- pydantic obj with .id (or None)
        repository_branch="main",
        env_vars={"FOO": "bar"},
    )
    print(project)

def test_agent():

    agent=msc.Agent.filter()
    agent=agent[0]


    agent.query(prompt="Test")


def test_project_data_nodes_updates():
    project = msc.Project.filter()[0]

    updates = []
    poll_interval_s = 2
    timeout_s = 120
    deadline = time.time() + timeout_s

    while not updates and time.time() < deadline:
        updates = project.get_data_nodes_updates()
        if not updates:
            remaining = max(0, int(deadline - time.time()))
            print(
                f"No data node updates yet for project {project.id}. "
                f"Retrying in {poll_interval_s}s (remaining: {remaining}s)..."
            )
            time.sleep(poll_interval_s)

    assert updates, f"No data node updates found for project {project.id} within {timeout_s}s."

    for data_node_update in updates:
        print(data_node_update)


def test_project_image_filter():
    images = msc.ProjectImage.filter()
    if not images:
        pytest.skip("No project images available for filter test.")

    image = images[0]
    project_id = image.related_project.id if hasattr(image.related_project, "id") else image.related_project
    repo_hash = image.project_repo_hash

    filtered_by_project = msc.ProjectImage.filter(related_project__id__in=[project_id])
    assert any(img.id == image.id for img in filtered_by_project)

    filtered_by_hash = msc.ProjectImage.filter(project_repo_hash=repo_hash)
    assert any(img.id == image.id for img in filtered_by_hash)

    filtered_by_hash_in = msc.ProjectImage.filter(project_repo_hash__in=[repo_hash])
    assert any(img.id == image.id for img in filtered_by_hash_in)

    with pytest.raises(ValueError):
        msc.ProjectImage.filter(related_project=project_id)


test_project_image_filter()
# users=msc.User.filter()
#
# msc.User.get(id=users[0].id,serializer="full")
#
# a=5
