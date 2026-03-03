import mainsequence.client as msc

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