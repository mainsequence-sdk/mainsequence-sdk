# Part 5.2: Streamlit Integration II
**Deploy the tutorial fixed-income dashboard with the CLI**

!!! warning "IMPORTANT"
    Dashboard releases are created from project resources tied to a pushed repository commit.
    If your dashboard files are only local and not pushed, the CLI cannot create a release for them.

## Introduction

In Part 5.1, you built the tutorial dashboard under:

```text
dashboards/tutorial_fixed_income_dashboard/
```

In this chapter, you will deploy that dashboard using the current CLI workflow.

The deployment flow is:

1. sync the project so the dashboard files are committed and pushed
2. create a project image for that pushed commit
3. list the project resources discovered from that commit
4. create a Streamlit dashboard release from the dashboard `app.py`
5. verify the resulting release

---

## 1) Sync the project

From the project root:

```bash
cd /path/to/your/project
mainsequence project sync -m "Add tutorial fixed-income dashboard"
```

This is the easiest way to make sure the dashboard files are:

- committed
- pushed
- visible to the backend resource discovery flow

## 2) Create a project image

List existing images first:

```bash
mainsequence project images list
```

If no image exists for the pushed commit, create one:

```bash
mainsequence project images create
```

The image must be built from a pushed commit. The CLI will either prompt you to select a pushed commit or let you pass one explicitly.

## 3) List project resources

Once the commit is pushed, list the resources discovered from the current remote head:

```bash
mainsequence project project_resource list
```

Find the resource ids for:

- `dashboards/tutorial_fixed_income_dashboard/app.py`
- `dashboards/tutorial_fixed_income_dashboard/README.md`

You will also need the image id from the previous step.

## 4) Create the dashboard release

Create the dashboard release with:

```bash
mainsequence project project_resource create_dashboard \
  --related-image-id <IMAGE_ID> \
  --resource-id <APP_RESOURCE_ID> \
  --readme-resource-id <README_RESOURCE_ID>
```

This is the current CLI flow for dashboard deployment.

Behind the scenes:

- the selected image defines the runtime environment
- the selected resource defines which `app.py` is released
- the CLI filters eligible resources so they match the same repo commit as the selected image

## 5) Verify the deployment

The simplest verification flow is:

1. confirm the image exists with `mainsequence project images list`
2. confirm the dashboard resources exist with `mainsequence project project_resource list`
3. confirm `create_dashboard` returned a release id

If you want a direct SDK verification, you can query releases for the dashboard resource:

```bash
PYTHONPATH=/Users/jose/code/MainSequenceClientSide/mainsequence-sdk \
.venv/bin/python - <<'PY'
from mainsequence.client.models_helpers import ProjectResource, ResourceRelease

app_resource = ProjectResource.get(path="dashboards/tutorial_fixed_income_dashboard/app.py")
releases = ResourceRelease.filter(resource=app_resource.id)

for release in releases:
    print(
        {
            "id": release.id,
            "release_kind": release.release_kind,
            "subdomain": release.subdomain,
            "related_image": release.related_image,
        }
    )
PY
```

If the release exists and points to the image you selected, the deployment path is correct.

## 6) Why this deployment chapter is different from the older draft

This version is intentionally narrower and more accurate:

- it does not ask you to clone an external dashboard repository
- it does not rely on a UI-first deployment flow
- it uses the current image/resource/release CLI commands
- it keeps dashboard construction and dashboard deployment as separate steps

## 7) Troubleshooting

- `project_resource list` shows no dashboard files:
  The dashboard files are not part of the pushed commit the backend is inspecting yet.
- `images list` shows no image for the commit:
  Create one with `mainsequence project images create`.
- `create_dashboard` says no matching resources exist:
  The selected image commit and the selected resource commit do not match.
- The dashboard opens but shows no rows:
  Use the landing-page refresh button to rebuild the tutorial assets and simulated prices.

At this point, your tutorial project has a current Streamlit dashboard and a current CLI deployment path that matches the SDK behavior in use today.

For the reusable helper layer behind these dashboard chapters, see [Streamlit Helpers](../../../knowledge/dashboards/streamlit/index.md).
