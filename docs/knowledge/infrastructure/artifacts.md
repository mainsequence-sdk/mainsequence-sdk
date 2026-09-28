# Artifacts

An `Artifact` is a platform file with a stable bucket, name, content, and creation timestamp. Use it for source files, reports, model binaries, and other payloads whose natural unit is a file. Buckets and Artifacts belong to an Organization Environment; SDK operations use the shared [Environment context](context.md): the Environment of the current registered branch, with existing authenticated runtime target checks. Operations raise a missing-Environment error if the branch has no Environment; unrelated SDK operations remain available.

```python
from mainsequence.client import Artifact

artifact = Artifact.upload_file(
    filepath="/data/report.pdf",
    name="report.pdf",
    bucket_name="Reports",
)

same_artifact = Artifact.get(bucket__name="Reports", name="report.pdf")
```

`upload_file()` uses the platform's get-or-create behavior. The returned Artifact UID is more stable than a temporary local path. Applications can retrieve it from jobs or other platform resources. Domain-specific table parsing and publishing belong to the independent domain package.
