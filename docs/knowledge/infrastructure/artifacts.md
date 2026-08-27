# Artifacts

Part 4 of the tutorial introduces jobs, schedules, and project images. This page explains another infrastructure concept that often appears right after that: **Artifacts**.

An `Artifact` is the platform's file-storage primitive. A `TimeIndexTableUpdater`
can produce a structured `TimeIndexMetaTable`; an `Artifact` stores and retrieves
files such as spreadsheets, CSV drops, reports, model binaries, or other payloads
that do not naturally start as a table.

## Quick Summary

In this guide, you will:

- understand what an `Artifact` is and when to use it
- upload files into platform buckets from the Python client
- retrieve those files later from jobs, `TimeIndexTableUpdater`s, or dashboards
- avoid common mistakes around duplicate uploads and fragile local paths

## Mental model

Think of an Artifact as a file with a stable platform identity:

- a **bucket**
- a **name**
- the **content**
- a **creation timestamp**

That gives you a reference that survives beyond your laptop or a temporary network share.

In practical terms:

- local path: where the file happens to live today
- Artifact: how the platform knows that file tomorrow

Buckets and Artifacts are owned by an Organization Environment. The SDK derives
that Environment from the process-frozen current Git branch and registered
`ProjectBranch`; callers do not provide an Environment UID or branch UID.

## When Artifacts are the right tool

Artifacts are a good fit when the natural unit of work is still a file.

Common examples:

- a vendor drops daily `.csv` or `.xls` files into a folder
- a job produces a PDF, HTML report, or model pickle
- a workflow needs a manually curated spreadsheet as input
- a downstream `TimeIndexTableUpdater` needs to normalize a raw file before publishing a clean table

## When a published time-index table is the better tool

Use a `TimeIndexTableUpdater` to produce a registered time-index table once the
data should become a stable, queryable dataset with a schema, metadata, and
incremental updates.

That distinction matters:

- Artifact: raw file or binary payload
- `TimeIndexMetaTable`: structured table meant for downstream reading
- `TimeIndexTableUpdater`: executable behavior that maintains that table

!!! tip "A practical rule"
    If downstream users want to read rows and columns, you usually want a
    published time-index table. If they need the original file, you usually want
    an `Artifact`.

## Basic lifecycle

Most Artifact workflows follow the same path:

1. Upload a file into a named bucket.
2. Retrieve it later by `bucket` and `name`.
3. Parse it inside a job, `TimeIndexTableUpdater`, or dashboard.
4. If the content becomes part of your analytical layer, use a `TimeIndexTableUpdater` to normalize it into a registered output table.

## Uploading files

The public client import is:

```python
from mainsequence.client import Artifact
```

Here is a clean version of the "vector file" upload flow:

```python
import os
from pathlib import Path

from tqdm import tqdm

from mainsequence.client import Artifact
from mainsequence.logconf import logger

BUCKET_NAME = "Vector de precios"
upload_path = Path(os.environ["VECTOR_UPLOAD_PATH"])

vector_files = sorted(
    path
    for path in upload_path.iterdir()
    if path.name.startswith("VectorAnalitico") and path.suffix.lower() == ".xls"
)

logger.info("Uploading %s vectors...", len(vector_files))

for path in tqdm(vector_files):
    artifact = Artifact.upload_file(
        filepath=str(path),
        name=path.name,
        bucket_name=BUCKET_NAME,
    )
    logger.info("Artifact available: %s (uid=%s)", path.name, artifact.uid)
```

### Why this version is simpler

- it uses `os.environ`, not `os.env`
- it treats the upload folder as a `Path`
- it lets `Artifact.upload_file()` handle the get-or-create behavior

The SDK implementation of `upload_file()` already delegates to a `get_or_create` endpoint, so you usually do not need a separate existence check first.

## If you want an explicit "skip if present" flow

Sometimes you still want the explicit check because you want custom logging or different behavior when the file already exists.

In that case, make sure you actually skip the upload:

```python
existing = Artifact.filter(name=path.name, bucket__name=BUCKET_NAME)
if existing:
    logger.info("Vector %s already uploaded", path.name)
    continue
```

Without the `continue`, the code will log "already uploaded" and then upload anyway.

## Reading an Artifact back

Once the file is in the platform, downstream code should stop depending on the original local path.

Read it back by bucket and name:

```python
import pandas as pd

from mainsequence.client import Artifact

source_artifact = Artifact.get(
    bucket__name="Vector de precios",
    name="VectorAnalitico_2026_03_15.xls",
)

vector_df = pd.read_excel(source_artifact.content)
```

That is the important shift: the file is now referenced through the platform, not through a laptop folder.

## Reading Artifacts inside a TimeIndexTableUpdater

This is a common pattern when an upstream system gives you files, but your downstream platform users need a proper table.

```python
import pandas as pd

from mainsequence.client import Artifact
from mainsequence.meta_tables import (
    TimeIndexTableUpdater,
    TimeIndexTableUpdateConfig,
    PlatformTimeIndexMetaTable,
)


class ExternalPricesConfig(TimeIndexTableUpdateConfig):
    artifact_name: str
    bucket_name: str


class ExternalPrices(TimeIndexTableUpdater):
    def __init__(
        self,
        config: ExternalPricesConfig,
        output_table: type[PlatformTimeIndexMetaTable],
    ):
        self.artifact_name = config.artifact_name
        self.bucket_name = config.bucket_name
        super().__init__(config=config, output_table=output_table)

    def update(self):
        source_artifact = Artifact.get(
            bucket__name=self.bucket_name,
            name=self.artifact_name,
        )
        prices_source = pd.read_csv(source_artifact.content)
        return prices_source
```

Use Alembic for storage schema migrations. The SDK storage class can remain a
normal `PlatformTimeIndexMetaTable`; migration execution is handled by Alembic
SQL, not an SDK schema-migration subclass.

This is one of the cleanest ways to bridge external operational files into the `TimeIndexTableUpdater` layer.

## How Artifacts fit with jobs

Artifacts and jobs work naturally together:

- a manual job can upload a new vendor file drop
- a scheduled job can refresh the bucket every day
- a downstream `TimeIndexTableUpdater` can read the Artifact and publish a normalized table
- a dashboard can read a generated report Artifact

This is why Artifacts belong in the infrastructure layer of the docs. They are not just a client convenience; they are part of how files move through the platform.

## Buckets and naming

Use names that will still make sense later.

### Bucket names

A bucket should usually describe a domain or workflow, not a person.

Good examples:

- `vector_de_precios`
- `vendor_prices`
- `model_artifacts`
- `dashboard_exports`

### Artifact names

The Artifact name should identify the file clearly.

Good patterns:

- `VectorAnalitico_2026_03_15.xls`
- `daily_positions_2026_03_15.csv`
- `stress_report_2026_03_15.html`

### created_by_resource_name

Use a stable producer label so you can see what created the file:

- `vector-upload-script`
- `nightly-prices-job`
- `stress-dashboard`

## Common mistakes

### Using the local file path as the long-term identifier

That works only on one machine. The Artifact reference is the durable identity.

### Using Artifacts for data that should really be a published table

Artifacts are good raw inputs and outputs. They are not a replacement for
structured tables or the updater behavior that maintains them.

### Forgetting that `upload_file()` already uses get-or-create

If you need explicit pre-checks, do them intentionally. Otherwise, keep the upload path simple.

### Logging "already uploaded" but still uploading

If you use `Artifact.filter(...)` first, add `continue` when you want to skip.

## How Artifacts fit the workflow

Artifacts usually become relevant when infrastructure concepts start to matter:

- first you build `TimeIndexTableUpdater`s
- then you learn how code runs as jobs
- then you learn how files can move through the same platform

That keeps file movement aligned with the same Job and consumer lifecycle.

## Related Reading

- [Scheduling Jobs](./scheduling_jobs.md)
- [Time-Index Table Updaters](../time_index_table_updates.md)
