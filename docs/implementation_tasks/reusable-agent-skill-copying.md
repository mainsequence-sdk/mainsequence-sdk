# Implementation Task: Reusable Agent Skill Copying And Pin Sentinel

Date: 2026-06-14

## Context

`mainsequence project update_agent_skills` currently owns one hard-coded copy
flow:

1. resolve the target project;
2. resolve that project's installed `agent_scaffold/skills/` bundle from its
   `.venv`;
3. copy each top-level scaffold skill folder into
   `.agents/skills/mainsequence/`;
4. overwrite matching Main Sequence-managed skill folders;
5. leave project-owned top-level skills under `.agents/skills/` untouched.

That works for the SDK itself, but it is not reusable by extension libraries.
For example, `ms-markets` should be able to ship its own packaged skills and
offer a command that copies them into a project without reimplementing the copy
rules or guessing where project agent skills live.

There is also no project-local record of which installed library version last
copied the managed skill folder. After a project updates its SDK or an extension
library, the project cannot tell whether `.agents/skills/mainsequence/` or
`.agents/skills/ms_markets/` is current.

The existing `ms-markets` command already solves part of this problem locally in
`msm copy-msm-skills`. Its important behaviors should inform the reusable SDK
helper:

- copy packaged skills into `.agents/skills/ms_markets/`;
- support `--dry-run` and `--json`;
- skip hidden and `__*` folders;
- block execution when run inside the `ms-markets` source checkout;
- block execution when the destination skill namespace resolves to the packaged
  skill source, because overwriting the destination would delete the source
  skills.

## Target Design

Move the skill-copy machinery into a reusable SDK function that extension
libraries can import.

The function should copy a library-owned skill bundle into a library-owned
namespace under the project `.agents/skills/` directory and write a sentinel file
inside that namespace recording the pinned source version.

The sentinel is mandatory. Every successful non-dry-run copy must write
`PINNED_FROM.txt`, and its pinned version must come from the installed library
version that supplied the skill bundle.

The function must protect extension libraries from copying over their own
packaged skill source. This is not optional. A library command must fail before
copying when the target project is the library source checkout or when the
destination namespace overlaps the source skill bundle.

Main Sequence should use the same function for:

```bash
mainsequence project update_agent_skills --path .
```

An extension library should be able to build its own command around the same
function, for example:

```bash
msm copy-msm-skills --path .
```

or:

```python
from mainsequence.agent_skills import copy_agent_skills

copy_agent_skills(
    project_dir=project_dir,
    library_name="ms-markets",
    skills_path=installed_ms_markets_skills_path,
    pinned_version=installed_ms_markets_version,
    command="msm copy-msm-skills",
)
```

## Reusable API Shape

Add a small importable module outside the Typer CLI module so downstream
libraries do not need to import `mainsequence.cli.cli`.

Proposed module:

```text
mainsequence/agent_skills.py
```

Proposed public API:

```python
from __future__ import annotations

from collections.abc import Callable, Sequence
from dataclasses import dataclass
from pathlib import Path


class AgentSkillCopyBlocked(RuntimeError):
    """Raised when copying would target the library's own skill source."""


@dataclass(frozen=True)
class CopiedAgentSkill:
    name: str
    source: Path
    destination: Path


@dataclass(frozen=True)
class AgentSkillCopyResult:
    library_name: str
    namespace: str
    project_dir: Path
    skills_path: Path
    destination_root: Path
    sentinel_path: Path
    pinned_version: str
    dry_run: bool
    copied: list[CopiedAgentSkill]


def copy_agent_skills(
    *,
    project_dir: Path,
    library_name: str,
    skills_path: Path,
    pinned_version: str,
    command: str | None = None,
    namespace: str | None = None,
    dry_run: bool = False,
    protected_project_roots: Sequence[Path] = (),
    project_guard: Callable[[Path], str | None] | None = None,
) -> AgentSkillCopyResult:
    ...
```

Behavior:

- `library_name` is the human/package name used in the sentinel.
- `pinned_version` is required and must be the installed version of
  `library_name` or of the package that owns `skills_path`.
- `namespace` defaults to a normalized library namespace:
  - lowercase;
  - replace `-` and `.` with `_`;
  - allow only ASCII letters, numbers, and `_`;
  - reject empty or path-like names.
- `skills_path` points to a directory containing top-level skill folders.
- The destination root is:

  ```text
  <project_dir>/.agents/skills/<namespace>/
  ```

- For every immediate child directory of `skills_path`:
  - skip files;
  - skip names starting with `.` or `__`;
  - copy the directory to the destination namespace;
  - overwrite only the matching destination folder.
- Do not delete unrelated folders in the destination namespace unless they are
  overwritten by a copied source folder.
- Do not touch project-owned skills outside the destination namespace.
- When `dry_run=True`, return the same copy plan without writing files.
- Write the sentinel after all non-dry-run copies succeed.
- Fail before copying if the caller cannot resolve a non-empty library version.

## Self-Copy Protection

The reusable helper must include generic path-overlap protections:

- resolve `skills_path` and `destination_root`;
- block if `destination_root` is the same path as `skills_path`;
- block if `destination_root` is inside `skills_path`;
- block if `skills_path` is inside `destination_root`;
- block if any individual skill destination overlaps the corresponding source
  skill directory in either direction.

These checks prevent the destructive case where the helper removes a destination
folder that is also the packaged source folder.

Generic path checks are not sufficient for editable/source-tree installs. A
library may intentionally keep its packaged source skills at:

```text
<library-checkout>/.agents/skills/<namespace>/
```

If a user runs the copy command from that library checkout, the destination can
be the same as the source or close enough to make the copy destructive. The
helper should therefore also support library-owned guards:

- `protected_project_roots`: resolved project roots that are never valid copy
  targets;
- `project_guard`: optional callback that returns a block reason when a project
  directory is recognized as the library's own source checkout.

For `ms-markets`, the caller would pass the source checkout root and a guard
equivalent to its current `_is_ms_markets_source_checkout(...)` check. The guard
should detect the library's own `pyproject.toml`, expected source package
directory, and existing managed skill namespace.

The result should make blocked copies explicit in CLI code. A command may either
let `AgentSkillCopyBlocked` fail the command or convert the block reason into a
JSON payload like the existing `msm copy-msm-skills` command.

## Pin Sentinel

Each managed library namespace gets its own sentinel:

```text
.agents/skills/<namespace>/PINNED_FROM.txt
```

Example for Main Sequence:

```text
schema=1
library_name=mainsequence
namespace=mainsequence
pinned_version=4.4.3
skills_path=/project/.venv/lib/python3.12/site-packages/agent_scaffold/skills
copied_at_utc=2026-06-14T12:34:56Z
command=mainsequence project update_agent_skills
```

Example for `ms-markets`:

```text
schema=1
library_name=ms-markets
namespace=ms_markets
pinned_version=0.8.1
skills_path=/project/.venv/lib/python3.12/site-packages/ms_markets/agent_skills
copied_at_utc=2026-06-14T12:34:56Z
command=msm copy-msm-skills
```

The sentinel is intentionally plain text so humans and simple tools can inspect
it without a parser. The SDK can add a parser later if project-health checks
need structured validation.

## Main Sequence CLI Integration

Refactor `project_update_agent_skills` so it:

1. resolves `project_dir` as today;
2. resolves the target project's installed `agent_scaffold` bundle as today;
3. resolves the target project's installed `mainsequence` version from the same
   `.venv`;
4. calls `copy_agent_skills(...)` with:

   ```python
   copy_agent_skills(
       project_dir=project_dir,
       library_name="mainsequence",
       namespace="mainsequence",
       skills_path=skills_dir,
       pinned_version=resolved_project_mainsequence_version,
       command="mainsequence project update_agent_skills",
       protected_project_roots=(sdk_source_checkout_root_if_known,),
   )
   ```

5. includes the sentinel path and pinned version in normal output and JSON
   output.

The version must come from the target project's environment, not from the CLI
process, because the copied skill bundle is loaded from the target project
`.venv`.

If version resolution fails, the command should fail before copying. A copied
skill namespace without a versioned sentinel is invalid.

## Extension Library Usage

Extension libraries should resolve their own installed skill path and version,
then call the SDK helper.

For `ms-markets`, the CLI would roughly do:

```python
from importlib import resources
from importlib.metadata import version

from mainsequence.agent_skills import copy_agent_skills


skills_path = resources.files("ms_markets").joinpath("agent_skills")
result = copy_agent_skills(
    project_dir=project_dir,
    library_name="ms-markets",
    namespace="ms_markets",
    skills_path=Path(str(skills_path)),
    pinned_version=version("ms-markets"),
    command="msm copy-msm-skills",
    protected_project_roots=(ms_markets_source_checkout_root,),
    project_guard=is_ms_markets_source_checkout,
)
```

That command writes only:

```text
.agents/skills/ms_markets/
```

It must not overwrite:

```text
.agents/skills/mainsequence/
```

## Implementation Tasks

- [x] Add `mainsequence/agent_skills.py`.
- [x] Add dataclasses for copied item/result payloads.
- [x] Add `AgentSkillCopyBlocked` with a clear block reason.
- [x] Add namespace normalization and validation.
- [x] Add generic source/destination overlap checks before any delete or copy.
- [x] Add support for `protected_project_roots` and `project_guard`.
- [x] Move `_copy_tree_overwrite(...)` or an equivalent private helper into the new
  module.
- [x] Implement `copy_agent_skills(...)`.
- [x] Implement sentinel writing as an internal helper.
- [x] Add a helper in CLI code to resolve an installed package version from the
  target project `.venv`, using the same interpreter that resolved the scaffold
  bundle.
- [x] Make version resolution mandatory for `project_update_agent_skills`; do not
  copy skills when the version cannot be resolved.
- [x] Refactor `project_update_agent_skills` to call `copy_agent_skills(...)`.
- [x] Extend CLI JSON output with:
  - `library_name`;
  - `namespace`;
  - `pinned_version`;
  - `sentinel_path`;
  - `destination_root`;
  - copied skill entries.
- [x] Extend human output to show the pinned version and sentinel path.
- [x] Update `docs/cli/index.md`.
- [x] Update `agent_scaffold/AGENTS.md` to tell agents to inspect
  `.agents/skills/mainsequence/PINNED_FROM.txt` when checking whether managed
  skills are current.
- [ ] Optionally add a future project-health command that compares sentinel versions
  with installed package versions. That command is not required for the first
  implementation.

## Tests

Add focused tests for the reusable function:

- [x] Copies immediate skill directories into `.agents/skills/<namespace>/`.
- [x] Skips files, hidden folders, and `__pycache__`.
- [x] Overwrites matching destination folders.
- [x] Preserves unrelated destination folders and project-owned top-level skills.
- [x] Writes `PINNED_FROM.txt` with library name, namespace, version, source path,
  timestamp, and command.
- [x] Requires `pinned_version`.
- [x] Never writes a sentinel with an empty or unknown version.
- [x] Dry-run returns the copy plan but writes no folders and no sentinel.
- [x] Blocks when destination root is the source skill root.
- [x] Blocks when destination root is inside the source skill root.
- [x] Blocks when source skill root is inside the destination root.
- [x] Blocks when `project_dir` matches a protected project root.
- [x] Blocks when `project_guard(project_dir)` returns a reason.
- [x] Blocked paths raise `AgentSkillCopyBlocked` before any filesystem writes.
- [x] Rejects invalid namespaces.

Update CLI tests:

- [x] `test_project_update_agent_skills_overwrites_matching_folders` should assert
  the sentinel exists under `.agents/skills/mainsequence/PINNED_FROM.txt`.
- [x] CLI JSON mode should include sentinel metadata.
- [x] The version resolver should be monkeypatched so tests do not depend on the
  actual local package version.

## Non-Goals

- Do not make extension libraries copy into `.agents/skills/mainsequence/`.
- Do not delete existing project-owned skill folders.
- Do not require extension libraries to use Typer or the Main Sequence CLI.
- Do not build the project-health/version-drift checker in the first change.
