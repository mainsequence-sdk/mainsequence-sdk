---
name: mainsequence-metatables-transition
description: "Use when a repository still depends on the MetaTables code that used to ship inside `mainsequence`: imports from `mainsequence.meta_tables` or `mainsequence.client.metatables`, table classes imported from `mainsequence.client`, or the retired `mainsequence meta-table`, `time-index-table`, `code-repository time-index-table-updates`, and `migrations` commands. Covers only the one-time move to the `mainsequence-metatable` package; all MetaTables usage after the move belongs to the skills that package installs."
---

# Move a repository to the `metatables` package

MetaTables no longer ships inside `mainsequence`. This skill moves one
repository's code and dependencies to the package that now owns it, and stops
there. It does not move tables or catalog records. For table modeling, queries,
DataSources, updaters, and Alembic migrations, use the skills installed in
step 2.

## 1. Find what the repository uses

Search code, scripts, workflow files, and `pyproject.toml` for:

- `mainsequence.meta_tables` and `mainsequence.client.metatables`
- `MetaTable`, `TimeIndexMetaTable`, `TimeIndexTableUpdate`, or `DataSource`
  imported from `mainsequence.client`
- `mainsequence meta-table`, `mainsequence time-index-table`,
  `mainsequence code-repository time-index-table-updates`, and
  `mainsequence migrations`
- the `mainsequence[local-data]` extra

Report what you found before editing. If nothing matches, there is nothing to
move.

## 2. Install the package and its skills

```bash
uv add mainsequence-metatable
uv run python -m metatables.sdk_compat
uv run metatables copy-metatables-skills --path .
```

The distribution is `mainsequence-metatable`; the import and the command are
`metatables`. Do not install the PyPI project named `metatables`: it is
unrelated. The compatibility check must pass before you continue. If it fails,
the installed `mainsequence` is not compatible; upgrading it is an update the
user has to request.

The last command copies the package's skills into `.agents/skills/metatables/`.

`mainsequence-metatable` brings back `alembic`, `sqlalchemy`, `pandas`,
`numpy`, `psycopg2-binary`, and `tqdm`, which the SDK no longer installs. The
`local-data` extra is gone: remove it, and declare `duckdb` and `pyarrow`
directly if the repository imports them.

## 3. Rewrite imports

| Old | New |
| --- | --- |
| `mainsequence.meta_tables` | `metatables` |
| `mainsequence.meta_tables.<module>` | `metatables.<module>` |
| `mainsequence.meta_tables.time_index_table_updates.<module>` | `metatables.updaters.<module>` |
| `mainsequence.client.metatables`, `mainsequence.client.metatables.core` | `metatables.models` |
| table classes from `mainsequence.client` | `metatables` |

Change module paths only. In an existing Alembic provider, update the provider
module and `env.py` the same way, and leave applied revision files untouched.
If a name is missing from `metatables`, report it instead of guessing a
substitute.

`CodeRepositoryBranch.get_time_index_table_updates()` and DataSource resolution
through `CodeRepositoryContext` have no SDK replacement. The `metatables` skills
cover what to use instead.

## 4. Replace commands

The four command groups keep their names: `mainsequence <group> ...` becomes
`metatables <group> ...`. Confirm each one with `metatables <group> --help`. If
a subcommand is missing or refuses to run, stop and report it. Do not pin an
older `mainsequence`, connect to the database directly, or register tables by
hand to get around it.

## 5. Check the result

- The search from step 1 returns nothing.
- The changed modules import and the repository's tests pass.
- `.agents/skills/metatables/` exists.

Then hand over to those skills.
