"""Generate the code reference pages and navigation."""

import copy
from pathlib import Path

import mkdocs_gen_files

nav = mkdocs_gen_files.Nav()
project_name = "mainsequence"

prefix = [project_name]

root = Path(__file__).parent.parent
src = root / project_name
docs_folder = root / "docs"

for path in sorted(src.rglob("*.py")):
    module_path = path.relative_to(src).with_suffix("")
    doc_path = path.relative_to(src).with_suffix(".md")
    full_doc_path = Path(f"{docs_folder}/reference", doc_path)

    parts = tuple(module_path.parts)
    # ───►  ignore every module under mainsequence.reportbuilder.*
    if "reportbuilder" in parts:
        continue
    if parts[-1] == "__init__":
        parts = parts[:-1]
        continue
    elif parts[-1] == "__main__":
        continue

    nav[parts] = doc_path.as_posix()

    with mkdocs_gen_files.open(full_doc_path, "w") as fd:
        full_path = copy.deepcopy(prefix)
        full_path.extend(parts)
        ident = ".".join(full_path)
        fd.write(f"::: {ident}")

    mkdocs_gen_files.set_edit_path(full_doc_path, path.relative_to(root))

with mkdocs_gen_files.open("reference/SUMMARY.md", "w") as nav_file:
    nav_file.writelines(nav.build_literate_nav())
