from os.path import join
from pathlib import Path

import mkdocs_gen_files

src_root = Path("data_platform/")
for path in src_root.glob("**/*.py"):
    ident = ".".join(path.with_suffix("").parts)
    ident = ident.replace(".__init__", "")

    doc_path = Path("dagster", path.relative_to(src_root)).with_suffix(".md")
    if path.name == "__init__.py":
        doc_path = Path(
            join(doc_path.parent, doc_path.parent.name)
            ).with_suffix(".md")

    with mkdocs_gen_files.open(doc_path, "w") as f:
        print("::: " + ident, file=f)