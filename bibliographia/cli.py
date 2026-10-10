"""Command-line entrypoints for the `bibliagraphia` package.

The build logic lives in `scripts/build_db.py` (kept standalone so it also
works as a plain `python scripts/build_db.py` from a repo checkout, with no
install needed). This module is the installed-package counterpart: it
locates that script in the repo checkout, imports its `main()`, and exposes
it as the `bibliagraphia-build` console script declared in pyproject.toml.

Both paths build the identical DuckDB graph from the canonical JSON.

Note: building requires the repo's `data/*.json` sources, so the entrypoint
expects to be run from (or installed as editable from) a full checkout.
"""
from __future__ import annotations

import importlib.util
from pathlib import Path


def _find_build_script() -> Path:
    """Locate scripts/build_db.py in the repo checkout.

    Resolves relative to this file's location so it works both for an
    editable install (`uv pip install -e .`) and a bare checkout.
    """
    script = Path(__file__).resolve().parent.parent / "scripts" / "build_db.py"
    if not script.exists():
        raise SystemExit(
            f"build script not found: {script}\n"
            "The build needs the repo's data/*.json sources, so run "
            "`bibliagraphia-build` from (or installed as editable from) a "
            "full repo checkout, or use `python scripts/build_db.py`."
        )
    return script


def build() -> None:
    """Console entrypoint: build data/bible.db from the JSON sources."""
    script = _find_build_script()
    spec = importlib.util.spec_from_file_location("bibliagraphia_build", script)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    mod.main()


if __name__ == "__main__":
    build()
