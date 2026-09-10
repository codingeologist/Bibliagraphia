"""Bibliagraphia — a Bible graph in a single-file DuckDB database.

This package exists so the FastAPI app (`app.api`) and the
`bibliagraphia-build` console entrypoint (in `bibliagraphia.cli`) can be
installed and imported as a package. The actual graph-build logic lives in
the standalone `scripts/build_db.py` (kept runnable without an install);
this module's `cli.build()` is the installed wrapper around it.
"""
from __future__ import annotations

__version__ = "0.2.0"

__all__ = ["__version__"]
