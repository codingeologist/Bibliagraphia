"""Check API annotation compatibility without requiring the Bible dataset."""
from __future__ import annotations

import subprocess
import sys
import unittest
from pathlib import Path


class APIStartupTest(unittest.TestCase):
    def test_routes_and_openapi_load(self):
        result = subprocess.run(
            [
                sys.executable,
                "-c",
                """
from unittest.mock import MagicMock, patch

connection = MagicMock()
connection.execute.return_value.fetchone.return_value = (1,)
with patch("duckdb.connect", return_value=connection):
    from app.api import app

schema = app.openapi()
for route, names in (
    ("/search", ("label",)),
    ("/map/points", ("region", "book")),
    ("/map/places", ()),
):
    parameters = {
        parameter["name"]: parameter
        for parameter in schema["paths"][route]["get"]["parameters"]
    } if names else {}
    for name in names:
        assert parameters[name]["required"] is False
assert "/map/places" in schema["paths"]
""",
            ],
            cwd=Path(__file__).resolve().parent.parent,
            capture_output=True,
            text=True,
        )
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
