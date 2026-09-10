"""Tests for the Bibliagraphia single-file DuckDB graph.

Builds the DB into a temp path (so we never touch the repo's data/bible.db)
and checks the shape the API and the docs rely on: node/edge counts, a
traversal, a cross-partition path, and a cross-version verse lookup.
"""
from __future__ import annotations

import os
from pathlib import Path

import duckdb
import pytest

REPO = Path(__file__).resolve().parent.parent
DATA = REPO / "data"


@pytest.fixture(scope="module")
def db_path(tmp_path_factory):
    p = tmp_path_factory.mktemp("bibliagraphia") / "bible.db"
    env = os.environ.copy()
    os.environ["BIBLE_DB_PATH"] = str(p)
    os.environ["BIBLE_DATA_DIR"] = str(DATA)
    try:
        import importlib.util
        spec = importlib.util.spec_from_file_location("build_db", REPO / "scripts" / "build_db.py")
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        mod.main()
    finally:
        os.environ.clear()
        os.environ.update(env)
    return p


def test_builds_and_counts(db_path):
    conn = duckdb.connect(str(db_path), read_only=True)
    try:
        n = dict(conn.execute("SELECT label, COUNT(*) FROM nodes GROUP BY label").fetchall())
        assert n["version"] == 3
        assert n["book"] == 73
        assert n["verse"] == 102722
        assert n["region"] == 36
        assert n["location"] == 7460
        e = dict(conn.execute("SELECT label, COUNT(*) FROM edges GROUP BY label").fetchall())
        assert e["verse_in_book"] == 102722
        assert e["verse_in_version"] == 102722
        assert e["location_in_region"] == 7460
        assert e["location_in_verse"] > 0
    finally:
        conn.close()


def test_traverse_verses_in_a_book(db_path):
    conn = duckdb.connect(str(db_path), read_only=True)
    try:
        start = conn.execute(
            "SELECT id FROM nodes WHERE label='book' AND book_code='GEN' LIMIT 1"
        ).fetchone()[0]
        rows = conn.execute(
            """
            WITH RECURSIVE d AS (
                SELECT to_id FROM edges WHERE from_id=? AND label='verse_in_book'
                UNION ALL
                SELECT e.to_id FROM edges e JOIN d ON e.from_id=d.to_id WHERE e.label='verse_in_book'
            )
            SELECT COUNT(*) FROM d
            """, [start]
        ).fetchone()[0]
        assert rows == 4594  # Genesis has ~4594 verses across the 3 versions
    finally:
        conn.close()


def test_locations_in_a_region(db_path):
    conn = duckdb.connect(str(db_path), read_only=True)
    try:
        n = conn.execute(
            """
            SELECT COUNT(*) FROM nodes r
            JOIN edges e ON e.from_id=r.id AND e.label='location_in_region'
            JOIN nodes l ON l.id=e.to_id
            WHERE r.label='region' AND r.name='Syria'
            """
        ).fetchone()[0]
        assert n > 100  # Syria is well-attested
    finally:
        conn.close()


def test_cross_version_verse(db_path):
    conn = duckdb.connect(str(db_path), read_only=True)
    try:
        rows = conn.execute(
            """
            SELECT version_code, json_extract_string(attrs,'text')
            FROM nodes WHERE label='verse' AND book_code='JOH'
              AND chapter=3 AND verse_number=16 ORDER BY version_code
            """
        ).fetchall()
        assert len(rows) == 3
        assert all("God" in r[1] or "Deus" in r[1] for r in rows)
    finally:
        conn.close()


def test_path_verse_to_region(db_path):
    # Pick a location that mentions a verse, walk to its region, and confirm
    # a 2-hop path exists: verse -> location (location_in_verse) ... then
    # region -> location (location_in_region). The BFS endpoint walks both
    # directions, so a verse should reach its region in <= 3 hops.
    conn = duckdb.connect(str(db_path), read_only=True)
    try:
        # any Syria location and its DRB verse
        loc = conn.execute(
            """
            SELECT l.id, l.book_code, l.chapter, l.verse_number FROM nodes l
            JOIN edges e ON e.to_id=l.id AND e.label='location_in_region'
            JOIN nodes r ON r.id=e.from_id
            WHERE r.name='Syria' LIMIT 1
            """
        ).fetchone()
        assert loc is not None
        vid = conn.execute(
            "SELECT id FROM nodes WHERE label='verse' AND version_code='DRB' "
            "AND book_code=? AND chapter=? AND verse_number=? LIMIT 1",
            [loc[1], loc[2], loc[3]]
        ).fetchone()
        assert vid is not None
        # bidirectional reachability check
        reachable = conn.execute(
            """
            WITH RECURSIVE b(node, depth) AS (
                SELECT to_id, 1 FROM edges WHERE from_id=?
                UNION
                SELECT from_id, 1 FROM edges WHERE to_id=?
                UNION
                SELECT CASE WHEN e.from_id=b.node THEN e.to_id ELSE e.from_id END, b.depth+1
                FROM edges e JOIN b ON (e.from_id=b.node OR e.to_id=b.node)
                WHERE b.depth < 5
            )
            SELECT EXISTS(SELECT 1 FROM b WHERE node=(
                SELECT id FROM nodes WHERE label='region' AND name='Syria' LIMIT 1))
            """, [vid[0], vid[0]]
        ).fetchone()[0]
        assert reachable
    finally:
        conn.close()
