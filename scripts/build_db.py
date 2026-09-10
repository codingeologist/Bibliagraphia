"""Build data/bible.db from the canonical JSON sources.

Mirrors sql/init_duckdb.sql + sql/load_data.sql but in a single idempotent Python script.

The graph is heterogeneous (versions, books, verses, regions, locations),
so we store every node in one `nodes` table (synthetic id + label + a few
pulled-out query columns + a JSON `attrs` blob for the rest) and every
relation in one `edges` table with a `label`. Edges are only created
between nodes we actually have (referential integrity), which keeps
partial views clean.

The JSON files are loaded straight into DuckDB temp tables with
read_json_auto (no Python row-by-row), then the graph is built with
INSERT ... SELECT. The whole build is a few seconds for ~120k nodes.

Usage:
    python scripts/build_db.py
    BIBLE_DB_PATH=/tmp/bible.db python scripts/build_db.py
"""
from __future__ import annotations

import os
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parent.parent  # repo root
DATA = Path(os.environ.get("BIBLE_DATA_DIR", ROOT / "data"))
DB_PATH = Path(os.environ.get("BIBLE_DB_PATH", DATA / "bible.db"))


def main() -> None:
    if not DATA.is_dir():
        raise SystemExit(f"data dir not found: {DATA}")
    for name in ("books.json", "versions.json", "verses.json",
                 "location_regions.json", "regions.json"):
        if not (DATA / name).exists():
            raise SystemExit(f"missing canonical source: {DATA / name}")

    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    if DB_PATH.exists():
        DB_PATH.unlink()

    conn = duckdb.connect(str(DB_PATH))

    # ---- Schema ----------------------------------------------------------
    conn.execute(
        """
        CREATE TABLE nodes (
            id            VARCHAR PRIMARY KEY,
            label         VARCHAR NOT NULL,
            name          VARCHAR,
            version_code  VARCHAR,
            book_code     VARCHAR,
            chapter       INTEGER,
            verse_number  INTEGER,
            attrs         JSON
        );
        """
    )
    conn.execute(
        """
        CREATE TABLE edges (
            from_id   VARCHAR NOT NULL,
            to_id     VARCHAR NOT NULL,
            label     VARCHAR NOT NULL,
            PRIMARY KEY (from_id, to_id, label)
        );
        """
    )

    # ---- Load the canonical JSON into temp tables -----------------------
    # read_json_auto auto-types the columns; we cast/coerce as we build nodes.
    paths = {k: str(DATA / f) for k, f in [
        ("versions", "versions.json"), ("books", "books.json"),
        ("verses", "verses.json"), ("locations", "location_regions.json"),
        ("regions", "regions.json"),
    ]}
    conn.execute("CREATE TEMP TABLE t_versions  AS SELECT * FROM read_json_auto(?)", [paths["versions"]])
    conn.execute("CREATE TEMP TABLE t_books     AS SELECT * FROM read_json_auto(?)", [paths["books"]])
    conn.execute("CREATE TEMP TABLE t_verses    AS SELECT * FROM read_json_auto(?)", [paths["verses"]])
    conn.execute("CREATE TEMP TABLE t_locations AS SELECT * FROM read_json_auto(?)", [paths["locations"]])
    conn.execute("CREATE TEMP TABLE t_regions   AS SELECT * FROM read_json_auto(?)", [paths["regions"]])

    # ---- Versions -------------------------------------------------------
    conn.execute(
        """
        INSERT INTO nodes
        SELECT 'version:' || code, 'version', name, code,
               NULL, NULL, NULL,
               to_json({
                   'name': name, 'full_name': full_name
               })
        FROM t_versions;
        """
    )

    # ---- Books ----------------------------------------------------------
    conn.execute(
        """
        INSERT INTO nodes
        SELECT 'book:' || code, 'book',
               COALESCE(kjv, rheims, vulgate), NULL, code, NULL, NULL,
               to_json({
                   'testament': testament, 'vulgate': vulgate, 'rheims': rheims,
                   'kjv': kjv, 'note': note
               })
        FROM t_books;
        """
    )

    # ---- Verses ---------------------------------------------------------
    # verse_number comes in as a field named "verse"; chapter is integer.
    conn.execute(
        """
        INSERT INTO nodes
        SELECT 'verse:' || version_code || ':' || book_code || ':'
                  || chapter || ':' || verse,
               'verse', book, version_code, book_code, chapter, verse,
               to_json({'version': version, 'text': text})
        FROM t_verses;
        """
    )

    # ---- Regions --------------------------------------------------------
    conn.execute(
        """
        INSERT INTO nodes
        SELECT 'region:' || name, 'region', name, NULL, NULL, NULL, NULL,
               to_json({'keywords': keywords, 'description': description})
        FROM t_regions;
        """
    )

    # ---- Locations ------------------------------------------------------
    # The same (primary_name, book, chapter, verse) can appear more than once
    # (e.g. "Gibeah 1" in Jdg 20:9 occurs twice), so the id is suffixed with a
    # per-group row number to stay unique. The natural key is kept in attrs.
    conn.execute(
        """
        INSERT INTO nodes
        SELECT 'location:' || primary_name || ':' || book_code || ':'
                  || chapter || ':' || verse || ':' || rn,
               'location', primary_name, NULL, book_code, chapter, verse,
               to_json({
                   'secondary_name': secondary_name, 'region': region,
                   'testament': testament, 'rheims': rheims, 'vulgate': vulgate,
                   'kjv': kjv, 'rheims_text': rheims_text, 'vulgate_text': vulgate_text,
                   'kjv_text': kjv_text, 'note': note::VARCHAR,
                   'latitude': latitude, 'longitude': longitude
               })
        FROM (
            SELECT *, row_number() OVER (
                PARTITION BY primary_name, book_code, chapter, verse
                ORDER BY latitude, longitude
            ) AS rn
            FROM t_locations
        );
        """
    )

    # ---- Edges (referential-integrity set joins) -----------------------
    # Note: location ids carry a dedup suffix, so location edges join on the
    # natural key columns (book_code/chapter/verse_number), not the id string.
    # Use json_extract_string() rather than the `->>'field'` operator: with
    # a JSON-typed attrs column the `->>` operator can be shadowed by column
    # name resolution in join scopes, whereas the function form is unambiguous.
    conn.execute(
        """
        INSERT OR IGNORE INTO edges (from_id, to_id, label)
        SELECT b.id, v.id, 'verse_in_book'
        FROM nodes b JOIN nodes v ON v.book_code = b.book_code
        WHERE b.label = 'book' AND v.label = 'verse';
        """
    )
    conn.execute(
        """
        INSERT OR IGNORE INTO edges (from_id, to_id, label)
        SELECT ver.id, v.id, 'verse_in_version'
        FROM nodes ver JOIN nodes v ON v.version_code = ver.version_code
        WHERE ver.label = 'version' AND v.label = 'verse';
        """
    )
    conn.execute(
        """
        INSERT OR IGNORE INTO edges (from_id, to_id, label)
        SELECT r.id, l.id, 'location_in_region'
        FROM nodes r JOIN nodes l
          ON r.label = 'region' AND l.label = 'location'
         AND json_extract_string(l.attrs, 'region') = r.name;
        """
    )
    conn.execute(
        """
        INSERT OR IGNORE INTO edges (from_id, to_id, label)
        SELECT v.id, l.id, 'location_in_verse'
        FROM nodes l JOIN nodes v
          ON v.label = 'verse' AND l.label = 'location'
         AND v.book_code = l.book_code
         AND v.chapter = l.chapter
         AND v.verse_number = l.verse_number;
        """
    )

    # ---- Indexes (after data — faster to build) -------------------------
    conn.execute("CREATE INDEX idx_nodes_label      ON nodes(label);")
    conn.execute("CREATE INDEX idx_nodes_book_code  ON nodes(book_code);")
    conn.execute("CREATE INDEX idx_nodes_version     ON nodes(version_code);")
    conn.execute("CREATE INDEX idx_nodes_name        ON nodes(name);")
    conn.execute("CREATE INDEX idx_edges_from        ON edges(from_id);")
    conn.execute("CREATE INDEX idx_edges_to          ON edges(to_id);")
    conn.execute("CREATE INDEX idx_edges_label       ON edges(label);")
    conn.execute("CREATE INDEX idx_edges_from_label ON edges(from_id, label);")

    # ---- Counts ---------------------------------------------------------
    n_nodes = conn.execute("SELECT COUNT(*) FROM nodes").fetchone()[0]
    n_edges = conn.execute("SELECT COUNT(*) FROM edges").fetchone()[0]
    counts = dict(conn.execute(
        "SELECT label, COUNT(*) FROM nodes GROUP BY label ORDER BY label"
    ).fetchall())
    ecounts = dict(conn.execute(
        "SELECT label, COUNT(*) FROM edges GROUP BY label ORDER BY label"
    ).fetchall())
    conn.close()

    print(f"Built {DB_PATH}")
    print(f"  nodes: {n_nodes}  ({counts})")
    print(f"  edges: {n_edges}  ({ecounts})")


if __name__ == "__main__":
    main()
