"""Bibliagraphia single-file DuckDB graph API.

A single FastAPI app that opens the materialised data/bible.db and runs
parameterized recursive SQL for traversal / path / search. All queries use ?
placeholders — no string interpolation — so there is no SQL-injection
surface (the original TypeQL loader built queries by f-string).

The UI is the standalone React frontend in `frontend/` (the Vite dev server
proxies to this API; in production nginx serves the built SPA and proxies
the JSON routes here). This process serves the JSON API only.

The graph is heterogeneous (versions, books, verses, regions, locations),
so endpoints take a `label` and an `edge` where relevant.
"""
from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Optional

import duckdb
from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

REPO_ROOT = Path(__file__).resolve().parent.parent

app = FastAPI(title="Bibliagraphia DuckDB Graph API")
app.add_middleware(
    CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"],
)

# Resolve the database path once. Docker mounts the volume at /data;
# locally fall back to ./data in the repo.
_DEFAULT_DB = os.environ.get("BIBLE_DB_PATH", "/data/bible.db")
DB_PATH = _DEFAULT_DB if Path(_DEFAULT_DB).exists() else str(
    REPO_ROOT / "data" / "bible.db"
)


def init_db() -> None:
    """Create the schema if missing and auto-build from JSON when empty.

    Lets the API run with zero setup as long as the canonical JSON sources
    are present.
    """
    conn = duckdb.connect(DB_PATH)
    try:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS nodes (
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
            CREATE TABLE IF NOT EXISTS edges (
                from_id   VARCHAR NOT NULL,
                to_id     VARCHAR NOT NULL,
                label     VARCHAR NOT NULL,
                PRIMARY KEY (from_id, to_id, label)
            );
            """
        )
        for stmt in (
            "CREATE INDEX IF NOT EXISTS idx_nodes_label ON nodes(label)",
            "CREATE INDEX IF NOT EXISTS idx_nodes_book_code ON nodes(book_code)",
            "CREATE INDEX IF NOT EXISTS idx_nodes_version ON nodes(version_code)",
            "CREATE INDEX IF NOT EXISTS idx_nodes_name ON nodes(name)",
            "CREATE INDEX IF NOT EXISTS idx_edges_from ON edges(from_id)",
            "CREATE INDEX IF NOT EXISTS idx_edges_to ON edges(to_id)",
            "CREATE INDEX IF NOT EXISTS idx_edges_label ON edges(label)",
            "CREATE INDEX IF NOT EXISTS idx_edges_from_label ON edges(from_id, label)",
        ):
            conn.execute(stmt)

        count = conn.execute("SELECT COUNT(*) FROM nodes").fetchone()[0]
        if count == 0:
            # Auto-build from the canonical JSON via the build script.
            import importlib.util
            build = REPO_ROOT / "scripts" / "build_db.py"
            if build.exists() and (REPO_ROOT / "data" / "verses.json").exists():
                conn.close()  # the build script opens the DB itself
                spec = importlib.util.spec_from_file_location("build_db", build)
                mod = importlib.util.module_from_spec(spec)
                # Point the build at this DB path.
                os.environ["BIBLE_DB_PATH"] = DB_PATH
                spec.loader.exec_module(mod)
                mod.main()
                conn = duckdb.connect(DB_PATH)
            else:
                app.state.empty_db = True
                return
        app.state.empty_db = False
    finally:
        conn.close()


init_db()


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #
def _row_to_node(row, columns) -> dict:
    n = dict(zip(columns, row))
    # attrs comes back as a JSON-typed value; render it as a dict for JSON.
    attrs = n.get("attrs")
    if isinstance(attrs, str):
        try:
            n["attrs"] = json.loads(attrs)
        except (ValueError, TypeError):
            pass
    return n


def _resolve(conn, label: str, name: str) -> Optional[str]:
    """Resolve a node by name within a label; return its id or None.

    Codes are the canonical identifiers — short, unique, and stable
    (books: GEN, JOH, REV; versions: KJV, VUL, DRB) — so they are tried
    before names. Names go from exact to loose, so users can type any
    of "JOH", "John", or "The Gospel According to John":
      1. exact name match
      2. book_code / version_code match (case-insensitive)
      3. name prefix ("ACT" matches "Acts")
      4. name contains ("John" matches "The Gospel According to John")
    """
    row = conn.execute(
        """
        SELECT id FROM nodes
        WHERE label = ?
          AND (name = ? OR name ILIKE ? OR name ILIKE ?
               OR UPPER(book_code) = UPPER(?)
               OR UPPER(version_code) = UPPER(?))
        ORDER BY name = ? DESC,
                 UPPER(book_code) = UPPER(?) DESC,
                 UPPER(version_code) = UPPER(?) DESC,
                 name ILIKE ? DESC,
                 length(name), name
        LIMIT 1
        """,
        [label, name, f"{name}%", f"%{name}%", name, name,
         name, name, name, f"{name}%"],
    ).fetchone()
    return row[0] if row else None


# --------------------------------------------------------------------------- #
# Endpoints
# --------------------------------------------------------------------------- #
@app.get("/health")
def health():
    return {"status": "ok", "db": DB_PATH, "empty": getattr(app.state, "empty_db", True)}


@app.get("/search")
def search(
    q: str = Query(..., min_length=1),
    label: Optional[str] = Query(None, description="restrict to a node label"),
    limit: int = Query(20, le=100),
):
    """Autocomplete: nodes whose name or book/version code starts with `q`.

    Codes are the canonical identifiers, so "JOH" finds John's Gospel even
    though its name ("The Gospel According to John") doesn't start with JOH.
    Books/versions/regions sort before locations and verses — the latter
    are per-mention instances that also carry book_code, and would otherwise
    flood the results for any code search.
    """
    conn = duckdb.connect(DB_PATH, read_only=True)
    try:
        like = f"{q}%"
        if label:
            rows = conn.execute(
                """
                SELECT id, label, name, book_code, chapter, verse_number, version_code
                FROM nodes
                WHERE label = ?
                  AND (name ILIKE ? OR UPPER(book_code) ILIKE UPPER(?)
                       OR UPPER(version_code) ILIKE UPPER(?))
                ORDER BY label IN ('location', 'verse'), label, name LIMIT ?;
                """,
                [label, like, like, like, limit],
            ).fetchall()
        else:
            rows = conn.execute(
                """
                SELECT id, label, name, book_code, chapter, verse_number, version_code
                FROM nodes
                WHERE name ILIKE ?
                   OR UPPER(book_code) ILIKE UPPER(?)
                   OR UPPER(version_code) ILIKE UPPER(?)
                ORDER BY label IN ('location', 'verse'), label, name LIMIT ?;
                """,
                [like, like, like, limit],
            ).fetchall()
        return {"query": q, "results": [
            {"id": r[0], "label": r[1], "name": r[2],
             "book_code": r[3], "chapter": r[4], "verse_number": r[5],
             "version_code": r[6]}
            for r in rows
        ]}
    finally:
        conn.close()


class TraversalRequest(BaseModel):
    start_node: str          # node name to resolve
    label: str = "book"      # label of the start node
    edge: str = "verse_in_book"  # edge label to recurse along


@app.post("/traverse")
def traverse(request: TraversalRequest):
    """Return all descendants of `start_node` reached by recursive edges of
    the given `label`, plus the edges between them. Direction is from->to
    (container -> leaf), e.g. a book fans out to its verses via verse_in_book.
    """
    conn = duckdb.connect(DB_PATH, read_only=True)
    try:
        start_id = _resolve(conn, request.label, request.start_node)
        if start_id is None:
            return {"error": f"No {request.label} named '{request.start_node}'"}

        rows = conn.execute(
            """
            WITH RECURSIVE descendants AS (
                SELECT to_id AS node_id, 1 AS level
                FROM edges
                WHERE from_id = ? AND label = ?
                UNION ALL
                SELECT e.to_id, d.level + 1
                FROM edges e
                JOIN descendants d ON e.from_id = d.node_id
                WHERE e.label = ?
            )
            SELECT n.id, n.label, n.name, n.version_code, n.book_code,
                   n.chapter, n.verse_number, n.attrs, d.level
            FROM descendants d
            JOIN nodes n ON n.id = d.node_id
            ORDER BY d.level, n.book_code, n.chapter, n.verse_number, n.name;
            """,
            [start_id, request.edge, request.edge],
        ).fetchall()
        columns = [d[0] for d in conn.description]
        nodes = [_row_to_node(r, columns) for r in rows]

        # Start node itself, for context.
        start_row = conn.execute(
            "SELECT id,label,name,version_code,book_code,chapter,verse_number,attrs "
            "FROM nodes WHERE id = ?",
            [start_id],
        ).fetchone()
        start_cols = [d[0] for d in conn.description]
        start_node = _row_to_node(start_row, start_cols) if start_row else {"id": start_id}

        # Derive a clean edge set: for each returned node, walk its incoming
        # edge of this label whose source is also in the result set.
        ids = {n["id"] for n in nodes}
        edges = (
            [
                {"source": from_id, "target": to_id, "label": request.edge}
                for (from_id, to_id) in conn.execute(
                    """
                    SELECT e.from_id, e.to_id FROM edges e
                    WHERE e.label = ?
                      AND e.to_id IN (SELECT unnest(?))
                      AND e.from_id IN (SELECT unnest(?))
                    """,
                    [request.edge, list(ids) or [""], [start_id] + list(ids)],
                ).fetchall()
            ]
            if ids
            else []
        )
        return {
            "start": request.start_node,
            "start_id": start_id,
            "start_node": start_node,
            "edge": request.edge,
            "count": len(nodes),
            "nodes": nodes,
            "edges": edges,
        }
    except Exception as exc:  # noqa: BLE001
        return {"error": str(exc)}
    finally:
        conn.close()


class PathRequest(BaseModel):
    source: str
    source_label: str = "verse"
    target: str
    target_label: str = "region"


@app.post("/path")
def path(req: PathRequest):
    """Shortest path between two named nodes (BFS), walking edges in either
    direction. DuckDB recursive CTEs can't cheaply maintain a visited set
    over 235k edges, so we load the adjacency once and do a plain BFS in
    Python — the graph is small and the result is a handful of hops.
    """
    conn = duckdb.connect(DB_PATH, read_only=True)
    try:
        src_id = _resolve(conn, req.source_label, req.source)
        tgt_id = _resolve(conn, req.target_label, req.target)
        if src_id is None:
            return {"error": f"No {req.source_label} named '{req.source}'. "
                            "Try the Search tool to find the exact name, "
                            "or a book code like JOH."}
        if tgt_id is None:
            return {"error": f"No {req.target_label} named '{req.target}'. "
                            "Try the Search tool to find the exact name."}
        if src_id == tgt_id:
            return {"found": True, "depth": 0, "source": req.source,
                    "target": req.target, "path": [src_id], "edges": []}

        # Load adjacency (both directions) once. 235k edges -> ~tens of MB, fine.
        rows = conn.execute("SELECT from_id, to_id, label FROM edges").fetchall()
        adj: dict[str, list[tuple[str, str]]] = {}
        for f, t, lbl in rows:
            adj.setdefault(f, []).append((t, lbl))
            adj.setdefault(t, []).append((f, lbl))

        # Plain BFS.
        from collections import deque
        prev: dict[str, tuple[str, str]] = {src_id: (None, None)}
        q = deque([src_id])
        found = False
        while q:
            cur = q.popleft()
            if cur == tgt_id:
                found = True
                break
            for nbr, lbl in adj.get(cur, []):
                if nbr not in prev:
                    prev[nbr] = (cur, lbl)
                    q.append(nbr)
        if not found:
            return {"source": req.source, "target": req.target,
                    "path": [], "edges": [], "found": False}

        # Reconstruct.
        path_ids = []
        path_labels = []
        cur = tgt_id
        while cur is not None:
            path_ids.append(cur)
            p, l = prev[cur]
            if l is not None:
                path_labels.append(l)
            cur = p
        path_ids.reverse()
        path_labels.reverse()

        node_rows = conn.execute(
            "SELECT id,label,name,version_code,book_code,chapter,verse_number,attrs "
            "FROM nodes WHERE id IN (SELECT unnest(?))",
            [path_ids],
        ).fetchall()
        cols = [d[0] for d in conn.description]
        by_id = {_row_to_node(r, cols)["id"]: _row_to_node(r, cols) for r in node_rows}
        nodes = [by_id[i] for i in path_ids if i in by_id]
        return {
            "source": req.source, "target": req.target,
            "source_id": src_id, "target_id": tgt_id,
            "found": True, "depth": len(path_ids) - 1,
            "path": nodes,
            "edges": [{"source": path_ids[i], "target": path_ids[i + 1],
                       "label": path_labels[i]} for i in range(len(path_labels))],
        }
    except Exception as exc:  # noqa: BLE001
        return {"error": str(exc)}
    finally:
        conn.close()


@app.get("/graph")
def graph(
    node: str = Query(..., min_length=1, description="start node: code or name"),
    label: str = Query(..., description="label of the start node"),
    hops: int = 1,   # 1–3, neighborhood radius
    limit: int = 200,  # 10–500, max nodes returned
    node_id: Optional[str] = None,
):
    """Ego-graph for the force-directed visualisation.

    The whole database (~150k nodes, 235k edges) is far too large to draw,
    so the UI queries the neighborhood of one node instead: BFS out from
    the start node up to `hops` levels, return every node reached (capped
    at `limit`) plus the edges among them. Level-by-level BFS means the cap
    cuts the *outermost* ring first — the start node and its immediate
    neighborhood always survive.
    """
    from collections import deque

    conn = duckdb.connect(DB_PATH, read_only=True)
    try:
        if node_id:
            row = conn.execute(
                "SELECT id FROM nodes WHERE id = ? AND label = ?",
                [node_id, label],
            ).fetchone()
            start_id = row[0] if row else None
        else:
            start_id = _resolve(conn, label, node)
        if start_id is None:
            return {"error": f"No {label} named '{node}'. Try Search, or a "
                            "book/version code like JOH or KJV."}

        rows = conn.execute("SELECT from_id, to_id, label FROM edges").fetchall()
        adj: dict[str, list[tuple[str, str]]] = {}
        for f, t, lbl in rows:
            adj.setdefault(f, []).append((t, lbl))
            adj.setdefault(t, []).append((f, lbl))

        # Level-by-level BFS with a node cap.
        included: dict[str, int] = {start_id: 0}  # id -> hop distance
        truncated = False
        frontier = [start_id]
        for level in range(1, hops + 1):
            if truncated:
                break
            next_frontier = []
            for cur in frontier:
                for nbr, lbl in adj.get(cur, []):
                    if nbr in included:
                        continue
                    if len(included) >= limit:
                        truncated = True
                        break
                    included[nbr] = level
                    next_frontier.append(nbr)
                if truncated:
                    break
            frontier = next_frontier
            if not frontier:
                break

        # Edges among the included nodes only.
        links = [
            {"source": f, "target": t, "label": lbl}
            for f, t, lbl in rows
            if f in included and t in included
        ]

        node_rows = conn.execute(
            "SELECT id,label,name,version_code,book_code,chapter,verse_number,attrs "
            "FROM nodes WHERE id IN (SELECT unnest(?))",
            [list(included)],
        ).fetchall()
        cols = [d[0] for d in conn.description]
        nodes = [_row_to_node(r, cols) for r in node_rows]
        for n in nodes:
            n["dist"] = included[n["id"]]
        nodes.sort(key=lambda n: (n["dist"], n["label"], n["name"] or ""))
        return {
            "start": node, "start_id": start_id, "hops": hops,
            "truncated": truncated,
            "count": len(nodes),
            "nodes": nodes,
            "links": links,
        }
    except Exception as exc:  # noqa: BLE001
        return {"error": str(exc)}
    finally:
        conn.close()


@app.get("/map")
def map_points(
    region: Optional[str] = None,  # region name, e.g. Syria
    book: Optional[str] = None,   # book code, e.g. JOH
    limit: int = 1000,  # 10–3000, max points returned
):
    """Location points for the map view.

    Every location mention-instance carries latitude/longitude in attrs.
    The scope is required — 7,460 points at once is too much for the
    browser — and is either a region (all mentions located in that region)
    or a book (all places that book mentions). Popups show the KJV text,
    so each point carries a short verse reference too.
    """
    conn = duckdb.connect(DB_PATH, read_only=True)
    try:
        where, params, scope = [], [], {}
        if region:
            region_id = _resolve(conn, "region", region)
            if region_id is None:
                return {"error": f"No region named '{region}'. Try Search first."}
            region_name = conn.execute(
                "SELECT name FROM nodes WHERE id = ?", [region_id]
            ).fetchone()[0]
            where.append("json_extract_string(attrs, 'region') = ?")
            params.append(region_name)
            scope["region"] = region_name
        if book:
            code = book.upper()
            exists = conn.execute(
                "SELECT count(*) FROM nodes WHERE label='book' AND UPPER(book_code) = ?",
                [code],
            ).fetchone()[0]
            if not exists:
                return {"error": f"No book with code '{book}'. Try Search, or a "
                                "code like JOH or GEN."}
            where.append("UPPER(book_code) = ?")
            params.append(code)
            scope["book"] = code
        if not where:
            return {"error": "Provide a region or a book to scope the map — "
                            "the whole database is too large to draw."}

        rows = conn.execute(
            """
            SELECT name,
                   json_extract_string(attrs, 'secondary_name'),
                   json_extract_string(attrs, 'region'),
                   book_code, chapter, verse_number,
                   json_extract(attrs, 'latitude')::DOUBLE,
                   json_extract(attrs, 'longitude')::DOUBLE,
                   json_extract_string(attrs, 'kjv_text')
            FROM nodes
            WHERE label = 'location'
              AND json_extract_string(attrs, 'latitude') IS NOT NULL
              AND """ + " AND ".join(where) + "\n            LIMIT ?;",
            params + [limit],
        ).fetchall()
        return {
            **scope,
            "count": len(rows),
            "truncated": len(rows) >= limit,
            "points": [
                {"name": r[0], "secondary_name": r[1], "region": r[2],
                 "book_code": r[3], "chapter": r[4], "verse_number": r[5],
                 "lat": r[6], "lng": r[7], "text": r[8]}
                for r in rows
            ],
        }
    except Exception as exc:  # noqa: BLE001
        return {"error": str(exc)}
    finally:
        conn.close()


@app.get("/verse")
def verse(
    book_code: str = Query(...),
    chapter: int = Query(..., ge=1),
    verse_number: int = Query(..., ge=1),
):
    """Cross-version comparison: return the same verse across all versions."""
    conn = duckdb.connect(DB_PATH, read_only=True)
    try:
        rows = conn.execute(
            """
            SELECT id, version_code, json_extract_string(attrs, 'text') AS text
            FROM nodes
            WHERE label = 'verse' AND book_code = ? AND chapter = ? AND verse_number = ?
            ORDER BY version_code;
            """,
            [book_code, chapter, verse_number],
        ).fetchall()
        return {"book_code": book_code, "chapter": chapter,
                "verse_number": verse_number,
                "verses": [{"id": r[0], "version": r[1], "text": r[2]} for r in rows]}
    finally:
        conn.close()


@app.get("/reader/catalog")
def reader_catalog(version_code: str = Query("KJV")):
    """Return books, versions, and chapters available in the selected version."""
    conn = duckdb.connect(DB_PATH, read_only=True)
    try:
        books = conn.execute(
            """
            SELECT book_code, name, json_extract_string(attrs, 'testament')
            FROM nodes
            WHERE label = 'book'
            ORDER BY rowid;
            """
        ).fetchall()
        versions = conn.execute(
            """
            SELECT version_code, name, json_extract_string(attrs, 'full_name')
            FROM nodes
            WHERE label = 'version'
            ORDER BY rowid;
            """
        ).fetchall()
        chapters = conn.execute(
            """
            SELECT book_code, list(DISTINCT chapter ORDER BY chapter)
            FROM nodes
            WHERE label = 'verse' AND version_code = UPPER(?)
            GROUP BY book_code;
            """,
            [version_code],
        ).fetchall()
        return {
            "books": [
                {"code": row[0], "name": row[1], "testament": row[2]}
                for row in books
            ],
            "versions": [
                {"code": row[0], "name": row[1], "full_name": row[2]}
                for row in versions
            ],
            "chapters": {row[0]: row[1] for row in chapters},
        }
    finally:
        conn.close()


@app.get("/chapter")
def read_chapter(
    book_code: str = Query(..., min_length=1),
    chapter: int = Query(..., ge=1),
    version_code: str = Query("KJV", min_length=1),
):
    """Return an ordered chapter of Bible text in the selected version."""
    conn = duckdb.connect(DB_PATH, read_only=True)
    try:
        row = conn.execute(
            """
            SELECT name
            FROM nodes
            WHERE label = 'book' AND UPPER(book_code) = UPPER(?)
            LIMIT 1;
            """,
            [book_code],
        ).fetchone()
        verses = conn.execute(
            """
            SELECT verse_number, json_extract_string(attrs, 'text') AS text
            FROM nodes
            WHERE label = 'verse'
              AND UPPER(book_code) = UPPER(?)
              AND chapter = ?
              AND UPPER(version_code) = UPPER(?)
            ORDER BY verse_number;
            """,
            [book_code, chapter, version_code],
        ).fetchall()
        locations = conn.execute(
            """
            SELECT l.id, l.verse_number, l.name,
                   json_extract_string(l.attrs, 'secondary_name') AS secondary_name,
                   json_extract_string(l.attrs, 'region') AS region
            FROM nodes l
            JOIN nodes v
              ON v.label = 'verse'
             AND v.book_code = l.book_code
             AND v.chapter = l.chapter
             AND v.verse_number = l.verse_number
             AND UPPER(v.version_code) = UPPER(?)
            WHERE l.label = 'location'
              AND UPPER(l.book_code) = UPPER(?)
              AND l.chapter = ?
            ORDER BY l.verse_number, l.name, l.id;
            """,
            [version_code, book_code, chapter],
        ).fetchall()
        return {
            "book_code": book_code.upper(),
            "book_name": row[0] if row else book_code.upper(),
            "chapter": chapter,
            "version": version_code.upper(),
            "verses": [{"number": verse[0], "text": verse[1]} for verse in verses],
            "locations": [
                {
                    "id": location[0],
                    "verse_number": location[1],
                    "name": location[2],
                    "aliases": [alias for alias in (location[2], location[3]) if alias],
                    "region": location[4],
                }
                for location in locations
            ],
        }
    finally:
        conn.close()
