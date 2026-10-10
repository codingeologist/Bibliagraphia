"""MCP (Model Context Protocol) server for Bibliagraphia.

Exposes the Bible graph as curated, LLM-friendly tools served from the same
FastAPI process as the JSON API (mounted at /mcp via FastMCP's ASGI app —
see the mount + lifespan wiring at the bottom of app/api.py).

The tools deliberately wrap the existing route handlers rather than
duplicating their SQL: one implementation, two interfaces (REST + MCP).
Imports of app.api are deferred into the tool bodies to avoid a circular
import (api.py imports this module at startup to build the mount).
"""
from __future__ import annotations

from typing import Optional

from fastmcp import FastMCP

mcp = FastMCP("Bibliagraphia")


@mcp.tool
def search_nodes(q: str, label: Optional[str] = None, limit: int = 20) -> dict:
    """Autocomplete search over graph nodes (books, versions, verses,
    regions, locations, figures) by name or code prefix.

    Use this first to resolve a human name (e.g. "John", "Syria",
    "Antioch", "David") to a node before calling graph tools.

    Args:
        q: Prefix to match on node name or book/version code (e.g. "Jer",
            "JOH", "KJV").
        label: Optional node label filter: book | version | verse |
            region | location | figure.
        limit: Max results (default 20, max 100).
    """
    from app.api import search

    return search(q=q, label=label, limit=limit)


@mcp.tool
def get_verse(book_code: str, chapter: int, verse_number: int) -> dict:
    """Return one Bible verse across all available versions (KJV, VUL, DRB)
    for side-by-side comparison.

    Args:
        book_code: Book code (e.g. GEN, JOH, REV — use search_nodes to
            find codes).
        chapter: Chapter number (1-based).
        verse_number: Verse number (1-based).
    """
    from app.api import verse

    return verse(book_code=book_code, chapter=chapter, verse_number=verse_number)


@mcp.tool
def read_chapter(book_code: str, chapter: int, version_code: str = "KJV") -> dict:
    """Return an ordered chapter of Bible text in the selected version,
    with location mentions linked to graph nodes.

    Args:
        book_code: Book code (e.g. GEN, JOH).
        chapter: Chapter number (1-based).
        version_code: Translation code: KJV | VUL | DRB (default KJV).
    """
    from app.api import read_chapter as chapter_endpoint

    return chapter_endpoint(
        book_code=book_code, chapter=chapter, version_code=version_code
    )


@mcp.tool
def reader_catalog(version_code: str = "KJV") -> dict:
    """List all books, available translations, and per-book chapter numbers
    for building reading navigation.

    Args:
        version_code: Translation code: KJV | VUL | DRB (default KJV).
    """
    from app.api import reader_catalog as catalog_endpoint

    return catalog_endpoint(version_code=version_code)


@mcp.tool
def traverse_graph(
    start_node: str,
    label: str = "book",
    edge: str = "verse_in_book",
) -> dict:
    """Recursively traverse the graph from a start node along one edge type
    and return all descendants plus the edges between them.

    Examples: all verses of a book (label="book", edge="verse_in_book"),
    all locations in a region (label="region", edge="location_in_region"),
    a figure's family tree (label="figure", edge="figure_relative_of" —
    each returned edge carries its kinship kind in attrs.relationship:
    father | mother | parent | sibling | partner).

    Args:
        start_node: Node name or code (e.g. "GEN", "Syria", "David").
        label: Label of the start node: book | version | region | figure.
        edge: Edge label to recurse along: verse_in_book |
            verse_in_version | location_in_verse | location_in_region |
            figure_in_verse | figure_relative_of.
    """
    from app.api import TraversalRequest, traverse

    return traverse(
        TraversalRequest(start_node=start_node, label=label, edge=edge)
    )


@mcp.tool
def find_path(
    source: str,
    target: str,
    source_label: str = "verse",
    target_label: str = "region",
) -> dict:
    """Find the shortest path (breadth-first) between two nodes, walking
    edges in either direction. Returns the node chain and the edges walked,
    each carrying its label and attrs (kinship edges name their kind:
    father | mother | parent | sibling | partner).

    Example: how a verse connects to a region via the locations it
    mentions (source_label="verse", target_label="region"), or how two
    figures relate (source_label="figure", target_label="figure").

    Args:
        source: Source node name or code.
        target: Target node name or code.
        source_label: Label of the source node: book | version | verse |
            region | location.
        target_label: Label of the target node.
    """
    from app.api import PathRequest, path

    return path(
        PathRequest(
            source=source,
            source_label=source_label,
            target=target,
            target_label=target_label,
        )
    )


@mcp.tool
def graph_neighborhood(
    node: str,
    label: str,
    hops: int = 1,
    limit: int = 200,
) -> dict:
    """Return the ego-graph around one node: every node reached within
    `hops` BFS levels (capped at `limit`) plus the edges among them.
    Useful for understanding how a verse, book, location, or region
    connects to its surroundings.

    Args:
        node: Start node name or code (e.g. "JOH", "Syria", "David").
        label: Label of the start node: book | version | verse | region |
            location | figure.
        hops: Neighborhood radius 1-3 (default 1).
        limit: Max nodes returned (default 200).
    """
    from app.api import graph

    return graph(node=node, label=label, hops=hops, limit=limit)


@mcp.tool
def map_locations(
    region: Optional[str] = None,
    book: Optional[str] = None,
    limit: int = 1000,
) -> dict:
    """Return geocoded location mentions (latitude/longitude + verse text)
    for a map view. Scope is required: a region or a book.

    Examples: every place mentioned in a region (region="Syria"), or every
    place a book mentions (book="ACT").

    Args:
        region: Region name (e.g. "Syria", "Judaea").
        book: Book code (e.g. JOH, ACT).
        limit: Max points (default 1000).
    """
    from app.api import map_points

    return map_points(region=region, book=book, limit=limit)


@mcp.tool
def explain_verse(
    book_code: str,
    chapter: int,
    verse_number: int,
    version_code: str = "KJV",
) -> dict:
    """Explain a Bible verse: gather the graph context — the verse across
    all translations, the verses immediately above and below for reading
    context, the places the graph links to the verse, and those places'
    region descriptions — then ask the FaithTech-hosted DeepSeek model to
    write a chat reply explaining what the passage is and the significance
    of the place and the persons involved.

    Requires the FAITHTECH_API_KEY environment variable on the server
    (plus optional FAITHTECH_API_BASE, FAITHTECH_MODEL, FAITHTECH_TIMEOUT).

    Args:
        book_code: Book code (e.g. GEN, JOH — use search_nodes to find
            codes).
        chapter: Chapter number (1-based).
        verse_number: Verse number (1-based).
        version_code: Translation used for the selected verse and its
            context verses: KJV | VUL | DRB (default KJV).
    """
    from app.api import explain

    return explain(
        book_code=book_code,
        chapter=chapter,
        verse_number=verse_number,
        version_code=version_code,
    )
