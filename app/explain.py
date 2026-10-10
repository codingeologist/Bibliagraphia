"""Verse explanation service — graph context plus a FaithTech DeepSeek chat.

Both the REST endpoint (`GET /explain` in app/api.py) and the MCP tool
(`explain_verse` in app/mcp.py) funnel through explain_verse() here. The
flow is deliberately two-stage:

1. fetch_graph_context() reads the single-file DuckDB graph directly and
   gathers everything relevant to one verse:
     - the selected verse in every available translation,
     - the verses immediately above and below it, as reading context,
     - the location mentions the graph links to the verse, each with its
       region and geographic coordinates,
     - the descriptions of those regions.
2. The gathered context is packed into a system + user prompt pair and sent
   to the DeepSeek model served for FaithTech on an OpenAI-compatible
   endpoint (model: BreezeSeekVision). The model writes a chat reply
   explaining what the passage is and the significance of the place and
   the persons involved.

Configuration (environment variables; all optional except the key):
    FAITHTECH_API_KEY    bearer token for the FaithTech API (required)
    FAITHTECH_API_BASE   base URL, default
                         https://mq2yi3izzu14g5-8000.proxy.runpod.net/v1
    FAITHTECH_MODEL      model name, default BreezeSeekVision
    FAITHTECH_TIMEOUT    request timeout in seconds, default 60
"""
from __future__ import annotations

import os
import re

import duckdb
import httpx2 as httpx  # API-compatible fork of httpx (already a fastmcp dependency)

DEFAULT_API_BASE = "https://mq2yi3izzu14g5-8000.proxy.runpod.net/v1"
DEFAULT_MODEL = "BreezeSeekVision"
DEFAULT_TIMEOUT = 60.0

# Sampling: explanations should be faithful and steady, so a low-ish
# temperature; generous max_tokens so the model never runs out of room
# mid-reply (a reasoning-heavy model can spend a while planning first).
TEMPERATURE = 0.4
MAX_TOKENS = 6000

# BreezeSeekVision is a reasoning model that plans before it writes. It is
# told to confine planning to one <_thinking>...<_end> block at the start
# of the reply; this strips that block so the reader only ever sees the
# explanation itself.
_THINKING_BLOCK = re.compile(r"^\s*<_thinking>.*?<_end>\s*", re.DOTALL)

SYSTEM_PROMPT = """\
You are the study companion of Bibliagraphia, a graph database that maps \
Bible versions, verses, locations, and regions together.

For every request you are given the graph's own facts:
- the selected verse in each available translation,
- the verses immediately before and after it, for reading context,
- the places (location mentions) the graph links to that verse, each with \
its region and coordinates,
- short descriptions of those regions from the graph.

Write one warm, clear chat reply that helps the reader understand the \
verse. Cover, in whatever order reads most naturally:

1. Passage — what the verse says and why it matters where it sits: use the \
neighbouring verses as context, then its place in the chapter, book, and \
the wider biblical narrative. If the translations supplied differ in a \
notable way, point it out.
2. Place — for each location the graph links to the verse, explain what \
the place is and its significance, grounding your words in the region \
descriptions supplied. Use coordinates only to orient the reader (e.g. \
"near the Dead Sea"); never invent coordinates.
3. Person — identify the people mentioned or implied in the verse and \
briefly explain who they are and why they matter.

House rules:
- If you need to think first, confine ALL planning and working notes to a
  single block that begins with <_thinking> and ends with <_end>, at the
  very start of your reply. Keep that block brief — at most a hundred
  words of notes, and never draft the reply inside it. Everything after
  <_end> is shown to the reader verbatim, so start it with the
  explanation itself — no meta-commentary, no notes about the task, no
  repeating the planning.
- Ground every claim about places and regions in the graph data supplied. \
Where the graph is silent you may draw on well-established biblical \
scholarship, but keep such additions modest and uncontroversial.
- Never claim a place is mentioned in the verse if the graph does not link \
it; if the graph links no place, say the verse names none.
- Explain, don't preach: no extended moralising or personal exhortation.
- Write in clear British English prose; short paragraphs, no headings.
"""


class ExplainError(Exception):
    """A user-facing failure of the explain flow (bad reference, missing
    key, upstream API trouble). The REST endpoint turns this into
    `{"error": ...}` like every other endpoint."""


# --------------------------------------------------------------------------- #
# Graph context
# --------------------------------------------------------------------------- #
def _db_path() -> str:
    """Resolve the database path the same way the API does (deferred import
    to avoid a circular import — api.py imports this module)."""
    from app.api import DB_PATH

    return DB_PATH


def _reference(book_code: str, chapter: int, verse_number: int) -> str:
    return f"{book_code.upper()} {chapter}:{verse_number}"


def fetch_graph_context(
    book_code: str, chapter: int, verse_number: int, version_code: str = "KJV",
) -> dict:
    """Gather everything the graph knows around one verse.

    Raises ExplainError for an unknown book or a verse the graph does not
    have; missing neighbours (chapter edges) and absent regions simply
    come back as empty, not as errors.
    """
    conn = duckdb.connect(_db_path(), read_only=True)
    try:
        book = conn.execute(
            """
            SELECT name, json_extract_string(attrs, 'testament')
            FROM nodes
            WHERE label = 'book' AND UPPER(book_code) = UPPER(?)
            LIMIT 1;
            """,
            [book_code],
        ).fetchone()
        if book is None:
            raise ExplainError(
                f"No book with code '{book_code}'. Try /search, or a code "
                "like JOH or GEN."
            )

        # The selected verse across every translation the graph holds.
        version_rows = conn.execute(
            """
            SELECT version_code, json_extract_string(attrs, 'text')
            FROM nodes
            WHERE label = 'verse'
              AND UPPER(book_code) = UPPER(?)
              AND chapter = ?
              AND verse_number = ?
            ORDER BY version_code;
            """,
            [book_code, chapter, verse_number],
        ).fetchall()
        if not version_rows:
            raise ExplainError(
                f"No verse {_reference(book_code, chapter, verse_number)} in "
                "the graph. Try /search or /reader/catalog to find a valid "
                "reference."
            )
        versions = [{"code": r[0], "text": r[1]} for r in version_rows]
        primary = next(
            (v["text"] for v in versions
             if v["code"].upper() == version_code.upper()),
            versions[0]["text"],
        )

        # The verses immediately above and below, in the requested version.
        def neighbour(offset: int) -> dict | None:
            target = verse_number + offset
            if target < 1:
                return None
            row = conn.execute(
                """
                SELECT json_extract_string(attrs, 'text')
                FROM nodes
                WHERE label = 'verse'
                  AND UPPER(book_code) = UPPER(?)
                  AND chapter = ?
                  AND verse_number = ?
                  AND UPPER(version_code) = UPPER(?)
                LIMIT 1;
                """,
                [book_code, chapter, target, version_code],
            ).fetchone()
            if row is None or row[0] is None:
                return None
            return {"chapter": chapter, "verse_number": target, "text": row[0]}

        # Places the graph links to this verse (location mentions are
        # translation-agnostic: one node per mention).
        location_rows = conn.execute(
            """
            SELECT name,
                   json_extract_string(attrs, 'secondary_name'),
                   json_extract_string(attrs, 'region'),
                   json_extract(attrs, 'latitude')::DOUBLE,
                   json_extract(attrs, 'longitude')::DOUBLE,
                   json_extract_string(attrs, 'note')
            FROM nodes
            WHERE label = 'location'
              AND UPPER(book_code) = UPPER(?)
              AND chapter = ?
              AND verse_number = ?
            ORDER BY name, id;
            """,
            [book_code, chapter, verse_number],
        ).fetchall()
        locations = [
            {
                "name": r[0],
                "secondary_name": r[1],
                "region": r[2],
                "latitude": r[3],
                "longitude": r[4],
                "note": r[5],
            }
            for r in location_rows
        ]

        # Descriptions of the regions those places sit in.
        region_names = sorted({l["region"] for l in locations if l["region"]})
        regions: list[dict] = []
        if region_names:
            region_rows = conn.execute(
                """
                SELECT name,
                       json_extract_string(attrs, 'keywords'),
                       json_extract_string(attrs, 'description')
                FROM nodes
                WHERE label = 'region' AND name IN (SELECT unnest(?))
                ORDER BY name;
                """,
                [region_names],
            ).fetchall()
            regions = [
                {"name": r[0], "keywords": r[1], "description": r[2]}
                for r in region_rows
            ]

        return {
            "reference": {
                "book_code": book_code.upper(),
                "book_name": book[0],
                "testament": book[1],
                "chapter": chapter,
                "verse_number": verse_number,
                "version": version_code.upper(),
            },
            "verse": {"text": primary, "versions": versions},
            "context": {"previous": neighbour(-1), "next": neighbour(+1)},
            "locations": locations,
            "regions": regions,
        }
    finally:
        conn.close()


# --------------------------------------------------------------------------- #
# Prompt assembly
# --------------------------------------------------------------------------- #
def build_messages(context: dict) -> list[dict]:
    """Turn the gathered graph context into the system + user prompt pair
    for the chat model."""
    ref = context["reference"]
    lines = [
        f"Book: {ref['book_name']}"
        + (f" ({ref['testament']})" if ref["testament"] else ""),
        f"Selected verse: {_reference(ref['book_code'], ref['chapter'], ref['verse_number'])}"
        f" (translation: {ref['version']})",
        "",
        "Translations of the selected verse:",
    ]
    for version in context["verse"]["versions"]:
        lines.append(f"  - {version['code']}: {version['text']}")

    prev, nxt = context["context"]["previous"], context["context"]["next"]
    if prev or nxt:
        lines += ["", "Reading context:"]
        if prev:
            lines.append(
                f"  - verse above ({_reference(ref['book_code'], prev['chapter'], prev['verse_number'])}): "
                f"{prev['text']}"
            )
        if nxt:
            lines.append(
                f"  - verse below ({_reference(ref['book_code'], nxt['chapter'], nxt['verse_number'])}): "
                f"{nxt['text']}"
            )

    if context["locations"]:
        lines += ["", "Places the graph links to this verse:"]
        for location in context["locations"]:
            place = location["name"]
            if location["secondary_name"]:
                place += f' (also known as: {location["secondary_name"]})'
            lines.append(f"  - {place}")
            region = f"Region: {location['region']}" if location["region"] else "Region: unknown"
            coords = (
                f"; coordinates: {location['latitude']:.4f}, {location['longitude']:.4f}"
                if location["latitude"] is not None and location["longitude"] is not None
                else ""
            )
            lines.append(f"      {region}{coords}")
    else:
        lines += ["", "Places the graph links to this verse: none."]

    if context["regions"]:
        lines += ["", "Region details from the graph:"]
        for region in context["regions"]:
            detail = f"  - {region['name']}: {region['description'] or 'no description in the graph.'}"
            if region["keywords"]:
                detail += f" (keywords: {region['keywords']})"
            lines.append(detail)

    lines += ["", "Write the explanation now."]
    return [
        {"role": "system", "content": SYSTEM_PROMPT},
        {"role": "user", "content": "\n".join(lines)},
    ]


# --------------------------------------------------------------------------- #
# Model call
# --------------------------------------------------------------------------- #
def _max_tokens() -> int:
    """Token budget for the model reply (planning block included)."""
    try:
        return int(os.environ.get("FAITHTECH_MAX_TOKENS") or MAX_TOKENS)
    except ValueError:
        return MAX_TOKENS


def _request_chat(messages: list[dict], base: str, model: str, api_key: str,
                   timeout: float) -> tuple[str, str | None]:
    """One round trip to the chat-completions endpoint; returns the raw
    assistant content (planning included, if any) and the finish reason."""
    try:
        response = httpx.post(
            f"{base}/chat/completions",
            headers={"Authorization": f"Bearer {api_key}"},
            json={
                "model": model,
                "messages": messages,
                "temperature": TEMPERATURE,
                "max_tokens": _max_tokens(),
            },
            timeout=timeout,
        )
    except httpx.HTTPError as exc:
        raise ExplainError(
            f"Could not reach the FaithTech API ({base}): {exc}"
        ) from exc

    if response.status_code != 200:
        raise ExplainError(
            f"FaithTech API returned {response.status_code}: "
            f"{response.text[:300]}"
        )
    try:
        choice = response.json()["choices"][0]
        return choice["message"]["content"], choice.get("finish_reason")
    except (KeyError, IndexError, ValueError) as exc:
        raise ExplainError(
            f"Unexpected response shape from the FaithTech API: {exc}"
        ) from exc


def chat_completion(messages: list[dict]) -> str:
    """Send the prompt pair to the FaithTech-hosted DeepSeek model
    (OpenAI-compatible) and return the reader-facing reply text.

    BreezeSeekVision is a reasoning model: it plans before it writes, in
    varying shapes (a <_thinking>...<_end> block, untagged notes before
    the block, or none at all). The reader-facing reply is whatever
    follows the *last* <_end> tag — a deterministic cut regardless of how
    the planning was shaped. An attempt that was cut off mid-planning
    (unclosed block, or finish_reason == "length") is retried once.
    """
    api_key = os.environ.get("FAITHTECH_API_KEY", "").strip()
    if not api_key:
        raise ExplainError(
            "FAITHTECH_API_KEY is not set — the explain feature calls the "
            "FaithTech DeepSeek API and needs a bearer token. Set the "
            "environment variable on the API server and retry."
        )
    base = (os.environ.get("FAITHTECH_API_BASE") or DEFAULT_API_BASE).rstrip("/")
    model = os.environ.get("FAITHTECH_MODEL") or DEFAULT_MODEL
    try:
        timeout = float(os.environ.get("FAITHTECH_TIMEOUT") or DEFAULT_TIMEOUT)
    except ValueError:
        timeout = DEFAULT_TIMEOUT

    for attempt in (1, 2):
        content, finish_reason = _request_chat(
            messages, base, model, api_key, timeout
        )
        content = content or ""
        unusable = (
            finish_reason == "length"
            or ("<_thinking>" in content and "<_end>" not in content)
        )
        if unusable and attempt == 1:
            continue  # planning ran long this run; try once more
        if unusable:
            raise ExplainError(
                "The model exhausted its token budget while planning and "
                "never wrote the explanation. Retry, or raise "
                "FAITHTECH_TIMEOUT / FAITHTECH_MAX_TOKENS."
            )
        # The reply is whatever follows the final <_end> tag; with no
        # planning tags at all, the whole content is the reply.
        if "<_end>" in content:
            reply = content.rsplit("<_end>", 1)[1]
        else:
            reply = content
        reply = _THINKING_BLOCK.sub("", reply)  # stray leftover blocks
        if reply.strip():
            return reply.strip()
        if attempt == 1:
            continue  # nothing but (short) planning came back
        raise ExplainError(
            "The model spent its reply on planning only and wrote no "
            "explanation. Retry."
        )
    raise ExplainError("unreachable")  # pragma: no cover


# --------------------------------------------------------------------------- #
# Orchestration
# --------------------------------------------------------------------------- #
def explain_verse(
    book_code: str, chapter: int, verse_number: int, version_code: str = "KJV",
) -> dict:
    """Gather the graph context for a verse and ask the model to explain it.

    Returns the full context (reference, verse + translations, neighbouring
    verses, locations, regions) plus the model's chat reply under
    `explanation`, so a client can render the explanation and its evidence
    together.
    """
    context = fetch_graph_context(book_code, chapter, verse_number, version_code)
    explanation = chat_completion(build_messages(context))
    return {**context, "explanation": explanation}
