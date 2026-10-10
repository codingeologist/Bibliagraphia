"""Tests for the /explain feature (app/explain.py).

The graph-context gathering runs against a small temp DuckDB fixture
(same shape as the reader tests); the model call is exercised through a
fake httpx transport so no network or API key is needed.
"""
from __future__ import annotations

import json

import duckdb
import pytest

from app import api
from app import explain as explain_service


@pytest.fixture
def explain_db(tmp_path, monkeypatch):
    monkeypatch.delenv("FAITHTECH_API_KEY", raising=False)
    path = tmp_path / "explain.db"
    conn = duckdb.connect(str(path))
    conn.execute(
        """
        CREATE TABLE nodes (
            id VARCHAR, label VARCHAR, name VARCHAR, version_code VARCHAR,
            book_code VARCHAR, chapter INTEGER, verse_number INTEGER, attrs JSON
        )
        """
    )
    conn.execute(
        "CREATE TABLE edges (from_id VARCHAR, to_id VARCHAR, label VARCHAR)"
    )
    conn.executemany(
        "INSERT INTO nodes VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        [
            ("book:GEN", "book", "Genesis", None, "GEN", None, None,
             json.dumps({"testament": "Old Testament"})),
            ("book:REV", "book", "Revelation", None, "REV", None, None,
             json.dumps({"testament": "New Testament"})),
            ("version:KJV", "version", "kjv", "KJV", None, None, None,
             json.dumps({"full_name": "King James Bible"})),
            ("version:DRB", "version", "rheims", "DRB", None, None, None,
             json.dumps({"full_name": "Douay-Rheims Bible"})),
            ("verse:KJV:GEN:1:1", "verse", "Genesis", "KJV", "GEN", 1, 1,
             json.dumps({"text": "In the beginning God created the heaven and the earth."})),
            ("verse:DRB:GEN:1:1", "verse", "Genesis", "DRB", "GEN", 1, 1,
             json.dumps({"text": "In the beginning God created heaven, and earth."})),
            ("verse:KJV:GEN:1:2", "verse", "Genesis", "KJV", "GEN", 1, 2,
             json.dumps({"text": "And a river went out of Eden to water the garden."})),
            ("verse:KJV:GEN:1:3", "verse", "Genesis", "KJV", "GEN", 1, 3,
             json.dumps({"text": "And the evening and the morning were the first day."})),
            ("location:Eden:GEN:1:2:1", "location", "Eden", None, "GEN", 1, 2,
             json.dumps({
                 "secondary_name": "Garden of Eden",
                 "region": "Canaan",
                 "latitude": 31.8,
                 "longitude": 35.2,
             })),
            ("region:Canaan", "region", "Canaan", None, None, None, None,
             json.dumps({
                 "keywords": "Jerusalem,Bethel",
                 "description": "The promised land between the Jordan and the sea.",
             })),
        ],
    )
    conn.close()
    monkeypatch.setattr(api, "DB_PATH", str(path))
    return path


class _FakeResponse:
    def __init__(self, status_code=200, payload=None, text=""):
        self.status_code = status_code
        self._payload = payload or {}
        self.text = text

    def json(self):
        return self._payload


def _ok_response(text):
    return _FakeResponse(payload={"choices": [{"message": {"content": text}}]})


def test_fetch_graph_context_gathers_neighbours_places_and_regions(explain_db):
    context = explain_service.fetch_graph_context("GEN", 1, 2, "KJV")

    assert context["reference"]["book_name"] == "Genesis"
    assert context["reference"]["testament"] == "Old Testament"
    assert context["verse"]["text"] == "And a river went out of Eden to water the garden."
    assert {v["code"] for v in context["verse"]["versions"]} == {"KJV"}
    # cross-version lookup on the verse that has both translations
    first = explain_service.fetch_graph_context("GEN", 1, 1, "KJV")
    assert {v["code"] for v in first["verse"]["versions"]} == {"KJV", "DRB"}
    assert context["context"]["previous"]["text"] == (
        "In the beginning God created the heaven and the earth."
    )
    assert context["context"]["next"]["text"] == (
        "And the evening and the morning were the first day."
    )
    assert context["locations"] == [{
        "name": "Eden",
        "secondary_name": "Garden of Eden",
        "region": "Canaan",
        "latitude": 31.8,
        "longitude": 35.2,
        "note": None,
    }]
    assert context["regions"] == [{
        "name": "Canaan",
        "keywords": "Jerusalem,Bethel",
        "description": "The promised land between the Jordan and the sea.",
    }]


def test_fetch_graph_context_at_chapter_start_has_no_verse_above(explain_db):
    context = explain_service.fetch_graph_context("GEN", 1, 1, "KJV")

    assert context["context"]["previous"] is None
    assert context["context"]["next"]["verse_number"] == 2


def test_fetch_graph_context_reports_unknown_references(explain_db):
    with pytest.raises(explain_service.ExplainError, match="No book"):
        explain_service.fetch_graph_context("XXX", 1, 1)
    with pytest.raises(explain_service.ExplainError, match="No verse"):
        explain_service.fetch_graph_context("REV", 9, 9)


def test_explain_endpoint_requires_api_key(explain_db):
    result = api.explain("GEN", 1, 2, version_code="KJV")

    assert "error" in result
    assert "FAITHTECH_API_KEY" in result["error"]


def test_explain_endpoint_returns_context_and_chat_reply(explain_db, monkeypatch):
    monkeypatch.setenv("FAITHTECH_API_KEY", "test-token")
    calls = {}

    def fake_post(url, headers=None, json=None, timeout=None):  # noqa: A002
        calls["url"] = url
        calls["headers"] = headers
        calls["payload"] = json
        return _ok_response("Eden marks the source of the river that watered the garden.")

    monkeypatch.setattr(explain_service.httpx, "post", fake_post)

    result = api.explain("gen", 1, 2, version_code="kjv")

    assert result["reference"] == {
        "book_code": "GEN", "book_name": "Genesis",
        "testament": "Old Testament", "chapter": 1, "verse_number": 2,
        "version": "KJV",
    }
    assert result["locations"][0]["name"] == "Eden"
    assert result["regions"][0]["name"] == "Canaan"
    assert result["explanation"] == (
        "Eden marks the source of the river that watered the garden."
    )
    # The prompt pair carries the system prompt and the graph facts.
    assert calls["url"] == (
        "https://mq2yi3izzu14g5-8000.proxy.runpod.net/v1/chat/completions"
    )
    assert calls["headers"]["Authorization"] == "Bearer test-token"
    system, user = calls["payload"]["messages"]
    assert system["role"] == "system"
    assert "Bibliagraphia" in system["content"]
    for fragment in (
        "GEN 1:2",
        "And a river went out of Eden to water the garden.",
        "In the beginning God created the heaven and the earth.",
        "And the evening and the morning were the first day.",
        "Eden (also known as: Garden of Eden)",
        "Region: Canaan",
        "The promised land between the Jordan and the sea.",
    ):
        assert fragment in user["content"], f"missing prompt fragment: {fragment}"
    assert calls["payload"]["model"] == explain_service.DEFAULT_MODEL


def test_explain_endpoint_surfaces_upstream_api_failures(explain_db, monkeypatch):
    monkeypatch.setenv("FAITHTECH_API_KEY", "test-token")

    def fake_post(url, headers=None, json=None, timeout=None):  # noqa: A002
        return _FakeResponse(status_code=401, text='{"error": "invalid key"}')

    monkeypatch.setattr(explain_service.httpx, "post", fake_post)

    result = api.explain("GEN", 1, 2, version_code="KJV")

    assert "error" in result
    assert "401" in result["error"]
    assert "invalid key" in result["error"]


def test_chat_completion_strips_the_models_planning_block(monkeypatch):
    monkeypatch.setenv("FAITHTECH_API_KEY", "test-token")
    thinking = (
        "<_thinking>The user wants an explanation of the verse. Let me plan."
        " Passage, place, person. Done.<_end>\n\n"
    )
    reply = "Naaman's anger turns on a simple comparison between two rivers."

    def fake_post(url, headers=None, json=None, timeout=None):  # noqa: A002
        return _ok_response(thinking + reply)

    monkeypatch.setattr(explain_service.httpx, "post", fake_post)

    assert explain_service.chat_completion([{"role": "user", "content": "x"}]) == reply


def test_chat_completion_keeps_reply_after_last_end_tag(monkeypatch):
    """Planning sometimes arrives as loose notes followed by a tagged
    block; the reply is always whatever follows the final <_end>."""
    monkeypatch.setenv("FAITHTECH_API_KEY", "test-token")
    content = (
        "The user wants an explanation. Cover passage, place, person.\n"
        "<_thinking>Also: keep it brief, no headings.<_end>\n\n"
        "Naaman's servants persuade him to humble himself and wash."
    )

    def fake_post(url, headers=None, json=None, timeout=None):  # noqa: A002
        return _ok_response(content)

    monkeypatch.setattr(explain_service.httpx, "post", fake_post)

    assert explain_service.chat_completion([{"role": "user", "content": "x"}]) == (
        "Naaman's servants persuade him to humble himself and wash."
    )


def test_chat_completion_flags_truncated_planning(monkeypatch):
    monkeypatch.setenv("FAITHTECH_API_KEY", "test-token")
    calls = {"n": 0}

    def fake_post(url, headers=None, json=None, timeout=None):  # noqa: A002
        calls["n"] += 1
        return _ok_response("<_thinking>plan, plan, plan and never stop")

    monkeypatch.setattr(explain_service.httpx, "post", fake_post)

    with pytest.raises(explain_service.ExplainError, match="planning"):
        explain_service.chat_completion([{"role": "user", "content": "x"}])
    assert calls["n"] == 2  # one automatic retry before giving up


def test_chat_completion_flags_finish_reason_length(monkeypatch):
    monkeypatch.setenv("FAITHTECH_API_KEY", "test-token")

    def fake_post(url, headers=None, json=None, timeout=None):  # noqa: A002
        return _FakeResponse(payload={
            "choices": [{
                "message": {"content": "<_thinking>almost there<_end> A truncated repl"},
                "finish_reason": "length",
            }]
        })

    monkeypatch.setattr(explain_service.httpx, "post", fake_post)

    with pytest.raises(explain_service.ExplainError, match="token budget"):
        explain_service.chat_completion([{"role": "user", "content": "x"}])


def test_explain_endpoint_reports_missing_verse(explain_db, monkeypatch):
    monkeypatch.setenv("FAITHTECH_API_KEY", "test-token")

    result = api.explain("REV", 9, 9)

    assert result == {
        "error": "No verse REV 9:9 in the graph. Try /search or "
                  "/reader/catalog to find a valid reference."
    }
