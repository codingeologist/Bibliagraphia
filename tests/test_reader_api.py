from __future__ import annotations

import json

import duckdb
import pytest

from app import api


@pytest.fixture
def reader_db(tmp_path, monkeypatch):
    path = tmp_path / "reader.db"
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
            ("book:EXO", "book", "Exodus", None, "EXO", None, None,
             json.dumps({"testament": "Old Testament"})),
            ("version:KJV", "version", "kjv", "KJV", None, None, None,
             json.dumps({"full_name": "King James Bible"})),
            ("version:DRB", "version", "rheims", "DRB", None, None, None,
             json.dumps({"full_name": "Douay-Rheims Bible"})),
            ("verse:KJV:GEN:1:2", "verse", "Genesis", "KJV", "GEN", 1, 2,
             json.dumps({"text": "And the earth was without form."})),
            ("verse:DRB:GEN:1:2", "verse", "Genesis", "DRB", "GEN", 1, 2,
             json.dumps({"text": "And the earth was void and empty."})),
            ("verse:KJV:GEN:1:1", "verse", "Genesis", "KJV", "GEN", 1, 1,
             json.dumps({"text": "In the beginning God created the heaven and the earth at Eden."})),
            ("verse:DRB:GEN:1:1", "verse", "Genesis", "DRB", "GEN", 1, 1,
             json.dumps({"text": "In the beginning God created heaven and earth at Eden."})),
            ("location:Eden:GEN:1:1:1", "location", "Eden", None, "GEN", 1, 1,
             json.dumps({
                 "secondary_name": "Garden of Eden",
                 "region": "Canaan",
                 "latitude": 31.8,
                 "longitude": 35.2,
             })),
            ("location:Garden:GEN:1:2:1", "location", "Garden of Eden", None, "GEN", 1, 2,
             json.dumps({
                 "secondary_name": "Eden",
                 "region": "Canaan",
                 "latitude": 31.8,
                 "longitude": 35.2,
             })),
            ("region:Canaan", "region", "Canaan", None, None, None, None,
             json.dumps({})),
        ],
    )
    conn.executemany(
        "INSERT INTO edges VALUES (?, ?, ?)",
        [
            ("verse:KJV:GEN:1:1", "location:Eden:GEN:1:1:1", "location_in_verse"),
            ("region:Canaan", "location:Eden:GEN:1:1:1", "location_in_region"),
        ],
    )
    conn.close()
    monkeypatch.setattr(api, "DB_PATH", str(path))
    return path


def test_reader_catalog_lists_available_versions_and_chapters(reader_db):
    result = api.reader_catalog("KJV")

    assert result["books"] == [
        {"code": "GEN", "name": "Genesis", "testament": "Old Testament"},
        {"code": "EXO", "name": "Exodus", "testament": "Old Testament"},
    ]
    assert result["chapters"] == {"GEN": [1]}
    assert {version["code"] for version in result["versions"]} == {"KJV", "DRB"}


def test_reader_chapter_returns_verses_in_order_for_selected_translation(reader_db):
    result = api.read_chapter("gen", 1, "kjv")

    assert result["book_name"] == "Genesis"
    assert result["version"] == "KJV"
    assert result["verses"] == [
        {"number": 1, "text": "In the beginning God created the heaven and the earth at Eden."},
        {"number": 2, "text": "And the earth was without form."},
    ]
    assert result["locations"] == [
        {
            "id": "location:Eden:GEN:1:1:1",
            "verse_number": 1,
            "name": "Eden",
            "aliases": ["Eden", "Garden of Eden"],
            "region": "Canaan",
        },
        {
            "id": "location:Garden:GEN:1:2:1",
            "verse_number": 2,
            "name": "Garden of Eden",
            "aliases": ["Garden of Eden", "Eden"],
            "region": "Canaan",
        },
    ]


def test_reader_chapter_returns_empty_passage_for_unavailable_text(reader_db):
    result = api.read_chapter("EXO", 1, "KJV")

    assert result["book_name"] == "Exodus"
    assert result["verses"] == []


def test_graph_can_resolve_exact_location_instance_by_id(reader_db):
    result = api.graph(
        "Eden", "location", node_id="location:Eden:GEN:1:1:1"
    )

    assert result["start_id"] == "location:Eden:GEN:1:1:1"
    assert {link["label"] for link in result["links"]} == {
        "location_in_verse",
        "location_in_region",
    }
    location = next(node for node in result["nodes"] if node["id"] == result["start_id"])
    assert location["attrs"]["latitude"] == 31.8
    assert location["attrs"]["longitude"] == 35.2


def test_place_relations_group_other_translations_and_matching_references(reader_db):
    result = api.place_relations("location:Eden:GEN:1:1:1")

    assert result["place"]["name"] == "Eden"
    assert result["place"]["attrs"] == {"latitude": 31.8, "longitude": 35.2}
    assert {item["code"] for item in result["reference"]["translations"]} == {"KJV", "DRB"}
    assert len(result["mentions"]) == 1
    mention = result["mentions"][0]
    assert (mention["book_name"], mention["chapter"], mention["verse_number"]) == (
        "Genesis", 1, 2,
    )
    assert {item["code"] for item in mention["translations"]} == {"KJV", "DRB"}
    assert mention["location_ids"] == [{"id": "location:Garden:GEN:1:2:1"}]


def test_place_relations_reports_unknown_location(reader_db):
    assert api.place_relations("missing") == {"error": "Location mention not found."}


def test_map_places_groups_aliases_and_repeated_mention_coordinates(reader_db):
    result = api.map_places()

    assert result["count"] == 1
    place = result["places"][0]
    assert place["id"] == "location:Eden:GEN:1:1:1"
    assert place["name"] == "Eden"
    assert place["aliases"] == ["Eden", "Garden of Eden"]
    assert place["region"] == "Canaan"
    assert (place["lat"], place["lng"]) == (31.8, 35.2)
    assert place["mention_count"] == 2
