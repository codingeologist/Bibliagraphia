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


@pytest.mark.parametrize("label", ["book", "verse", "location", "version", "region"])
def test_search_initial_suggestions_are_limited_per_type(reader_db, label):
    result = api.search(q="", label=label, limit=3)
    assert 0 < len(result["results"]) <= 3
    assert all(node["label"] == label for node in result["results"])


def test_search_filters_suggestions_by_text_and_type(reader_db):
    result = api.search(q="Eden", label="location", limit=3)
    assert len(result["results"]) == 2
    assert all("Eden" in node["name"] for node in result["results"])
    assert api.search(q="unmatched", label="book", limit=3)["results"] == []


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


@pytest.fixture
def dense_graph(reader_db):
    conn = duckdb.connect(str(reader_db))
    for index in range(30):
        place_id = f"location:Neighbour:{index}"
        verse_id = f"verse:KJV:GEN:2:{index + 1}"
        conn.executemany(
            "INSERT INTO nodes VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            [
                (place_id, "location", f"Neighbour {index}", None, "GEN", 2, index + 1, "{}"),
                (verse_id, "verse", "Genesis", "KJV", "GEN", 2, index + 1, "{}"),
            ],
        )
        conn.executemany(
            "INSERT INTO edges VALUES (?, ?, ?)",
            [
                ("region:Canaan", place_id, "location_in_region"),
                (verse_id, place_id, "location_in_verse"),
                ("book:GEN", verse_id, "verse_in_book"),
                ("version:KJV", verse_id, "verse_in_version"),
                (verse_id, "location:Eden:GEN:1:1:1", "location_in_verse"),
            ],
        )
    conn.close()


def test_balanced_graph_preserves_passages_and_limits_region_fanout(dense_graph):
    result = api.graph("Eden", "location", hops=3, balanced=True)
    assert "error" not in result
    assert sum(node["label"] == "verse" for node in result["nodes"]) >= 10
    assert result["hidden_connections"]["region:Canaan"]["location"] > 0
    assert result["count"] < 200
    assert result["branch_limited"]
    assert not result["truncated"]
    assert all(link["source"] in {node["id"] for node in result["nodes"]}
               and link["target"] in {node["id"] for node in result["nodes"]}
               for link in result["links"])


def test_balanced_graph_show_more_widens_centre_branch(dense_graph):
    initial = api.graph("Eden", "location", hops=1, balanced=True)
    expanded = api.graph("Eden", "location", hops=1, balanced=True, branch_expansion=2)
    assert initial["hidden_connections"][initial["start_id"]]["verse"] == 21
    assert expanded["hidden_connections"][expanded["start_id"]]["verse"] == 11
    assert expanded["count"] == initial["count"] + 10
    assert expanded["branch_expansion"] == 2


def test_balanced_graph_keeps_total_cap_and_legacy_expansion(dense_graph):
    capped = api.graph("Eden", "location", hops=3, limit=15, balanced=True)
    assert capped["count"] == 15
    assert capped["truncated"]
    legacy = api.graph("Eden", "location", hops=1)
    assert sum(node["label"] == "verse" for node in legacy["nodes"]) == 31
    assert not legacy["hidden_connections"]
    invalid = api.graph("Eden", "location", balanced=True, branch_expansion=21)
    assert "error" in invalid


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
