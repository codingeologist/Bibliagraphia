import assert from "node:assert/strict";
import test from "node:test";
import { describeNode, nodeTypeName, pathNode, pathRelationshipDescription, relationshipDescription, relationshipHref } from "./nodeLinks.js";

test("links preserve exact verse translation and reference", () => {
  const node = {
    id: "verse:KJV:GEN:1:1", label: "verse", name: "Genesis",
    book_code: "GEN", chapter: 1, verse_number: 1, version_code: "KJV",
  };
  assert.equal(describeNode(node), "Genesis 1:1 · KJV");
  const url = new URL(relationshipHref(node), "http://localhost");
  assert.equal(url.pathname, "/relationships");
  assert.equal(url.searchParams.get("graph_node_id"), node.id);
  assert.equal(url.searchParams.get("graph_label"), "verse");
  assert.equal(url.searchParams.get("graph_hops"), "3");
});

test("place links distinguish repeated mentions", () => {
  const node = { id: "location:Haran:GEN:12:4:1", label: "location", name: "Haran", book_code: "GEN", chapter: 12, verse_number: 4 };
  assert.equal(describeNode(node), "Haran · GEN 12:4");
  assert.equal(new URL(relationshipHref(node), "http://localhost").searchParams.get("graph_node_id"), node.id);
});

test("zero-hop paths retain their node type instead of assuming book", () => {
  const node = pathNode("version:KJV");
  assert.equal(node.label, "version");
  assert.equal(new URL(relationshipHref(node), "http://localhost").searchParams.get("graph_label"), "version");
});

test("structured path nodes are unchanged", () => {
  const node = { id: "region:Syria", label: "region", name: "Syria" };
  assert.equal(pathNode(node), node);
});

test("node types use reader-friendly names", () => {
  assert.equal(nodeTypeName("verse"), "Passage");
  assert.equal(nodeTypeName("location"), "Place");
  assert.equal(nodeTypeName("version"), "Translation");
  assert.equal(nodeTypeName("figure"), "Figure");
});

test("connection explanations respect both directions of a walk", () => {
  const cases = [
    ["verse_in_book", "book", "verse", "Contains passage", "Belongs to book"],
    ["verse_in_version", "version", "verse", "Contains passage", "Available in translation"],
    ["location_in_verse", "verse", "location", "Mentions place", "Mentioned in passage"],
    ["location_in_region", "region", "location", "Contains place", "Located in region"],
    ["figure_in_verse", "verse", "figure", "Mentions figure", "Mentioned in passage"],
  ];
  for (const [label, sourceType, targetType, forward, reverse] of cases) {
    assert.equal(pathRelationshipDescription(label, sourceType), forward);
    assert.equal(pathRelationshipDescription(label, targetType), reverse);
    assert.equal(relationshipDescription(label, true), forward);
    assert.equal(relationshipDescription(label, false), reverse);
  }
});
