import assert from "node:assert/strict";
import test from "node:test";
import { previewNeighbours } from "./graphPreview.js";

test("preview includes every available type before filling remaining spaces", () => {
  const nodes = Array.from({ length: 20 }, (_, index) => ({ id: `verse:${index}`, label: "verse" }));
  for (const label of ["figure", "location", "region", "book", "version"]) {
    nodes.push({ id: label, label });
  }
  const shown = previewNeighbours(nodes);
  assert.equal(shown.length, 6);
  assert.equal(new Set(shown.map((node) => node.label)).size, 6);
});

test("preview fills spare spaces without duplicates or inventing missing types", () => {
  const nodes = [
    { id: "verse:1", label: "verse" }, { id: "verse:2", label: "verse" },
    { id: "figure:1", label: "figure" }, { id: "verse:1", label: "verse" },
  ];
  assert.equal(previewNeighbours(nodes).length, 3);
  assert.deepEqual(previewNeighbours([]), []);
  assert.equal(previewNeighbours(nodes, 1).length, 2);
});
