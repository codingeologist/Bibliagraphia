import assert from "node:assert/strict";
import test from "node:test";
import { mentionSegments } from "./readerMentions.js";

test("people and places keep exact text, word boundaries and repeat mentions", () => {
  const text = "Naomi's Ruth went to Moab; Ruth returned. Ruthless is not Ruth.";
  const ruth = { id: "figure:Ruth", name: "Ruth" };
  const segments = mentionSegments(text, [{ id: "place:Moab", name: "Moab" }], [
    ruth, { id: "figure:Naomi", name: "Naomi" },
  ]);
  assert.equal(segments.map((part) => part.text).join(""), text);
  assert.deepEqual(segments.filter((part) => part.figures).map((part) => part.text),
    ["Naomi", "Ruth", "Ruth", "Ruth"]);
  assert.equal(segments.find((part) => part.locations).text, "Moab");
});

test("explicit aliases and longest matches work without using broad keywords", () => {
  const mary = { id: "figure:Mary Magdalene", name: "Mary Magdalene" };
  const segments = mentionSegments("Noëmi met Mary Magdalene in Moab.", [], [
    { id: "figure:Naomi", name: "Naomi", aliases: ["Noëmi"], keywords: "Moab" },
    { id: "figure:Mary", name: "Mary" }, mary,
  ]);
  assert.deepEqual(segments.filter((part) => part.figures).map((part) => part.figures[0].id),
    ["figure:Naomi", mary.id]);
});

test("place actions win colliding names and absent people stay plain", () => {
  const place = { id: "place:Judah", name: "Judah" };
  const segments = mentionSegments("Judah and Naomi", [place], [{ id: "figure:Judah", name: "Judah" }]);
  assert.deepEqual(segments[0].locations, [place]);
  assert.equal(segments[1].text, " and Naomi");
  assert.deepEqual(mentionSegments("No matches"), [{ text: "No matches" }]);
});
