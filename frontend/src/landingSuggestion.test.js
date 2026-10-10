import assert from "node:assert/strict";
import test from "node:test";
import { nextSuggestedBook, suggestedBooks } from "./landingSuggestion.js";
import { readerStateHref } from "./readerState.js";

test("suggestions rotate through each book without consecutive repeats and wrap around", () => {
  let previous = null;
  const seen = new Set();
  for (let index = 0; index < suggestedBooks.length; index++) {
    const suggestion = nextSuggestedBook(previous);
    assert.notEqual(suggestion.book, previous);
    seen.add(suggestion.book);
    previous = suggestion.book;
  }
  assert.equal(seen.size, suggestedBooks.length);
  assert.deepEqual(nextSuggestedBook(previous), suggestedBooks[0]);
  assert.deepEqual(nextSuggestedBook("unknown"), suggestedBooks[0]);
});

test("reading a suggestion preserves the selected book and both translations", () => {
  for (const suggestion of suggestedBooks) {
    const url = new URL(readerStateHref({
      book: suggestion.book, chapter: 1, version: "KJV", comparisons: ["DRB"],
    }), "http://localhost");
    assert.equal(url.searchParams.get("book"), suggestion.book);
    assert.equal(url.searchParams.get("chapter"), "1");
    assert.equal(url.searchParams.get("version"), "KJV");
    assert.deepEqual(url.searchParams.getAll("compare"), ["DRB"]);
  }
});
