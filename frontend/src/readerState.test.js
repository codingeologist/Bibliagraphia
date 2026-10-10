import assert from "node:assert/strict";
import test from "node:test";
import { loadReaderState, readerStateHref, saveReaderState, validReaderState } from "./readerState.js";

const position = { book: "RUT", bookName: "Ruth", chapter: 1, version: "KJV", comparisons: ["DRB", "VUL"] };

test("reading links preserve chapter, translation and all comparisons", () => {
  const url = new URL(readerStateHref(position), "http://localhost");
  assert.equal(url.pathname, "/read");
  assert.equal(url.searchParams.get("book"), "RUT");
  assert.equal(url.searchParams.get("chapter"), "1");
  assert.equal(url.searchParams.get("version"), "KJV");
  assert.deepEqual(url.searchParams.getAll("compare"), ["DRB", "VUL"]);
});

test("reading positions reject invalid chapters and incomplete data", () => {
  assert.equal(validReaderState(position), true);
  for (const invalid of [null, {}, { ...position, chapter: 0 }, { ...position, chapter: "1" },
    { ...position, comparisons: [null] }, { ...position, book: "../" }]) {
    assert.equal(validReaderState(invalid), false);
  }
});

test("reading position persists and missing or corrupt storage is handled explicitly", () => {
  const previousStorage = globalThis.localStorage;
  const previousWarn = console.warn;
  let raw = null;
  const warnings = [];
  globalThis.localStorage = { getItem: () => raw, setItem: (_, value) => { raw = value; } };
  console.warn = (...args) => warnings.push(args);
  try {
    assert.equal(loadReaderState(), null);
    saveReaderState(position);
    assert.deepEqual(loadReaderState(), position);
    raw = "{";
    assert.equal(loadReaderState(), null);
    raw = "{}";
    assert.equal(loadReaderState(), null);
    globalThis.localStorage.getItem = () => { throw new Error("Storage unavailable"); };
    assert.equal(loadReaderState(), null);
    globalThis.localStorage.setItem = () => { throw new Error("Storage unavailable"); };
    saveReaderState(position);
    assert.equal(warnings.length, 4);
  } finally {
    console.warn = previousWarn;
    globalThis.localStorage = previousStorage;
  }
});
