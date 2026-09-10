// Bibliagraphia frontend — plain JS, no build step. Talks to the FastAPI
// app on the same origin. Keeps state minimal: each section is independent.
"use strict";

const $ = (id) => document.getElementById(id);

// ---- health -------------------------------------------------------------
async function health() {
  try {
    const r = await fetch("/health");
    const j = await r.json();
    $("health").textContent =
      `db: ${j.db}  ·  ${j.empty ? "EMPTY — run scripts/build_db.py" : "loaded"}`;
  } catch (e) {
    $("health").textContent = "API unreachable";
  }
}

// ---- helpers ------------------------------------------------------------
function li(node, onclick) {
  const el = document.createElement("li");
  el.textContent = node.name || node.id;
  el.innerHTML = `<span class="badge">${node.label}</span>` + (node.name || node.id);
  if (node.book_code) {
    const ref = ` ${node.book_code}${node.chapter ? " " + node.chapter + ":" + node.verse_number : ""}`;
    el.innerHTML += `<span class="hint">${ref}</span>`;
  }
  if (onclick) el.onclick = () => onclick(node);
  return el;
}
function clearList(ul) { while (ul.firstChild) ul.removeChild(ul.firstChild); }
function showErr(ul, msg) {
  clearList(ul);
  const e = document.createElement("li");
  e.className = "error"; e.textContent = msg; ul.appendChild(e);
}

// ---- search -------------------------------------------------------------
async function doSearch() {
  const q = $("search-q").value.trim();
  const label = $("search-label").value;
  if (!q) return;
  const ul = $("search-results"); clearList(ul);
  const r = await fetch(`/search?q=${encodeURIComponent(q)}${label ? "&label=" + label : ""}`);
  const j = await r.json();
  if (j.error) return showErr(ul, j.error);
  if (!j.results.length) { ul.innerHTML = "<li class='hint'>no matches</li>"; return; }
  for (const n of j.results) {
    ul.appendChild(li(n, (node) => {
      // Clicking a result pre-fills the relevant section.
      if (node.label === "book" || node.label === "region" || node.label === "version") {
        $("trav-node").value = node.name;
        $("trav-label").value = node.label;
        $("trav-edge").value =
          node.label === "book" ? "verse_in_book" :
          node.label === "region" ? "location_in_region" : "verse_in_version";
      } else if (node.label === "verse") {
        $("vs-book").value = node.book_code;
        $("vs-ch").value = node.chapter;
        $("vs-vr").value = node.verse_number;
      }
    }));
  }
}

// ---- traverse -----------------------------------------------------------
async function doTraverse() {
  const start_node = $("trav-node").value.trim();
  const label = $("trav-label").value;
  const edge = $("trav-edge").value;
  if (!start_node) return;
  const ul = $("trav-results"); clearList(ul);
  $("trav-summary").textContent = "querying…";
  const r = await fetch("/traverse", {
    method: "POST", headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ start_node, label, edge }),
  });
  const j = await r.json();
  if (j.error) { $("trav-summary").textContent = ""; return showErr(ul, j.error); }
  $("trav-summary").textContent = `${j.count} descendants of "${j.start}" via ${j.edge}`;
  const shown = j.nodes.slice(0, 200);
  for (const n of shown) {
    ul.appendChild(li(n, (node) => {
      if (node.label === "verse") {
        $("vs-book").value = node.book_code;
        $("vs-ch").value = node.chapter;
        $("vs-vr").value = node.verse_number;
      } else if (node.label === "location") {
        $("path-src").value = node.name;
        $("path-src-label").value = "location";
        $("path-tgt").value = (node.attrs && node.attrs.region) || "";
        $("path-tgt-label").value = "region";
      }
    }));
  }
  if (j.nodes.length > 200) {
    const more = document.createElement("li");
    more.className = "hint";
    more.textContent = `… ${j.nodes.length - 200} more omitted`;
    ul.appendChild(more);
  }
}

// ---- path ---------------------------------------------------------------
async function doPath() {
  const source = $("path-src").value.trim();
  const source_label = $("path-src-label").value;
  const target = $("path-tgt").value.trim();
  const target_label = $("path-tgt-label").value;
  if (!source || !target) return;
  const ol = $("path-results"); clearList(ol);
  const r = await fetch("/path", {
    method: "POST", headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ source, source_label, target, target_label }),
  });
  const j = await r.json();
  if (j.error) { const li = document.createElement("li"); li.className = "error"; li.textContent = j.error; ol.appendChild(li); return; }
  if (!j.found) { ol.innerHTML = "<li class='hint'>no path within depth 10</li>"; return; }
  j.path.forEach((n, i) => {
    const li = document.createElement("li");
    li.innerHTML = `<span class="badge">${n.label}</span>${n.name || n.id}` +
      (n.book_code ? ` <span class="hint">${n.book_code} ${n.chapter || ""}${n.verse_number ? ":" + n.verse_number : ""}</span>` : "");
    ol.appendChild(li);
    if (j.edges[i]) {
      const e = document.createElement("li");
      e.className = "edge"; e.textContent = "↳ " + j.edges[i].label;
      ol.appendChild(e);
    }
  });
}

// ---- cross-version verse ------------------------------------------------
async function doVerse() {
  const book_code = $("vs-book").value.trim().toUpperCase();
  const chapter = parseInt($("vs-ch").value, 10);
  const verse_number = parseInt($("vs-vr").value, 10);
  if (!book_code || !chapter || !verse_number) return;
  const box = $("verse-results"); box.innerHTML = "querying…";
  const r = await fetch(`/verse?book_code=${encodeURIComponent(book_code)}&chapter=${chapter}&verse_number=${verse_number}`);
  const j = await r.json();
  if (!j.verses || !j.verses.length) { box.innerHTML = "<span class='hint'>no verse found</span>"; return; }
  box.innerHTML = "";
  for (const v of j.verses) {
    const row = document.createElement("div");
    row.className = "vrow";
    row.innerHTML = `<div class="vname">${v.version}</div><div>${v.text || ""}</div>`;
    box.appendChild(row);
  }
}

// ---- wire up ------------------------------------------------------------
$("search-btn").onclick = doSearch;
$("search-q").addEventListener("keydown", (e) => { if (e.key === "Enter") doSearch(); });
$("trav-btn").onclick = doTraverse;
$("path-btn").onclick = doPath;
$("vs-btn").onclick = doVerse;
health();
