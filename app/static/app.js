// Bibliagraphia frontend — plain JS, no build step. Talks to the FastAPI
// app on the same origin. Keeps state minimal: each section is independent.
"use strict";

const $ = (id) => document.getElementById(id);

// ---- theme (dark mode) ---------------------------------------------------
// Applied on <html> so CSS variables cascade everywhere; persisted in
// localStorage, defaulting to the OS preference.
function applyTheme(dark) {
  document.documentElement.classList.toggle("dark", dark);
  $("theme-toggle").textContent = dark ? "☀️" : "🌙";
}
applyTheme(
  localStorage.getItem("theme")
    ? localStorage.getItem("theme") === "dark"
    : window.matchMedia("(prefers-color-scheme: dark)").matches
);
$("theme-toggle").onclick = () => {
  const dark = !document.documentElement.classList.contains("dark");
  localStorage.setItem("theme", dark ? "dark" : "light");
  applyTheme(dark);
  drawGraph(); // canvas colors are theme-dependent
};

// CSS variables used by the canvas renderer.
function cssVar(name) {
  return getComputedStyle(document.documentElement).getPropertyValue(name).trim();
}

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
        // Prefer codes where they exist — canonical, unambiguous, and what
        // the API's resolver tries first.
        const code = node.label === "book" ? node.book_code
          : node.label === "version" ? node.version_code : null;
        $("trav-node").value = code || node.name;
        $("trav-label").value = node.label;
        $("trav-edge").value =
          node.label === "book" ? "verse_in_book" :
          node.label === "region" ? "location_in_region" : "verse_in_version";
        // Also offer it as a path source and a graph center.
        $("path-src").value = code || node.name;
        $("path-src-label").value = node.label;
        $("graph-node").value = code || node.name;
        $("graph-label").value = node.label;
        if (node.label === "region" || node.label === "book") {
          $("map-scope").value = node.label;
          $("map-q").value = code || node.name;
        }
      } else if (node.label === "verse") {
        $("vs-book").value = node.book_code;
        $("vs-ch").value = node.chapter;
        $("vs-vr").value = node.verse_number;
        $("graph-node").value = node.name || node.book_code;
        $("graph-label").value = "verse";
      } else {
        // locations have no codes and can't be traversed — only graph them.
        $("graph-node").value = node.name;
        $("graph-label").value = "location";
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

// ---- graph explorer -------------------------------------------------------
// Minimal dependency-free force-directed layout: repulsion between all node
// pairs (O(n²) is fine for ≤500 nodes), springs along links, gravity toward
// the center, velocity damping. Rendered on a canvas with pan/zoom/hover.

// Node colors are theme-aware (defined as CSS variables, brightened in dark mode).
const LABEL_COLORS = {
  book: "--node-book", version: "--node-version", region: "--node-region",
  location: "--node-location", verse: "--node-verse",
};
function nodeColor(label) { return cssVar(LABEL_COLORS[label]) || "#999"; }

const g = {
  nodes: [], links: [], byId: new Map(),
  view: { x: 0, y: 0, scale: 1 },
  sim: null, hovered: null, running: false,
};

function graphSize() {
  const c = $("graph-canvas");
  const r = c.getBoundingClientRect();
  return { w: r.width, h: r.height };
}

function fitView() {
  const { w, h } = graphSize();
  g.view = { x: w / 2, y: h / 2, scale: 1 };
  if (!g.nodes.length) return;
  let minX = 1e9, maxX = -1e9, minY = 1e9, maxY = -1e9;
  for (const n of g.nodes) {
    minX = Math.min(minX, n.x); maxX = Math.max(maxX, n.x);
    minY = Math.min(minY, n.y); maxY = Math.max(maxY, n.y);
  }
  const pad = 50;
  const sx = (w - pad) / Math.max(maxX - minX, 1);
  const sy = (h - pad) / Math.max(maxY - minY, 1);
  g.view.scale = Math.min(sx, sy, 2.5);
  g.view.x = w / 2 - ((minX + maxX) / 2) * g.view.scale;
  g.view.y = h / 2 - ((minY + maxY) / 2) * g.view.scale;
}

function nodeRadius(n) { return n.id === g.startId ? 10 : n.label === "verse" ? 3 : 6; }

function drawGraph() {
  const c = $("graph-canvas");
  const ctx = c.getContext("2d");
  const dpr = window.devicePixelRatio || 1;
  const { w, h } = graphSize();
  if (c.width !== Math.round(w * dpr) || c.height !== Math.round(h * dpr)) {
    c.width = Math.round(w * dpr); c.height = Math.round(h * dpr);
  }
  ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
  ctx.clearRect(0, 0, w, h);
  ctx.save();
  ctx.translate(g.view.x, g.view.y);
  ctx.scale(g.view.scale, g.view.scale);

  // links
  ctx.strokeStyle = cssVar("--edge"); ctx.lineWidth = 1 / g.view.scale;
  ctx.beginPath();
  for (const l of g.links) {
    const a = g.byId.get(l.source), b = g.byId.get(l.target);
    if (!a || !b) continue;
    ctx.moveTo(a.x, a.y); ctx.lineTo(b.x, b.y);
  }
  ctx.stroke();

  // nodes
  for (const n of g.nodes) {
    ctx.beginPath();
    ctx.arc(n.x, n.y, nodeRadius(n) / (g.hovered === n ? 0.8 : 1), 0, Math.PI * 2);
    ctx.fillStyle = nodeColor(n.label);
    ctx.fill();
    if (n === g.hovered || n.id === g.startId || g.view.scale > 1.6) {
      ctx.fillStyle = cssVar("--graph-text");
      ctx.font = `${10 / g.view.scale}px sans-serif`;
      ctx.textAlign = "center";
      let label = n.name || n.id;
      if (n.label === "verse" && n.chapter) {
        // Include the version: verse nodes are per-translation, so
        // "John 3:16" alone is ambiguous when several versions are drawn.
        label = `${n.name || n.book_code} ${n.chapter}:${n.verse_number}` +
                (n.version_code ? ` (${n.version_code})` : "");
      } else if (n.label === "version" && n.version_code) {
        label = `${n.name || ""} (${n.version_code})`.trim();
      }
      ctx.fillText(label, n.x, n.y - nodeRadius(n) - 3 / g.view.scale);
    }
  }
  ctx.restore();
}

function tick() {
  const nodes = g.nodes, links = g.links;
  const REP = 6000, SPRING = 0.02, GRAVITY = 0.01, DAMP = 0.85;
  for (const n of nodes) { n.vx = (n.vx || 0) * DAMP; n.vy = (n.vy || 0) * DAMP; }
  // repulsion
  for (let i = 0; i < nodes.length; i++) {
    for (let j = i + 1; j < nodes.length; j++) {
      const a = nodes[i], b = nodes[j];
      let dx = a.x - b.x, dy = a.y - b.y;
      let d2 = dx * dx + dy * dy;
      if (d2 < 1) { dx = (Math.random() - .5); dy = (Math.random() - .5); d2 = dx * dx + dy * dy; }
      const f = REP / d2;
      const d = Math.sqrt(d2);
      const fx = (dx / d) * f, fy = (dy / d) * f;
      a.vx += fx; a.vy += fy; b.vx -= fx; b.vy -= fy;
    }
  }
  // springs
  for (const l of links) {
    const a = g.byId.get(l.source), b = g.byId.get(l.target);
    if (!a || !b) continue;
    const dx = b.x - a.x, dy = b.y - a.y;
    const d = Math.max(Math.sqrt(dx * dx + dy * dy), 1);
    const f = (d - 70) * SPRING;
    const fx = (dx / d) * f, fy = (dy / d) * f;
    a.vx += fx; a.vy += fy; b.vx -= fx; b.vy -= fy;
  }
  // gravity toward origin
  for (const n of nodes) {
    n.vx -= n.x * GRAVITY; n.vy -= n.y * GRAVITY;
    const maxV = 8;
    n.vx = Math.max(-maxV, Math.min(maxV, n.vx));
    n.vy = Math.max(-maxV, Math.min(maxV, n.vy));
    n.x += n.vx; n.y += n.vy;
  }
  drawGraph();
  if (g.running) g.sim = requestAnimationFrame(tick);
}

function stopSim() { g.running = false; if (g.sim) cancelAnimationFrame(g.sim); }

async function doGraph(overrideNode, overrideLabel) {
  // Guard against being used directly as an event handler (the click event
  // would land in overrideNode).
  if (typeof overrideNode !== "string") overrideNode = null;
  const node = (overrideNode || $("graph-node").value).trim();
  const label = overrideLabel || $("graph-label").value;
  const hops = parseInt($("graph-hops").value, 10);
  if (!node) return;
  $("graph-summary").textContent = "querying…";
  const r = await fetch(`/graph?node=${encodeURIComponent(node)}&label=${encodeURIComponent(label)}&hops=${hops}`);
  const j = await r.json();
  if (j.error) { stopSim(); $("graph-summary").textContent = ""; return alert(j.error); }
  if (!j.count) { $("graph-summary").textContent = "no nodes"; return; }

  $("graph-node").value = node;
  g.startId = j.start_id;
  g.byId = new Map();
  g.nodes = j.nodes.map((n, i) => {
    const angle = (i / j.nodes.length) * Math.PI * 2;
    const sim = { ...n, x: Math.cos(angle) * (60 + 120 * (n.dist || 0)) + (Math.random() - .5) * 30,
                  y: Math.sin(angle) * (60 + 120 * (n.dist || 0)) + (Math.random() - .5) * 30 };
    g.byId.set(n.id, sim);
    return sim;
  });
  g.links = j.links;
  $("graph-summary").textContent =
    `${j.count} nodes, ${j.links.length} edges around "${j.start}" (${j.hops} hop${j.hops > 1 ? "s" : ""})` +
    (j.truncated ? " — outer hops capped at 200 nodes; try fewer hops" : "");
  renderLegend();
  fitView();
  stopSim();
  g.running = true;
  // Let it settle, then freeze so the user can pan/zoom calmly.
  setTimeout(() => { g.running = false; }, 8000);
  g.sim = requestAnimationFrame(tick);
}

function renderLegend() {
  const present = new Set(g.nodes.map((n) => n.label));
  $("graph-legend").innerHTML = [...present].map(
    (l) => `<span class="key"><span class="dot" style="background:${nodeColor(l)}"></span>${l}</span>`
  ).join("");
}

// fullscreen toggle: fill the screen with the graph, refit on enter/leave.
(function graphFullscreen() {
  const wrap = $("graph-wrap");
  const btn = $("graph-fs");
  btn.onclick = () => {
    const fsEl = document.fullscreenElement || document.webkitFullscreenElement;
    if (fsEl) {
      (document.exitFullscreen || document.webkitExitFullscreen).call(document);
    } else {
      (wrap.requestFullscreen || wrap.webkitRequestFullscreen).call(wrap);
    }
  };
  const onFsChange = () => {
    btn.textContent = (document.fullscreenElement || document.webkitFullscreenElement)
      ? "✕" : "⛶";
    // Canvas backing store must catch up with the new CSS size; wait a
    // frame for layout, then refit the view so nothing sits off-screen.
    requestAnimationFrame(() => {
      fitView();
      drawGraph();
    });
  };
  document.addEventListener("fullscreenchange", onFsChange);
  document.addEventListener("webkitfullscreenchange", onFsChange);
  // Esc leaves fullscreen; also allow the canvas dblclick refit inside it.
})();

// canvas interactions: pan, zoom, hover, click-to-recenter
(function graphInteract() {
  const c = $("graph-canvas");
  let drag = null;
  const toWorld = (e) => {
    const r = c.getBoundingClientRect();
    return { x: (e.clientX - r.left - g.view.x) / g.view.scale,
             y: (e.clientY - r.top - g.view.y) / g.view.scale };
  };
  const nodeAt = (e) => {
    const p = toWorld(e);
    for (const n of g.nodes) {
      const dx = p.x - n.x, dy = p.y - n.y;
      const r = nodeRadius(n) + 3 / g.view.scale;
      if (dx * dx + dy * dy < r * r) return n;
    }
    return null;
  };
  c.addEventListener("mousedown", (e) => {
    const n = nodeAt(e);
    drag = { x: e.clientX, y: e.clientY, node: n, moved: false };
    c.classList.add("dragging");
  });
  c.addEventListener("mousemove", (e) => {
    if (drag) {
      if (Math.abs(e.clientX - drag.x) + Math.abs(e.clientY - drag.y) > 3) drag.moved = true;
      if (!drag.node) {
        g.view.x += e.clientX - drag.x;
        g.view.y += e.clientY - drag.y;
        drag.x = e.clientX; drag.y = e.clientY;
        drawGraph();
      } else {
        const p = toWorld(e);
        drag.node.x = p.x; drag.node.y = p.y;
        drag.node.vx = 0; drag.node.vy = 0;
        drawGraph();
      }
      return;
    }
    const n = nodeAt(e);
    if (n !== g.hovered) { g.hovered = n; c.style.cursor = n ? "pointer" : "grab"; drawGraph(); }
  });
  c.addEventListener("mouseleave", () => { if (!drag) { g.hovered = null; drawGraph(); } });
  window.addEventListener("mouseup", (e) => {
    if (!drag) return;
    c.classList.remove("dragging");
    const wasClick = !drag.moved && drag.node;
    drag = null;
    if (wasClick) {
      // click a node -> recenter the graph there.
      const n = g.hovered;
      if (n) doGraph(
        n.label === "book" ? (n.book_code || n.name)
        : n.label === "version" ? (n.version_code || n.name)
        : (n.name || n.book_code),
        n.label
      );
    }
  });
  c.addEventListener("wheel", (e) => {
    e.preventDefault();
    const r = c.getBoundingClientRect();
    const mx = e.clientX - r.left, my = e.clientY - r.top;
    const factor = e.deltaY < 0 ? 1.15 : 1 / 1.15;
    const s2 = g.view.scale * factor;
    g.view.x = mx - (mx - g.view.x) * factor;
    g.view.y = my - (my - g.view.y) * factor;
    g.view.scale = s2;
    drawGraph();
  }, { passive: false });
  c.addEventListener("dblclick", () => { fitView(); drawGraph(); });
  window.addEventListener("resize", () => { fitView(); drawGraph(); });
})();

// ---- map -----------------------------------------------------------------
// Leaflet with a CARTO basemap and the DARE "imperium" overlay (the same
// stack as the klokantech Roman Empire demo). Theme-aware: dark basemap
// when the app is in dark mode.

const m = { map: null, base: null, overlay: null, layer: null };

const BASE_TILES = {
  light: "https://{s}.basemaps.cartocdn.com/light_all/{z}/{x}/{y}{r}.png",
  dark: "https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png",
  satellite: "https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}",
};
const CARTO_ATTR = "© OpenStreetMap contributors © CARTO";
const ESRI_ATTR = "Esri, Maxar, Earthstar Geographics — Esri World Imagery";
const DARE_URL = "https://dare.ht.lu.se/tiles/imperium/{z}/{x}/{y}.png";

// A fullscreen toggle that looks native: it lives inside a leaflet-bar,
// so it renders exactly like the built-in zoom buttons.
const FullscreenControl = L.Control.extend({
  options: { position: "topleft" },
  onAdd() {
    const container = L.DomUtil.create("div", "leaflet-bar leaflet-control");
    const a = L.DomUtil.create("a", "leaflet-control-fullscreen", container);
    a.href = "#";
    a.title = "Toggle fullscreen";
    a.style.textDecoration = "none";
    this._a = a;
    L.DomEvent.on(container, "click", (e) => {
      L.DomEvent.stopPropagation(e);
      L.DomEvent.preventDefault(e);
      const fsEl = document.fullscreenElement || document.webkitFullscreenElement;
      if (fsEl) {
        (document.exitFullscreen || document.webkitExitFullscreen).call(document);
      } else {
        const el = $("map");
        (el.requestFullscreen || el.webkitRequestFullscreen).call(el);
      }
    });
    this._sync = () => {
      a.textContent = (document.fullscreenElement || document.webkitFullscreenElement)
        ? "\u2715" : "\u26F6";
    };
    this._sync();
    return container;
  },
  onRemove() {
    L.DomEvent.off(this._a, "click");
  },
});

function initMap() {
  m.map = L.map("map", { maxZoom: 12 });
  m.fsControl = new FullscreenControl().addTo(m.map);
  // Keyless satellite imagery (Esri World Imagery); CARTO kept as a
  // fallback in BASE_TILES in case we ever want a street-map variant.
  m.base = L.tileLayer(BASE_TILES.satellite, { attribution: ESRI_ATTR, maxZoom: 19 }).addTo(m.map);
  m.overlay = L.tileLayer(DARE_URL, {
    attribution: "DARE: Digital Atlas of the Roman Empire",
    maxZoom: 11, opacity: 0.85,
  }).addTo(m.map);
  m.layer = L.layerGroup().addTo(m.map);
  m.map.setView([33, 40], 5); // Levant, where most biblical places are
}

function refreshMapTheme() {
  // Satellite imagery works for both themes, so nothing to swap — kept
  // as a hook in case a theme-dependent basemap returns.
}

function popupHtml(p) {
  const name = p.secondary_name ? `${p.name} (${p.secondary_name})` : p.name;
  return `<div class="map-popup">` +
    `<div class="p-name">${name}</div>` +
    `<div class="p-ref">${p.region} · ${p.book_code} ${p.chapter}:${p.verse_number}</div>` +
    (p.text ? `<div class="p-text">${p.text}</div>` : "") +
    `</div>`;
}

async function doMap() {
  if (typeof L === "undefined") {
    alert("Leaflet failed to load (offline or CDN blocked). " +
          "The map needs internet access.");
    return;
  }
  const scope = $("map-scope").value;
  const q = $("map-q").value.trim();
  if (!q) return;
  if (!m.map) initMap();
  m.map.scrollWheelZoom.disable(); // don't hijack page scroll on hover
  $("map-summary").textContent = "querying…";
  const param = scope === "region" ? `region=${encodeURIComponent(q)}` : `book=${encodeURIComponent(q)}`;
  const r = await fetch(`/map/points?${param}`);
  const j = await r.json();
  if (j.error) { $("map-summary").textContent = ""; return alert(j.error); }
  m.layer.clearLayers();
  const color = cssVar("--node-location");
  const bounds = [];
  for (const p of j.points) {
    L.circleMarker([p.lat, p.lng], {
      radius: 5, weight: 1, color: "#fff", fillColor: color, fillOpacity: .9,
    })
      .bindPopup(() => popupHtml(p), { maxWidth: 300 })
      .addTo(m.layer);
    bounds.push([p.lat, p.lng]);
  }
  $("map-summary").textContent =
    `${j.count} location mentions in ${scope === "region" ? j.region : j.book}` +
    (j.truncated ? ` — capped at 1000, try a narrower scope` : "");
  if (bounds.length) m.map.fitBounds(bounds, { padding: [30, 30], maxZoom: 9 });
}

// fullscreen change: let Leaflet re-measure its container, and update
// the control's icon (⛶ <-> ✕) wherever it came from.
document.addEventListener("fullscreenchange", onMapFsChange);
document.addEventListener("webkitfullscreenchange", onMapFsChange);
function onMapFsChange() {
  if (m.fsControl && m.fsControl._sync) m.fsControl._sync();
  if (!m.map) return;
  // wait a frame for the new CSS size, then let Leaflet re-measure.
  setTimeout(() => m.map.invalidateSize(), 100);
}

// keep basemap in sync with the theme toggle
(function mapThemeHook() {
  const btn = $("theme-toggle");
  const orig = btn.onclick;
  btn.onclick = () => { if (orig) orig(); refreshMapTheme(); };
  // enable wheel-zoom after an explicit click on the map, like Google Maps
  document.addEventListener("click", (e) => {
    if (m.map && document.getElementById("map").contains(e.target))
      m.map.scrollWheelZoom.enable();
  });
})();

// ---- help toggle ---------------------------------------------------------
$("help-toggle").onclick = () => {
  const body = $("help-body");
  const btn = $("help-toggle");
  const open = !body.hidden;
  body.hidden = open;
  btn.setAttribute("aria-expanded", String(!open));
  btn.textContent = open ? "How to query the database ▸" : "How to query the database ▾";
};

// ---- wire up ------------------------------------------------------------
$("search-btn").onclick = doSearch;
$("search-q").addEventListener("keydown", (e) => { if (e.key === "Enter") doSearch(); });
$("trav-btn").onclick = doTraverse;
$("path-btn").onclick = doPath;
$("graph-btn").onclick = () => doGraph();
$("map-btn").onclick = () => doMap();
$("map-q").addEventListener("keydown", (e) => { if (e.key === "Enter") doMap(); });
$("graph-node").addEventListener("keydown", (e) => { if (e.key === "Enter") doGraph(); });
$("vs-btn").onclick = doVerse;
health();
