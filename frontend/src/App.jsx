import { useCallback, useEffect, useState } from "react";
import { get, post } from "./api.js";
import GraphExplorer from "./GraphExplorer.jsx";
import MapExplorer from "./MapExplorer.jsx";
import ReaderPage from "./ReaderPage.jsx";

const labels = ["book", "verse", "location", "region", "version"];
const initialTraversal = { node: "", label: "book", edge: "verse_in_book" };
const initialPath = {
  source: "",
  sourceLabel: "book",
  target: "",
  targetLabel: "region",
};
const initialVerse = { book: "", chapter: "", verse: "" };
const displayName = (node) => node.name || node.id;
const nodeCode = (node) =>
  node.label === "book" ? node.book_code
    : node.label === "version" ? node.version_code
      : null;

function Field({ label, value, onChange, placeholder, type = "text", min }) {
  return (
    <input
      aria-label={label}
      type={type}
      min={min}
      value={value}
      placeholder={placeholder}
      onChange={(event) => onChange(event.target.value)}
    />
  );
}

function Select({ label, value, onChange, options }) {
  return (
    <select aria-label={label} value={value} onChange={(event) => onChange(event.target.value)}>
      {options.map((option) => {
        const [optionValue, text] = Array.isArray(option) ? option : [option, option];
        return <option key={optionValue} value={optionValue}>{text}</option>;
      })}
    </select>
  );
}

function Panel({ title, eyebrow, children, className = "", id }) {
  return (
    <section className={`panel ${className}`} id={id}>
      {eyebrow && <p className="eyebrow">{eyebrow}</p>}
      <h2>{title}</h2>
      {children}
    </section>
  );
}

function Notice({ children, error = false }) {
  if (!children) return null;
  return <p className={`notice${error ? " error" : ""}`} role={error ? "alert" : "status"}>{children}</p>;
}

function ResultRow({ node, onClick }) {
  return (
    <li>
      <button className="result-row" onClick={() => onClick?.(node)}>
        <span className={`badge badge-${node.label}`}>{node.label}</span>
        <span className="result-name">{displayName(node)}</span>
        {(node.book_code || node.version_code) && (
          <span className="result-meta">
            {node.book_code}
            {node.chapter ? ` ${node.chapter}:${node.verse_number}` : ""}
            {node.version_code && !node.book_code ? ` ${node.version_code}` : ""}
          </span>
        )}
      </button>
    </li>
  );
}

function ExplorePage() {
  const [dark, setDark] = useState(() => {
    const saved = localStorage.getItem("theme");
    return saved ? saved === "dark" : window.matchMedia("(prefers-color-scheme: dark)").matches;
  });
  const [health, setHealth] = useState({ status: "connecting", db: "" });
  const [helpOpen, setHelpOpen] = useState(false);
  const [search, setSearch] = useState({ query: "", label: "" });
  const [searchResults, setSearchResults] = useState([]);
  const [searchMessage, setSearchMessage] = useState("");
  const [searchError, setSearchError] = useState("");
  const [searchLoading, setSearchLoading] = useState(false);
  const [traversal, setTraversal] = useState(initialTraversal);
  const [traverseResult, setTraverseResult] = useState(null);
  const [traverseMessage, setTraverseMessage] = useState("");
  const [traverseError, setTraverseError] = useState("");
  const [pathForm, setPathForm] = useState(initialPath);
  const [pathResult, setPathResult] = useState(null);
  const [pathError, setPathError] = useState("");
  const [verseForm, setVerseForm] = useState(initialVerse);
  const [verseResult, setVerseResult] = useState(null);
  const [verseError, setVerseError] = useState("");
  const [graphSeed, setGraphSeed] = useState(() => {
    const params = new URLSearchParams(window.location.search);
    const node = params.get("graph_node");
    return node ? { node, label: params.get("graph_label") || "location" } : null;
  });
  const [mapSeed, setMapSeed] = useState(() => {
    const params = new URLSearchParams(window.location.search);
    const region = params.get("map_region");
    const book = params.get("map_book");
    return region ? { scope: "region", query: region, nonce: Date.now() }
      : book ? { scope: "book", query: book, nonce: Date.now() }
        : null;
  });
  const [busy, setBusy] = useState({});

  useEffect(() => {
    document.documentElement.classList.toggle("dark", dark);
    localStorage.setItem("theme", dark ? "dark" : "light");
  }, [dark]);

  useEffect(() => {
    let active = true;
    get("/health")
      .then((result) => active && setHealth({
        status: result.empty ? "empty" : "connected",
        db: result.db,
      }))
      .catch(() => active && setHealth({ status: "offline", db: "" }));
    return () => { active = false; };
  }, []);

  const setLoading = (key, value) => setBusy((current) => ({ ...current, [key]: value }));

  const runSearch = async (event) => {
    event.preventDefault();
    const query = search.query.trim();
    if (!query) return;
    setSearchLoading(true);
    setSearchError("");
    setSearchMessage("");
    try {
      const params = new URLSearchParams({ q: query });
      if (search.label) params.set("label", search.label);
      const result = await get(`/search?${params}`);
      setSearchResults(result.results);
      if (!result.results.length) setSearchMessage("No matching nodes. Try another name or code.");
    } catch (error) {
      setSearchError(error.message);
      setSearchResults([]);
    } finally {
      setSearchLoading(false);
    }
  };

  const chooseNode = (node) => {
    const code = nodeCode(node) || displayName(node);
    if (["book", "region", "version"].includes(node.label)) {
      const edge = node.label === "book" ? "verse_in_book"
        : node.label === "region" ? "location_in_region" : "verse_in_version";
      setTraversal({ node: code, label: node.label, edge });
      setPathForm((current) => ({ ...current, source: code, sourceLabel: node.label }));
      setGraphSeed({ node: code, label: node.label });
      if (node.label === "book" || node.label === "region") {
        setMapSeed({
          scope: node.label === "book" ? "book" : "region",
          query: code,
          nonce: Date.now(),
        });
      }
    } else if (node.label === "verse") {
      setVerseForm({
        book: node.book_code || "",
        chapter: String(node.chapter || ""),
        verse: String(node.verse_number || ""),
      });
      setGraphSeed({ node: displayName(node), label: "verse" });
    } else {
      setGraphSeed({ node: displayName(node), label: "location" });
    }
  };

  const runTraverse = async (event) => {
    event.preventDefault();
    if (!traversal.node.trim()) return;
    setLoading("traverse", true);
    setTraverseResult(null);
    setTraverseError("");
    try {
      setTraverseResult(await post("/traverse", {
        start_node: traversal.node.trim(),
        label: traversal.label,
        edge: traversal.edge,
      }));
    } catch (error) {
      setTraverseError(error.message);
    } finally {
      setLoading("traverse", false);
    }
  };

  const runPath = async (event) => {
    event.preventDefault();
    if (!pathForm.source.trim() || !pathForm.target.trim()) return;
    setLoading("path", true);
    setPathResult(null);
    setPathError("");
    try {
      setPathResult(await post("/path", {
        source: pathForm.source.trim(),
        source_label: pathForm.sourceLabel,
        target: pathForm.target.trim(),
        target_label: pathForm.targetLabel,
      }));
    } catch (error) {
      setPathError(error.message);
    } finally {
      setLoading("path", false);
    }
  };

  const runVerse = async (event) => {
    event.preventDefault();
    const { book, chapter, verse } = verseForm;
    if (!book.trim() || !chapter || !verse) return;
    setLoading("verse", true);
    setVerseResult(null);
    setVerseError("");
    try {
      const params = new URLSearchParams({
        book_code: book.trim().toUpperCase(),
        chapter,
        verse_number: verse,
      });
      setVerseResult(await get(`/verse?${params}`));
    } catch (error) {
      setVerseError(error.message);
    } finally {
      setLoading("verse", false);
    }
  };

  const setTraversalField = (key, value) => {
    setTraversal((current) => {
      if (key === "label") {
        const edge = value === "book" ? "verse_in_book"
          : value === "region" ? "location_in_region" : "verse_in_version";
        return { ...current, label: value, edge };
      }
      return { ...current, [key]: value };
    });
  };

  const setPathField = (key, value) => setPathForm((current) => ({ ...current, [key]: value }));
  const setVerseField = (key, value) => setVerseForm((current) => ({ ...current, [key]: value }));

  const chooseTraversedNode = useCallback((node) => {
    if (node.label === "verse") {
      setVerseForm({
        book: node.book_code || "",
        chapter: String(node.chapter || ""),
        verse: String(node.verse_number || ""),
      });
    } else if (node.label === "location") {
      setPathForm((current) => ({
        ...current,
        source: node.name || "",
        sourceLabel: "location",
        target: node.attrs?.region || "",
        targetLabel: "region",
      }));
    }
  }, []);

  return (
    <div className="app-shell">
      <header className="topbar">
        <div className="brand">
          <img className="brand-mark" src="/favicon.svg" alt="" aria-hidden="true" />
          <div>
            <h1>Bibliagraphia</h1>
            <p>A living map of scripture</p>
          </div>
        </div>
        <div className="header-actions">
          <nav className="site-nav" aria-label="Main navigation">
            <a href="/" aria-current="page">Explore</a>
            <a href="/read">Read Bible</a>
          </nav>
          <span className={`connection connection-${health.status}`}>
            <span className="connection-dot" />
            {health.status === "connected" ? "API connected"
              : health.status === "empty" ? "Database empty"
                : health.status === "offline" ? "API unreachable" : "Connecting"}
          </span>
          <button className="theme-toggle" type="button" onClick={() => setDark((value) => !value)}>
            {dark ? "☀️" : "🌙"} <span>{dark ? "Light" : "Dark"}</span>
          </button>
        </div>
      </header>

      <main>
        <section className="intro">
          <div>
            <p className="eyebrow">A Bible graph in DuckDB</p>
            <h2>Explore the connections<br />between <em>people, places &amp; passages.</em></h2>
            <p className="intro-copy">
              Search the graph, trace a path through scripture, compare translations,
              and see biblical places on the map.
            </p>
          </div>
          <div className="intro-art" aria-hidden="true">
            <span className="art-line line-one" /><span className="art-line line-two" />
            <span className="art-line line-three" />
            <span className="art-node node-one">BOOK</span>
            <span className="art-node node-two">VERSE</span>
            <span className="art-node node-three">PLACE</span>
            <span className="art-node node-four">REGION</span>
          </div>
        </section>

        <section className="help-wrap">
          <button
            className="help-toggle"
            type="button"
            aria-expanded={helpOpen}
            onClick={() => setHelpOpen((value) => !value)}
          >
            <span><span className="help-icon">i</span> How to explore the graph</span>
            <span>{helpOpen ? "−" : "+"}</span>
          </button>
          {helpOpen && (
            <div className="help-content">
              <p>The database is a property graph of books, verses, Bible versions, regions, and locations.</p>
              <div className="help-grid">
                <p><code>verse_in_book</code><br />Book → its verses</p>
                <p><code>verse_in_version</code><br />Version → its verses</p>
                <p><code>location_in_region</code><br />Region → its places</p>
              </div>
              <p>Search for a node, then select a result to prefill the relevant tools. The graph explorer displays up to three hops; the map needs a book or region scope.</p>
            </div>
          )}
        </section>

        <div className="workspace">
          <div className="primary-column">
            <Panel title="Find a connection" eyebrow="01 — Search" className="search-panel">
              <p className="panel-copy">Look up a book, verse, translation, place or region by name or code.</p>
              <form className="form-row search-form" onSubmit={runSearch}>
                <Field
                  label="Search the graph"
                  value={search.query}
                  onChange={(query) => setSearch((current) => ({ ...current, query }))}
                  placeholder="Try “JOH”, “KJV” or “Jerusalem”…"
                />
                <Select
                  label="Filter search by type"
                  value={search.label}
                  onChange={(label) => setSearch((current) => ({ ...current, label }))}
                  options={[["", "All types"], ...labels]}
                />
                <button className="button button-primary" disabled={searchLoading}>
                  {searchLoading ? "Searching…" : "Search"} <span aria-hidden="true">↗</span>
                </button>
              </form>
              <Notice error={Boolean(searchError)}>{searchError || searchMessage}</Notice>
              {searchResults.length > 0 && (
                <ul className="result-list" aria-label="Search results">
                  {searchResults.map((node) => (
                    <ResultRow key={node.id} node={node} onClick={chooseNode} />
                  ))}
                </ul>
              )}
            </Panel>

            <Panel title="Walk the graph" eyebrow="02 — Traverse">
              <p className="panel-copy">Follow a relationship from a starting node to its descendants.</p>
              <form className="form-row" onSubmit={runTraverse}>
                <Field
                  label="Starting node"
                  value={traversal.node}
                  onChange={(node) => setTraversalField("node", node)}
                  placeholder="Starting node — e.g. GEN"
                />
                <Select
                  label="Starting node type"
                  value={traversal.label}
                  onChange={(label) => setTraversalField("label", label)}
                  options={["book", "region", "version"]}
                />
                <Select
                  label="Relationship"
                  value={traversal.edge}
                  onChange={(edge) => setTraversalField("edge", edge)}
                  options={["verse_in_book", "location_in_region", "verse_in_version"]}
                />
                <button className="button button-secondary" disabled={busy.traverse}>
                  {busy.traverse ? "Walking…" : "Traverse"}
                </button>
              </form>
              <Notice error={Boolean(traverseError)}>{traverseError}</Notice>
              {traverseResult && (
                <>
                  <p className="result-summary">
                    {traverseResult.count} descendants of “{traverseResult.start}” via {traverseResult.edge}
                  </p>
                  <ul className="result-list compact" aria-label="Traversal results">
                    {traverseResult.nodes.slice(0, 200).map((node) => (
                      <ResultRow key={`${node.id}-${node.level}`} node={node} onClick={chooseTraversedNode} />
                    ))}
                    {traverseResult.nodes.length > 200 && (
                      <li className="more-results">… {traverseResult.nodes.length - 200} more omitted</li>
                    )}
                    {!traverseResult.nodes.length && <li className="empty-result">No descendants found.</li>}
                  </ul>
                </>
              )}
            </Panel>

            <Panel title="Find the shortest path" eyebrow="03 — Connect">
              <p className="panel-copy">Discover how two nodes are connected through the graph, up to ten hops.</p>
              <form className="form-row path-form" onSubmit={runPath}>
                <Field
                  label="Path starting node"
                  value={pathForm.source}
                  onChange={(source) => setPathField("source", source)}
                  placeholder="From — e.g. JOH"
                />
                <Select
                  label="Starting node type"
                  value={pathForm.sourceLabel}
                  onChange={(sourceLabel) => setPathField("sourceLabel", sourceLabel)}
                  options={["book", "verse", "version", "location"]}
                />
                <span className="path-arrow" aria-hidden="true">→</span>
                <Field
                  label="Path destination"
                  value={pathForm.target}
                  onChange={(target) => setPathField("target", target)}
                  placeholder="To — e.g. Syria"
                />
                <Select
                  label="Destination node type"
                  value={pathForm.targetLabel}
                  onChange={(targetLabel) => setPathField("targetLabel", targetLabel)}
                  options={["region", "location", "book", "version"]}
                />
                <button className="button button-secondary" disabled={busy.path}>
                  {busy.path ? "Connecting…" : "Find path"}
                </button>
              </form>
              <Notice error={Boolean(pathError)}>{pathError}</Notice>
              {pathResult && !pathResult.found && (
                <p className="empty-result">No path found within depth 10.</p>
              )}
              {pathResult?.found && (
                <ol className="path-list">
                  {pathResult.path.map((node, index) => {
                    const entry = typeof node === "string" ? { id: node, name: node, label: "node" } : node;
                    return (
                    <li key={entry.id}>
                      <span className="path-node">
                        <span className={`badge badge-${entry.label}`}>{entry.label}</span>
                        {displayName(entry)}
                      </span>
                      {pathResult.edges[index] && (
                        <span className="path-edge">↓ {pathResult.edges[index].label}</span>
                      )}
                    </li>
                    );
                  })}
                </ol>
              )}
            </Panel>
          </div>

          <div className="secondary-column">
            <Panel title="Graph explorer" eyebrow="04 — Visualise" className="graph-panel" id="graph-explorer">
              <p className="panel-copy">Explore a node’s neighbourhood. Drag to pan, scroll to zoom, select a node to recenter.</p>
              <GraphExplorer seed={graphSeed} />
            </Panel>

            <Panel title="Map of places" eyebrow="05 — Locate" className="map-panel" id="map-explorer">
              <p className="panel-copy">Plot biblical places by region or book. Select a point to read its verse.</p>
              <MapExplorer seed={mapSeed} />
            </Panel>

            <Panel title="Compare translations" eyebrow="06 — Compare">
              <p className="panel-copy">Read one verse side by side in every loaded Bible version.</p>
              <form className="form-row verse-form" onSubmit={runVerse}>
                <Field
                  label="Book code"
                  value={verseForm.book}
                  onChange={(book) => setVerseField("book", book)}
                  placeholder="JOH"
                />
                <Field
                  label="Chapter"
                  type="number"
                  min="1"
                  value={verseForm.chapter}
                  onChange={(chapter) => setVerseField("chapter", chapter)}
                  placeholder="Chapter"
                />
                <Field
                  label="Verse"
                  type="number"
                  min="1"
                  value={verseForm.verse}
                  onChange={(verse) => setVerseField("verse", verse)}
                  placeholder="Verse"
                />
                <button className="button button-secondary" disabled={busy.verse}>
                  {busy.verse ? "Comparing…" : "Compare"}
                </button>
              </form>
              <Notice error={Boolean(verseError)}>{verseError}</Notice>
              {verseResult && (
                <div className="verse-results">
                  {!verseResult.verses.length && <p className="empty-result">No verse found for this reference.</p>}
                  {verseResult.verses.map((item) => (
                    <article className="verse-card" key={item.id}>
                      <p>{item.version}</p>
                      <div>{item.text}</div>
                    </article>
                  ))}
                </div>
              )}
            </Panel>
          </div>
        </div>
      </main>

      <footer>
        <span className={`footer-dot connection-${health.status}`} />
        {health.status === "connected"
          ? "API connected · database ready"
          : health.status === "empty" ? "API connected · database is empty"
            : health.status === "offline" ? "Could not reach the API"
              : "Checking API connection…"}
      </footer>
    </div>
  );
}

function App() {
  return window.location.pathname === "/read" ? <ReaderPage /> : <ExplorePage />;
}

export default App;
