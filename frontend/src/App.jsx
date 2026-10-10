import React, { useEffect, useRef, useState } from "react";
import { flushSync } from "react-dom";
import { get, post } from "./api.js";
import ExploreDashboard from "./ExploreDashboard.jsx";
import LandingPage from "./LandingPage.jsx";
import MapPage from "./MapPage.jsx";
import ReaderPage from "./ReaderPage.jsx";
import SiteHeader from "./SiteHeader.jsx";
import RelationshipsPage from "./RelationshipsPage.jsx";
import GraphNodeSearch from "./GraphNodeSearch.jsx";
import McpPage from "./McpPage.jsx";
import AboutPage from "./AboutPage.jsx";
import { describeNode, nodeTypeName, pathNode, pathRelationshipDescription, relationshipHref } from "./nodeLinks.js";

const labels = ["book", "verse", "location", "region", "version", "figure"];
const genesis = { id: "book:GEN", label: "book", name: "Genesis", book_code: "GEN" };
const initialTraversal = { node: genesis, edge: "verse_in_book" };
const traversalTypes = ["book", "region", "version"];
const initialPath = {
  source: genesis,
  target: { id: "region:Syria", label: "region", name: "Syria" },
};
const displayName = describeNode;

function Field({ label, value, onChange, placeholder, type = "text", min }) {
  return (
    <input
      aria-label={label}
      type={type}
      min={min}
      required
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

function ResultRow({ node, friendly = false, onSelectFigure }) {
  return (
    <li>
      <a className="result-row no-underline" href={relationshipHref(node)}>
        <span className={`badge badge-${node.label}`}>{nodeTypeName(node.label)}</span>
        <span className="result-name overflow-hidden text-ellipsis">{displayName(node)}</span>
        {!friendly && (node.book_code || node.version_code) && (
          <span className="result-meta ml-auto whitespace-nowrap text-[10px] text-muted">
            {node.book_code}
            {node.chapter ? ` ${node.chapter}:${node.verse_number}` : ""}
            {node.version_code && !node.book_code ? ` ${node.version_code}` : ""}
          </span>
        )}
      </a>
      {node.label === "figure" && onSelectFigure && (
        <button className="button button-secondary" onClick={() => onSelectFigure(node)}>
          About {displayName(node)}
        </button>
      )}
    </li>
  );
}

function ExploreTools({ mode }) {
  const [dark, setDark] = useState(() => {
    const saved = localStorage.getItem("theme");
    return saved ? saved === "dark" : window.matchMedia("(prefers-color-scheme: dark)").matches;
  });
  const [health, setHealth] = useState({ status: "connecting", db: "" });
  const [search, setSearch] = useState({ query: "GEN", label: "" });
  const [searchResults, setSearchResults] = useState([]);
  const [searchMessage, setSearchMessage] = useState("");
  const [searchError, setSearchError] = useState("");
  const [searchLoading, setSearchLoading] = useState(false);
  const [figure, setFigure] = useState(null);
  const [traversal, setTraversal] = useState(initialTraversal);
  const [traverseResult, setTraverseResult] = useState(null);
  const [traverseError, setTraverseError] = useState("");
  const [pathForm, setPathForm] = useState(initialPath);
  const [pathResult, setPathResult] = useState(null);
  const [pathError, setPathError] = useState("");
  const [busy, setBusy] = useState({});
  const requestsRef = useRef({ search: 0, traverse: 0, path: 0 });

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
    event?.preventDefault();
    const query = search.query.trim();
    if (!query) {
      setSearchError("Enter a name or abbreviation to search.");
      return;
    }
    const requestId = ++requestsRef.current.search;
    setSearchLoading(true);
    setSearchError("");
    setSearchMessage("");
    try {
      const params = new URLSearchParams({ q: query });
      if (search.label) params.set("label", search.label);
      const result = await get(`/search?${params}`);
      if (requestId !== requestsRef.current.search) return;
      setSearchResults(result.results);
      setFigure(null);
      if (!result.results.length) setSearchMessage("No matches found. Try another name or a translation abbreviation, such as KJV.");
    } catch (error) {
      if (requestId !== requestsRef.current.search) return;
      setSearchError(error.message);
      setSearchResults([]);
    } finally {
      if (requestId === requestsRef.current.search) setSearchLoading(false);
    }
  };

  const runTraverse = async (event) => {
    event?.preventDefault();
    if (!traversal.node) {
      setTraverseError("Choose a book, region or translation.");
      return;
    }
    const requestId = ++requestsRef.current.traverse;
    setLoading("traverse", true);
    setTraverseResult(null);
    setTraverseError("");
    try {
      const result = await post("/traverse", {
        start_node: traversal.node.id,
        label: traversal.node.label,
        edge: traversal.edge,
      });
      if (requestId === requestsRef.current.traverse) setTraverseResult(result);
    } catch (error) {
      if (requestId === requestsRef.current.traverse) setTraverseError(error.message);
    } finally {
      if (requestId === requestsRef.current.traverse) setLoading("traverse", false);
    }
  };

  const runPath = async (event) => {
    event?.preventDefault();
    if (!pathForm.source || !pathForm.target) {
      setPathError("Choose two books, passages, places, regions or translations.");
      return;
    }
    const requestId = ++requestsRef.current.path;
    setLoading("path", true);
    setPathResult(null);
    setPathError("");
    try {
      const result = await post("/path", {
        source: pathForm.source.id,
        source_label: pathForm.source.label,
        target: pathForm.target.id,
        target_label: pathForm.target.label,
      });
      if (requestId === requestsRef.current.path) setPathResult({
        ...result,
        path: result.path.map((node) => typeof node === "string"
          ? [pathForm.source, pathForm.target].find((selected) => selected.id === node) || pathNode(node)
          : node),
      });
    } catch (error) {
      if (requestId === requestsRef.current.path) setPathError(error.message);
    } finally {
      if (requestId === requestsRef.current.path) setLoading("path", false);
    }
  };

  useEffect(() => {
    if (mode === "search") runSearch();
    else {
      runTraverse();
      runPath();
    }
    return () => {
      requestsRef.current.search++;
      requestsRef.current.traverse++;
      requestsRef.current.path++;
    };
  }, [mode]);

  const chooseTraversal = (node) => {
    requestsRef.current.traverse++;
    setLoading("traverse", false);
    setTraverseResult(null);
    setTraverseError("");
    const edge = node.label === "book" ? "verse_in_book"
      : node.label === "region" ? "location_in_region" : "verse_in_version";
    setTraversal({ node, edge });
  };

  const choosePath = (key, node) => {
    requestsRef.current.path++;
    setLoading("path", false);
    setPathResult(null);
    setPathError("");
    setPathForm((current) => ({ ...current, [key]: node }));
  };

  return (
    <div className="app-shell min-h-screen">
      <SiteHeader currentPage="/explore" dark={dark} onToggleTheme={() => setDark((value) => !value)} />

      <main className="explore-tools">
        <Notice error={health.status === "offline"}>
          {health.status === "offline" ? "Cannot connect to Bibliagraphia. Try again shortly."
            : health.status === "empty" ? "No Bible data is available." : ""}
        </Notice>
        <header className="explore-dashboard-heading">
          <a className="text-accent text-[12px]" href="/explore">&larr; Back to Explore</a>
          {mode === "search" && (
            <>
              <h2>Search scripture</h2>
              <p>Find a passage, person or place, then see what it connects to.</p>
            </>
          )}
        </header>
        <div className={`explore-tools-grid ${mode === "search" ? "search-tools" : ""}`}>
          <div className="primary-column">
            {mode === "search" && (
            <Panel title="Find in the Bible" className="search-panel">
              <p className="panel-copy">Search books, passages, people, places, regions and translations. You can also use abbreviations such as GEN or KJV.</p>
              <form className="form-row search-form" onSubmit={runSearch}>
                <Field
                  label="Search the Bible"
                  value={search.query}
                  onChange={(query) => setSearch((current) => ({ ...current, query }))}
                  placeholder="Try “Ruth”, “KJV” or “Jerusalem”…"
                />
                <Select
                  label="Filter search by type"
                  value={search.label}
                  onChange={(label) => setSearch((current) => ({ ...current, label }))}
                  options={[["", "Everything"], ...labels.map((label) => [label, nodeTypeName(label)])]}
                />
                <button className="button button-primary" disabled={searchLoading}>
                  {searchLoading ? "Searching…" : "Search"} <span aria-hidden="true">↗</span>
                </button>
              </form>
              <Notice error={Boolean(searchError)}>{searchError || searchMessage}</Notice>
              {searchLoading && <Notice>Finding matches...</Notice>}
              {searchResults.length > 0 && (
                <ul className="result-list" aria-label="Search results">
                  {searchResults.map((node) => (
                    <ResultRow key={node.id} node={node} onSelectFigure={setFigure} />
                  ))}
                </ul>
              )}
              {figure && (
                <div className="figure-detail" aria-live="polite">
                  <p className="eyebrow">{[figure.testament, figure.category].filter(Boolean).join(" · ")}</p>
                  <h3>{displayName(figure)}</h3>
                  <p>{figure.description}</p>
                </div>
              )}
            </Panel>
            )}

            {mode === "connections" && <>
            <Panel title="What’s connected?" eyebrow="Explore connections">
              <p className="panel-copy">Choose a book, region or translation to see its passages or places.</p>
              <form className="connection-form" onSubmit={runTraverse}>
                <GraphNodeSearch
                  label="Start with a book, region or translation"
                  searchLabel="Find a book, region or translation"
                  placeholder="Search by name…"
                  visibleLabel
                  allowedTypes={traversalTypes}
                  node={traversal.node}
                  onSelect={chooseTraversal}
                />
                <button className="button button-secondary" disabled={busy.traverse}>
                  {busy.traverse ? "Finding connections…" : "Show connections"}
                </button>
              </form>
              <Notice error={Boolean(traverseError)}>{traverseError}</Notice>
              {busy.traverse && <Notice>Finding connected passages and places...</Notice>}
              {traverseResult && (
                <>
                  <p className="result-summary">
                    {traverseResult.count} {traverseResult.edge === "location_in_region" ? "places" : "passages"} in {displayName(traverseResult.start_node)}
                  </p>
                  <ul className="result-list compact" aria-label="Connected passages and places">
                    {traverseResult.nodes.slice(0, 200).map((node) => (
                      <ResultRow key={`${node.id}-${node.level}`} node={node} friendly />
                    ))}
                    {traverseResult.nodes.length > 200 && (
                      <li className="more-results">Showing the first 200. {traverseResult.nodes.length - 200} more available.</li>
                    )}
                    {!traverseResult.nodes.length && <li className="empty-result">No connections found.</li>}
                  </ul>
                </>
              )}
            </Panel>

            <Panel title="How are these connected?" eyebrow="Follow a connection">
              <p className="panel-copy">Choose any two passages, people, places, books, regions or translations to see how they connect.</p>
              <form className="connection-form" onSubmit={runPath}>
                <GraphNodeSearch
                  label="From"
                  searchLabel="Find a starting point"
                  visibleLabel
                  node={pathForm.source}
                  onSelect={(node) => choosePath("source", node)}
                />
                <GraphNodeSearch
                  label="To"
                  searchLabel="Find a destination"
                  visibleLabel
                  node={pathForm.target}
                  onSelect={(node) => choosePath("target", node)}
                />
                <button className="button button-secondary" disabled={busy.path}>
                  {busy.path ? "Finding a connection…" : "Show connection"}
                </button>
              </form>
              <Notice error={Boolean(pathError)}>{pathError}</Notice>
              {busy.path && <Notice>Finding a connection...</Notice>}
              {pathResult && !pathResult.found && (
                <p className="empty-result">No recorded connection found between these choices.</p>
              )}
              {pathResult?.found && (
                <>
                <p className="result-summary">
                  {pathResult.depth === 0 ? "Both choices refer to the same item." : "Follow the recorded links below. Select any item to explore further."}
                </p>
                <ol className="path-list">
                  {pathResult.path.map((node, index) => {
                    const entry = pathNode(node);
                    return (
                    <li key={entry.id}>
                      <a className="path-node text-ink no-underline" href={relationshipHref(entry)}>
                        <span className={`badge badge-${entry.label}`}>{nodeTypeName(entry.label)}</span>
                        {displayName(entry)}
                      </a>
                      {entry.label === "verse" && entry.attrs?.text && (
                        <p className="mt-2 font-display text-[14px] leading-relaxed">{entry.attrs.text}</p>
                      )}
                      {pathResult.edges[index] && (
                        <span className="path-edge">↓ {pathRelationshipDescription(
                          pathResult.edges[index].label,
                          entry.label,
                          pathResult.edges[index],
                          entry.id,
                        )}</span>
                      )}
                    </li>
                    );
                  })}
                </ol>
                </>
              )}
            </Panel>
            </>}
          </div>
        </div>
      </main>

    </div>
  );
}

function App() {
  const [route, setRoute] = useState(() => ({ path: window.location.pathname, key: 0 }));
  const expandingRef = useRef(false);
  const mapReadyRef = useRef(null);

  useEffect(() => {
    const restoreRoute = () => {
      mapReadyRef.current?.();
      setRoute((current) => ({ path: window.location.pathname, key: current.key + 1 }));
    };
    window.addEventListener("popstate", restoreRoute);
    return () => window.removeEventListener("popstate", restoreRoute);
  }, []);

  const expandMap = (event, initialMap) => {
    if (event.defaultPrevented || event.button !== 0
      || event.metaKey || event.ctrlKey || event.shiftKey || event.altKey
      || !document.startViewTransition
      || window.matchMedia("(prefers-reduced-motion: reduce)").matches) return;

    event.preventDefault();
    if (expandingRef.current) return;
    expandingRef.current = true;
    const href = event.currentTarget.href;
    const focusMap = () => {
      expandingRef.current = false;
      mapReadyRef.current = null;
      document.querySelector(".map-page-panel")?.focus({ preventScroll: true });
    };
    const transition = document.startViewTransition(async () => {
      const mapReady = new Promise((resolve) => {
        mapReadyRef.current = resolve;
        window.history.replaceState(null, "", initialMap.readerHref);
        window.history.pushState(null, "", href);
        flushSync(() => setRoute((current) => ({
          path: "/map",
          key: current.key + 1,
          initialMap,
          onMapReady: resolve,
        })));
        window.scrollTo(0, 0);
      });
      await mapReady;
    });
    transition.finished.then(focusMap, (error) => {
      console.error("Map expansion transition failed:", error);
      focusMap();
    });
  };

  if (route.path === "/relationships") return <RelationshipsPage key={route.key} />;
  if (route.path === "/read") return <ReaderPage key={route.key} onExpandMap={expandMap} />;
  if (route.path === "/map") {
    return <MapPage key={route.key} initialMap={route.initialMap} onMapReady={route.onMapReady} />;
  }
  if (route.path === "/explore/search") return <ExploreTools key={`${route.path}-${route.key}`} mode="search" />;
  if (route.path === "/explore/connections") return <ExploreTools key={`${route.path}-${route.key}`} mode="connections" />;
  if (route.path === "/explore") return <ExploreDashboard key={route.key} />;
  if (route.path === "/connect-mcp") return <McpPage key={route.key} />;
  if (route.path === "/about") return <AboutPage key={route.key} />;
  if (route.path === "/") return <LandingPage />;
  return <ExploreDashboard key={route.key} />;
}

export default App;
