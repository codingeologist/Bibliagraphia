import { useEffect, useId, useRef, useState } from "react";
import { get } from "./api.js";
import { NodeIcon } from "./nodeIcons.jsx";

const groups = [
  ["book", "Books"], ["verse", "Passages"], ["location", "Places"],
  ["version", "Translations"], ["region", "Regions"], ["figure", "Figures"],
];

const describe = (node) => node.label === "verse"
  ? `${node.name || node.book_code} ${node.chapter}:${node.verse_number} · ${node.version_code}`
  : node.label === "location" && node.chapter
    ? `${node.name} · ${node.book_code} ${node.chapter}:${node.verse_number}`
    : node.name || node.id;

function GraphNodeSearch({ node, fallback, onSelect }) {
  const id = useId();
  const inputRef = useRef(null);
  const triggerRef = useRef(null);
  const [open, setOpen] = useState(false);
  const [query, setQuery] = useState("");
  const [results, setResults] = useState([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");
  const [active, setActive] = useState(-1);

  useEffect(() => {
    if (!open) return undefined;
    inputRef.current?.focus();
    let cancelled = false;
    setLoading(true);
    setError("");
    setResults([]);
    setActive(-1);
    const timer = window.setTimeout(async () => {
      try {
        const matches = await Promise.all(groups.map(async ([label]) => {
          const result = await get(`/search?${new URLSearchParams({ q: query.trim(), label, limit: "3" })}`);
          return result.results;
        }));
        if (!cancelled) setResults(matches.flat());
      } catch (requestError) {
        if (!cancelled) setError(requestError.message);
      } finally {
        if (!cancelled) setLoading(false);
      }
    }, 250);
    return () => { cancelled = true; window.clearTimeout(timer); };
  }, [open, query]);

  const close = () => {
    setOpen(false);
    triggerRef.current?.focus();
  };
  const choose = (selected) => {
    onSelect(selected);
    close();
  };

  return (
    <div className="graph-node-search" onBlur={(event) => {
      if (!event.currentTarget.contains(event.relatedTarget)) setOpen(false);
    }}>
      <button ref={triggerRef} type="button" className="graph-node-search-trigger"
        aria-label="Choose graph centre" aria-expanded={open} aria-controls={`${id}-panel`}
        onClick={() => { setQuery(""); setOpen((value) => !value); }}>
        {node && <NodeIcon type={node.label} />}
        <span>{node ? describe(node) : fallback || "Choose a node"}</span>
        <span aria-hidden="true">⌄</span>
      </button>
      {open && (
        <div id={`${id}-panel`} className="graph-node-search-panel">
          <input ref={inputRef} role="combobox" aria-label="Search graph nodes"
            aria-autocomplete="list" aria-expanded="true" aria-controls={`${id}-results`}
            aria-activedescendant={active >= 0 ? `${id}-option-${active}` : undefined}
            placeholder="Search books, passages, places…" autoComplete="off" value={query}
            onChange={(event) => setQuery(event.target.value)}
            onKeyDown={(event) => {
              if (event.key === "Escape") { event.preventDefault(); event.stopPropagation(); close(); }
              else if (event.key === "ArrowDown" || event.key === "ArrowUp") {
                event.preventDefault();
                if (results.length) setActive((value) =>
                  (value + (event.key === "ArrowDown" ? 1 : value < 0 ? 0 : -1) + results.length) % results.length);
              } else if (event.key === "Enter") {
                event.preventDefault();
                if (results.length) choose(results[active < 0 ? 0 : active]);
              }
            }} />
          {loading && <p role="status">Searching nodes…</p>}
          {error && <p className="notice error" role="alert">{error}</p>}
          {!loading && !error && !results.length && <p role="status">No matching nodes.</p>}
          <div id={`${id}-results`} role="listbox" aria-label="Matching graph nodes" aria-busy={loading}>
            {groups.map(([type, title]) => {
              const matches = results.filter((item) => item.label === type);
              return matches.length > 0 && (
                <div key={type} role="group" aria-label={title}>
                  <div className="graph-node-search-group" aria-hidden="true">{title}</div>
                  {matches.map((item) => {
                    const index = results.indexOf(item);
                    return (
                      <div key={item.id} id={`${id}-option-${index}`} role="option"
                        aria-selected={active === index}
                        onPointerDown={(event) => event.preventDefault()} onClick={() => choose(item)}>
                        <NodeIcon type={type} /><span>{describe(item)}</span>
                      </div>
                    );
                  })}
                </div>
              );
            })}
          </div>
        </div>
      )}
    </div>
  );
}

export default GraphNodeSearch;
