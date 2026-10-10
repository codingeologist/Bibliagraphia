import { useEffect, useState } from "react";
import { get, post } from "./api.js";
import SiteHeader from "./SiteHeader.jsx";
import { NavigationIcon } from "./navigation.jsx";
import { NodeIcon } from "./nodeIcons.jsx";
import { describeNode, pathNode, relationshipHref } from "./nodeLinks.js";

const widgets = [
  {
    id: "search", title: "Search scripture", icon: "search", href: "/explore/search",
    action: "Open search", description: "Find books, passages, translations, places, regions and biblical figures.",
    load: () => get("/search?q=&label=book&limit=4"),
  },
  {
    id: "read", title: "Read and compare", icon: "book",
    href: "/read?book=GEN&chapter=1&version=KJV&compare=DRB",
    action: "Open reader", description: "Start at Genesis 1:1 and compare translations side by side.",
    load: () => get("/verse?book_code=GEN&chapter=1&verse_number=1"),
  },
  {
    id: "relationships", title: "Relationships", icon: "relationships",
    href: "/relationships?graph_node=GEN&graph_label=book&graph_node_id=book%3AGEN&graph_hops=3",
    action: "Explore relationships", description: "Follow the connections around Genesis.",
    load: () => get("/graph?node=GEN&label=book&hops=1&balanced=true"),
  },
  {
    id: "map", title: "Places in Genesis", icon: "region", href: "/map",
    action: "Open map", description: "Discover the places mentioned in Genesis and their passages.",
    load: () => get("/map/points?book=GEN"),
  },
  {
    id: "connections", title: "Trace a connection", icon: "relationships", href: "/explore/connections",
    action: "Open connections", description: "Walk relationships or find a path from Genesis to Syria.",
    load: () => post("/path", { source: "GEN", source_label: "book", target: "Syria", target_label: "region" }),
  },
];

function GraphPreview({ graph }) {
  const centre = graph.nodes.find((node) => node.id === graph.start_id);
  if (!centre) return <p className="widget-empty">No graph nodes found.</p>;
  const neighbours = graph.nodes.filter((node) => node.id !== graph.start_id).slice(0, 6);
  const points = neighbours.map((node, index) => {
    const angle = index * 2 * Math.PI / neighbours.length - Math.PI / 2;
    return { node, x: 160 + Math.cos(angle) * 112, y: 95 + Math.sin(angle) * 63 };
  });
  return (
    <>
      <svg className="widget-graph" viewBox="0 0 320 190" role="img" aria-label={`Genesis connected to ${neighbours.length} sample passages`}>
        {points.map(({ node, x, y }) => <line key={node.id} x1="160" y1="95" x2={x} y2={y} className="stroke-edge" />)}
        {points.map(({ node, x, y }) => (
          <g key={node.id}>
            <circle cx={x} cy={y} r="12" className="fill-node-verse" />
            <text x={x} y={y + 26} textAnchor="middle">{node.chapter}:{node.verse_number} {node.version_code}</text>
          </g>
        ))}
        <circle cx="160" cy="95" r="24" className="fill-node-book" />
        <text x="160" y="99" textAnchor="middle" className="fill-panel">GEN</text>
      </svg>
      <p className="widget-meta">{graph.count} nodes in this preview · {graph.links.length} connections{graph.branch_limited ? " · more available" : ""}</p>
    </>
  );
}

function WidgetContent({ id, data }) {
  if (id === "search") return data.results.length ? (
    <ul className="widget-list">
      {data.results.map((node) => <li key={node.id}><a href={relationshipHref(node)}><NodeIcon type={node.label} />{describeNode(node)}<small>{node.book_code}</small></a></li>)}
    </ul>
  ) : <p className="widget-empty">No books found.</p>;
  if (id === "read") return data.verses.length ? (
    <div className="widget-verses">
      {data.verses.slice(0, 2).map((verse) => <blockquote key={verse.id}><cite>{verse.version} · Genesis 1:1</cite><p>{verse.text}</p></blockquote>)}
      <p className="widget-meta">{data.verses.length} translations available for this verse</p>
    </div>
  ) : <p className="widget-empty">Genesis 1:1 is not available.</p>;
  if (id === "relationships") return <GraphPreview graph={data} />;
  if (id === "map") {
    const places = [...new Map(data.points.map((point) => [point.name, point])).values()].slice(0, 5);
    return (
      <>
        <p className="widget-stat">{data.count}<span>location mentions{data.truncated ? " shown (limited)" : " in Genesis"}</span></p>
        <ul className="widget-list">
          {places.map((place) => <li key={place.name}><a href={`/map?${new URLSearchParams({ place: place.name })}`}><NodeIcon type="location" />{place.name}<small>{place.region}</small></a></li>)}
        </ul>
        {!places.length && <p className="widget-empty">No mapped places found.</p>}
      </>
    );
  }
  return data.found ? (
    <ol className="widget-path">
      {data.path.map((node) => {
        const entry = pathNode(node);
        return <li key={entry.id}><a href={relationshipHref(entry)}><NodeIcon type={entry.label} />{describeNode(entry)}</a></li>;
      })}
    </ol>
  ) : <p className="widget-empty">No path found between Genesis and Syria.</p>;
}

function PreviewWidget({ widget }) {
  const [result, setResult] = useState({ data: null, error: "", loading: true });
  const [revision, setRevision] = useState(0);
  useEffect(() => {
    let active = true;
    setResult({ data: null, error: "", loading: true });
    widget.load().then(
      (data) => { if (active) setResult({ data, error: "", loading: false }); },
      (error) => { if (active) setResult({ data: null, error: error.message, loading: false }); },
    );
    return () => { active = false; };
  }, [widget, revision]);
  return (
    <section className="panel explore-widget" aria-labelledby={`widget-${widget.id}`} aria-busy={result.loading}>
      <div className="widget-heading"><NavigationIcon type={widget.icon} /><h2 id={`widget-${widget.id}`}>{widget.title}</h2></div>
      <p className="panel-copy">{widget.description}</p>
      <div className="widget-content">
        {result.loading && <p className="widget-empty" role="status">Loading preview...</p>}
        {result.error && <div role="alert"><p className="notice error">{result.error}</p><button className="button button-secondary" onClick={() => setRevision((value) => value + 1)}>Retry preview</button></div>}
        {result.data && <WidgetContent id={widget.id} data={result.data} />}
      </div>
      <a className="widget-launch" href={widget.href}>{widget.action}<span aria-hidden="true">&rarr;</span></a>
    </section>
  );
}

export default function ExploreDashboard() {
  const [dark, setDark] = useState(() => {
    const saved = localStorage.getItem("theme");
    return saved ? saved === "dark" : window.matchMedia("(prefers-color-scheme: dark)").matches;
  });
  useEffect(() => {
    document.documentElement.classList.toggle("dark", dark);
    localStorage.setItem("theme", dark ? "dark" : "light");
  }, [dark]);
  return (
    <div className="app-shell min-h-screen">
      <SiteHeader currentPage="/explore" dark={dark} onToggleTheme={() => setDark((value) => !value)} />
      <main className="explore-dashboard">
        <div className="explore-widget-grid">{widgets.map((widget) => <PreviewWidget key={widget.id} widget={widget} />)}</div>
      </main>
    </div>
  );
}
