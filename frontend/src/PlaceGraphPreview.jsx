import { useEffect, useState } from "react";
import { get } from "./api.js";
import { nodeIconPaths } from "./nodeIcons.jsx";
import { describeNode, relationshipHref } from "./nodeLinks.js";

function PlaceGraphPreview({ location, suppliedGraph, heading = "Related passages and places" }) {
  const [graph, setGraph] = useState(null);
  const [error, setError] = useState("");

  useEffect(() => {
    if (suppliedGraph) return undefined;
    let cancelled = false;
    setGraph(null);
    setError("");
    const params = new URLSearchParams({
      node: location.name, label: location.label || "location", node_id: location.id, hops: "1",
    });
    get(`/graph?${params}`).then((result) => {
      if (!cancelled) setGraph(result);
    }).catch((requestError) => {
      if (!cancelled) setError(requestError.message);
    });
    return () => { cancelled = true; };
  }, [location.id, location.name, location.label, suppliedGraph]);

  const data = suppliedGraph || graph;
  const centre = data?.nodes.find((node) => node.id === data.start_id);
  const neighbourIds = new Set(data?.links.flatMap((link) =>
    link.source === data.start_id ? [link.target]
      : link.target === data.start_id ? [link.source] : []) || []);
  const neighbours = data?.nodes.filter((node) => neighbourIds.has(node.id)) || [];
  const shown = neighbours.slice(0, 6);
  const name = location.name.replace(/\s+\d+$/, "");
  const href = `/relationships?${new URLSearchParams({
    graph_node: location.name, graph_label: location.label || "location", graph_node_id: location.id,
  })}`;
  const nodes = centre ? [
    { node: centre, x: 160, y: 130 },
    ...shown.map((node, index) => {
      const angle = -Math.PI / 2 + index * 2 * Math.PI / shown.length;
      return { node, x: 160 + Math.cos(angle) * 105, y: 130 + Math.sin(angle) * 87 };
    }),
  ] : [];

  return (
    <section className="place-graph-section mb-5">
      <div className="place-graph-heading">
        <h4>{heading}</h4>
        <a href={href} aria-label={`Explore more connections for ${name}`} title="Open in Relationships">
          <span aria-hidden="true">⛶</span> See more
        </a>
      </div>
      {!data && !error && <p className="reader-loading" role="status">Loading connections…</p>}
      {error && <p className="notice error" role="alert">{error}</p>}
      {data && centre && (
        <>
          <svg
            className="place-graph-canvas"
            viewBox="0 0 320 270"
            role="group"
            aria-label={`${name} connected to ${shown.length ? shown.map(describeNode).join("; ") : "no other items"}`}
          >
            {nodes.slice(1).map(({ node, x, y }) => (
              <line key={node.id} x1="160" y1="130" x2={x} y2={y} stroke="var(--edge)" />
            ))}
            {nodes.map(({ node, x, y }, index) => {
              const text = node.label === "verse"
                ? `${node.book_code} ${node.chapter}:${node.verse_number}`
                : describeNode(node);
              return (
                <a key={node.id} href={relationshipHref(node)} aria-label={`Explore relationships for ${describeNode(node)}`}>
                <g transform={`translate(${x} ${y})`}>
                  <title>{describeNode(node)}</title>
                  <circle r={index === 0 ? 22 : 17} fill="var(--panel)" stroke={`var(--node-${node.label})`} strokeWidth={index === 0 ? 2.5 : 1.5} />
                  <path d={nodeIconPaths[node.label] || nodeIconPaths.verse} transform="translate(-10 -10) scale(.83)" fill="none" stroke={`var(--node-${node.label})`} strokeWidth="1.8" strokeLinecap="round" strokeLinejoin="round" />
                  <text y={index === 0 ? 36 : 30} textAnchor="middle">{text.length > 20 ? `${text.slice(0, 19)}…` : text}</text>
                  {node.label === "verse" && <text y="43" textAnchor="middle" className="place-graph-version">{node.version_code}</text>}
                </g>
                </a>
              );
            })}
          </svg>
          {!neighbours.length && <p className="empty-result">No related passages or places found.</p>}
          {(neighbours.length > shown.length || data.truncated) && (
            <p className="place-graph-caption mt-[7px] mb-0 text-[11px] text-muted">Showing {shown.length} connections. Select an item to explore it, or choose See more.</p>
          )}
        </>
      )}
    </section>
  );
}

export default PlaceGraphPreview;
