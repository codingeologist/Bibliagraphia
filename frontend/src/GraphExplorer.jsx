import { useCallback, useEffect, useRef, useState } from "react";
import {
  forceCenter,
  forceCollide,
  forceLink,
  forceManyBody,
  forceSimulation,
  forceX,
  forceY,
} from "d3-force";
import { get } from "./api.js";
import { NodeIcon, nodeIconPaths } from "./nodeIcons.jsx";
import GraphNodeSearch from "./GraphNodeSearch.jsx";
import { saveRelationshipState } from "./relationshipState.js";
import { relationshipDescription } from "./nodeLinks.js";

const colorVars = {
  book: "--node-book",
  version: "--node-version",
  region: "--node-region",
  location: "--node-location",
  verse: "--node-verse",
  figure: "--node-figure",
};

const nodeName = (node) => node.name || node.id;
const graphNodeQuery = (node) =>
  node.label === "book" ? node.book_code || node.name
    : node.label === "version" ? node.version_code || node.name
      : node.name || node.book_code;

const nodeTypeNames = {
  book: "Books",
  verse: "Passages",
  version: "Translations",
  location: "Places",
  region: "Regions",
  figure: "Figures",
};

const nodeDescription = (node) => node.label === "verse"
  ? `${node.name || node.book_code} ${node.chapter}:${node.verse_number} · ${node.version_code}`
  : node.label === "version"
    ? node.attrs?.full_name || node.version_code || nodeName(node)
    : nodeName(node);

function GraphExplorer({ seed, fullPage = false }) {
  const [query, setQuery] = useState("");
  const [label, setLabel] = useState("book");
  const [hops, setHops] = useState(fullPage ? seed?.hops || "3" : "1");
  const [showNames, setShowNames] = useState(seed?.showNames === true);
  const showNamesRef = useRef(showNames);
  showNamesRef.current = showNames;
  const [hiddenTypes, setHiddenTypes] = useState(() => new Set());
  const hiddenTypesRef = useRef(hiddenTypes);
  hiddenTypesRef.current = hiddenTypes;
  const [graph, setGraph] = useState(null);
  const [summary, setSummary] = useState("");
  const [error, setError] = useState("");
  const [loading, setLoading] = useState(false);
  const canvasRef = useRef(null);
  const wrapperRef = useRef(null);
  const nodesRef = useRef([]);
  const linksRef = useRef([]);
  const viewRef = useRef({ x: 0, y: 0, scale: 1 });
  const startIdRef = useRef("");
  const dragRef = useRef(null);
  const hoveredRef = useRef(null);
  const runRef = useRef(null);
  const requestRef = useRef(0);
  const nodeMenuRef = useRef(null);
  const lastRequestRef = useRef("");
  const exactCentreRef = useRef(null);
  const branchExpansionRef = useRef(1);
  const [controlRevision, setControlRevision] = useState(0);

  const runGraph = useCallback(async (node = query, nodeLabel = label, nodeId = "", expansion) => {
    const start = node.trim();
    if (!start) {
      setError("Enter a graph centre.");
      setLoading(false);
      setSummary("");
      return;
    }
    const requestId = ++requestRef.current;
    const previousCentre = exactCentreRef.current;
    if (nodeId !== previousCentre?.id || start !== previousCentre?.query || nodeLabel !== previousCentre?.label) {
      setHiddenTypes((current) => {
        if (!current.has(nodeLabel)) return current;
        const next = new Set(current);
        next.delete(nodeLabel);
        return next;
      });
    }
    branchExpansionRef.current = expansion ?? (
      nodeId && nodeId === previousCentre?.id ? branchExpansionRef.current : 1
    );
    lastRequestRef.current = JSON.stringify([start, nodeLabel, hops]);
    exactCentreRef.current = { query: start, label: nodeLabel, id: nodeId };
    setLoading(true);
    setError("");
    setSummary("Loading graph…");
    try {
      const params = new URLSearchParams({ node: start, label: nodeLabel, hops });
      // Balanced mode in both views: passages are capped per branch so
      // figures and their kinship edges stay visible in the node budget.
      params.set("balanced", "true");
      params.set("branch_expansion", String(branchExpansionRef.current));
      if (nodeId) params.set("node_id", nodeId);
      const result = await get(`/graph?${params}`);
      if (requestId !== requestRef.current) return;
      setQuery(start);
      setLabel(nodeLabel);
      startIdRef.current = result.start_id;
      exactCentreRef.current = { query: start, label: nodeLabel, id: result.start_id };
      setGraph(result);
      if (fullPage) {
        const urlParams = new URLSearchParams({
          graph_node: start, graph_label: nodeLabel, graph_node_id: result.start_id,
          graph_hops: hops,
        });
        window.history.replaceState(null, "", `/relationships?${urlParams}`);
        saveRelationshipState({ node: start, label: nodeLabel, id: result.start_id, hops, showNames: showNamesRef.current });
      }
      setSummary(result.count
        ? `${result.count} nodes · ${result.links.length} connections · ${result.hops} hop${result.hops === 1 ? "" : "s"}`
          + (result.truncated ? " · capped at 200" : "")
          + (result.branch_limited ? " · more connections available" : "")
        : "No connected nodes found.");
    } catch (requestError) {
      if (requestId !== requestRef.current) return;
      setGraph(null);
      setSummary("");
      setError(requestError.message);
    } finally {
      if (requestId === requestRef.current) setLoading(false);
    }
  }, [fullPage, hops, label, query]);

  runRef.current = runGraph;

  useEffect(() => {
    if (!seed) return;
    setQuery(seed.node);
    setLabel(seed.label);
    runRef.current?.(seed.node, seed.label, seed.id);
  }, [seed]);

  useEffect(() => {
    if (!fullPage || !controlRevision) return undefined;
    const start = query.trim();
    if (lastRequestRef.current === JSON.stringify([start, label, hops])) return undefined;
    ++requestRef.current;
    const timer = window.setTimeout(() => {
      const centre = exactCentreRef.current;
      const id = centre?.query === start && centre?.label === label ? centre.id : "";
      runRef.current?.(start, label, id);
    }, 350);
    return () => window.clearTimeout(timer);
  }, [fullPage, controlRevision, query, label, hops]);

  const draw = useCallback(() => {
    const canvas = canvasRef.current;
    if (!canvas) return;
    const context = canvas.getContext("2d");
    const bounds = canvas.getBoundingClientRect();
    if (!bounds.width || !bounds.height) return;
    const ratio = window.devicePixelRatio || 1;
    if (canvas.width !== Math.round(bounds.width * ratio)
      || canvas.height !== Math.round(bounds.height * ratio)) {
      canvas.width = Math.round(bounds.width * ratio);
      canvas.height = Math.round(bounds.height * ratio);
    }
    context.setTransform(ratio, 0, 0, ratio, 0, 0);
    context.clearRect(0, 0, bounds.width, bounds.height);
    const view = viewRef.current;
    const nodes = nodesRef.current;
    const links = linksRef.current;
    const rootStyles = getComputedStyle(document.documentElement);
    context.save();
    context.translate(view.x, view.y);
    context.scale(view.scale, view.scale);

    const visibleLink = (link) => typeof link.source === "object"
      && typeof link.target === "object"
      && !hiddenTypesRef.current.has(link.source.label)
      && !hiddenTypesRef.current.has(link.target.label);

    context.strokeStyle = rootStyles.getPropertyValue("--edge").trim();
    context.lineWidth = 1 / view.scale;
    context.beginPath();
    for (const link of links) {
      if (visibleLink(link) && link.label !== "figure_relative_of") {
        context.moveTo(link.source.x, link.source.y);
        context.lineTo(link.target.x, link.target.y);
      }
    }
    context.stroke();

    // Kinship edges between figures: dashed, in the figure colour.
    const kinship = links.filter((link) => visibleLink(link)
      && link.label === "figure_relative_of");
    if (kinship.length) {
      context.save();
      context.strokeStyle = rootStyles.getPropertyValue("--node-figure").trim();
      context.setLineDash([5 / view.scale, 4 / view.scale]);
      context.beginPath();
      for (const link of kinship) {
        context.moveTo(link.source.x, link.source.y);
        context.lineTo(link.target.x, link.target.y);
      }
      context.stroke();
      context.restore();
    }

    for (const node of nodes) {
      if (hiddenTypesRef.current.has(node.label)) continue;
      const radius = fullPage ? (node.id === startIdRef.current ? 20 : 15)
        : node.id === startIdRef.current ? 9 : node.label === "verse" ? 3 : 5.5;
      context.beginPath();
      context.arc(node.x || 0, node.y || 0, radius / (node === hoveredRef.current ? 0.78 : 1), 0, Math.PI * 2);
      context.fillStyle = rootStyles.getPropertyValue(colorVars[node.label]).trim() || "#888";
      context.fill();
      if (fullPage) {
        context.save();
        context.translate((node.x || 0) - 9, (node.y || 0) - 9);
        context.scale(0.75, 0.75);
        context.strokeStyle = rootStyles.getPropertyValue("--panel").trim();
        context.lineWidth = 1.8;
        context.lineCap = "round";
        context.lineJoin = "round";
        context.stroke(new Path2D(nodeIconPaths[node.label] || nodeIconPaths.verse));
        context.restore();
      }
      if ((fullPage && showNamesRef.current) || node === hoveredRef.current || node.id === startIdRef.current || view.scale > 1.5) {
        context.font = `${fullPage ? 11 / view.scale : 10 / view.scale}px sans-serif`;
        context.textAlign = "center";
        let text = nodeName(node);
        if (node.label === "verse" && node.chapter) {
          text = `${node.name || node.book_code} ${node.chapter}:${node.verse_number}`
            + (node.version_code ? ` (${node.version_code})` : "");
        } else if (node.label === "version" && node.version_code) {
          text = `${node.name || ""} (${node.version_code})`.trim();
        }
        const textX = node.x || 0;
        const textY = (node.y || 0) - radius - 4 / view.scale;
        if (fullPage) {
          const padding = 6 / view.scale;
          const width = context.measureText(text).width + padding * 2;
          const height = 22 / view.scale;
          const left = textX - width / 2;
          const top = textY - 15 / view.scale;
          const corner = 5 / view.scale;
          context.beginPath();
          context.moveTo(left + corner, top);
          context.lineTo(left + width - corner, top);
          context.quadraticCurveTo(left + width, top, left + width, top + corner);
          context.lineTo(left + width, top + height - corner);
          context.quadraticCurveTo(left + width, top + height, left + width - corner, top + height);
          context.lineTo(left + corner, top + height);
          context.quadraticCurveTo(left, top + height, left, top + height - corner);
          context.lineTo(left, top + corner);
          context.quadraticCurveTo(left, top, left + corner, top);
          context.closePath();
          context.fillStyle = rootStyles.getPropertyValue("--panel").trim();
          context.fill();
          context.strokeStyle = rootStyles.getPropertyValue(
            node.id === startIdRef.current || node === hoveredRef.current
              ? colorVars[node.label] : "--line",
          ).trim();
          context.lineWidth = 1 / view.scale;
          context.stroke();
        }
        context.fillStyle = rootStyles.getPropertyValue("--graph-text").trim();
        context.fillText(text, textX, textY);
      }
    }
    context.restore();
  }, [fullPage]);

  useEffect(() => {
    hoveredRef.current = null;
    draw();
  }, [hiddenTypes, draw]);

  useEffect(() => {
    draw();
    const centre = exactCentreRef.current;
    if (fullPage && centre?.id) {
      saveRelationshipState({ node: centre.query, label: centre.label, id: centre.id, hops, showNames });
    }
  }, [showNames, draw, fullPage, hops]);

  useEffect(() => {
    if (!graph?.nodes?.length) {
      nodesRef.current = [];
      linksRef.current = [];
      draw();
      return undefined;
    }

    const canvas = canvasRef.current;
    if (!canvas) return undefined;
    const nodes = graph.nodes.map((node, index) => {
      const angle = (index / graph.nodes.length) * Math.PI * 2;
      const radius = 40 + (node.dist || 0) * 95;
      return {
        ...node,
        x: Math.cos(angle) * radius + (Math.random() - 0.5) * 20,
        y: Math.sin(angle) * radius + (Math.random() - 0.5) * 20,
      };
    });
    const links = graph.links.map((link) => ({ ...link }));
    nodesRef.current = nodes;
    linksRef.current = links;

    const width = canvas.clientWidth;
    const height = canvas.clientHeight;
    const minX = Math.min(...nodes.map((node) => node.x));
    const maxX = Math.max(...nodes.map((node) => node.x));
    const minY = Math.min(...nodes.map((node) => node.y));
    const maxY = Math.max(...nodes.map((node) => node.y));
    const scale = Math.min(
      (width - 50) / Math.max(maxX - minX, 1),
      (height - 50) / Math.max(maxY - minY, 1),
      2.5,
    );
    viewRef.current = {
      x: width / 2 - ((minX + maxX) / 2) * scale,
      y: height / 2 - ((minY + maxY) / 2) * scale,
      scale,
    };

    const simulation = forceSimulation(nodes)
      .force("link", forceLink(links).id((node) => node.id).distance(fullPage ? 100 : 68).strength(0.35))
      .force("charge", forceManyBody().strength(fullPage ? -180 : -85))
      .force("collision", fullPage ? forceCollide(23) : null)
      .force("x", forceX(0).strength(0.025))
      .force("y", forceY(0).strength(0.025))
      .force("center", forceCenter(0, 0))
      .on("tick", draw);
    const observer = new ResizeObserver(draw);
    observer.observe(canvas);
    draw();
    const settleTimer = window.setTimeout(() => simulation.stop(), 8000);
    return () => {
      window.clearTimeout(settleTimer);
      simulation.stop();
      observer.disconnect();
    };
  }, [draw, fullPage, graph]);

  useEffect(() => {
    const observer = new MutationObserver(draw);
    observer.observe(document.documentElement, { attributes: true, attributeFilter: ["class"] });
    return () => observer.disconnect();
  }, [draw]);

  const toWorld = (event) => {
    const rect = canvasRef.current.getBoundingClientRect();
    const view = viewRef.current;
    return {
      x: (event.clientX - rect.left - view.x) / view.scale,
      y: (event.clientY - rect.top - view.y) / view.scale,
    };
  };

  const nodeAt = (event) => {
    const point = toWorld(event);
    return [...nodesRef.current].reverse().find((node) => {
      if (hiddenTypesRef.current.has(node.label)) return false;
      const radius = fullPage ? (node.id === startIdRef.current ? 20 : 15)
        : node.id === startIdRef.current ? 9 : node.label === "verse" ? 3 : 5.5;
      const dx = point.x - node.x;
      const dy = point.y - node.y;
      return dx * dx + dy * dy < (radius + 5 / viewRef.current.scale) ** 2;
    });
  };

  const onPointerDown = (event) => {
    const node = nodeAt(event);
    dragRef.current = {
      x: event.clientX,
      y: event.clientY,
      node,
      moved: false,
      pointerId: event.pointerId,
    };
    event.currentTarget.setPointerCapture(event.pointerId);
  };

  const onPointerMove = (event) => {
    const drag = dragRef.current;
    if (!drag) {
      hoveredRef.current = nodeAt(event) || null;
      event.currentTarget.style.cursor = hoveredRef.current ? "pointer" : "grab";
      draw();
      return;
    }
    const dx = event.clientX - drag.x;
    const dy = event.clientY - drag.y;
    if (Math.abs(dx) + Math.abs(dy) > 3) drag.moved = true;
    if (drag.node) {
      const position = toWorld(event);
      drag.node.fx = position.x;
      drag.node.fy = position.y;
      drag.node.x = position.x;
      drag.node.y = position.y;
    } else {
      viewRef.current.x += dx;
      viewRef.current.y += dy;
    }
    drag.x = event.clientX;
    drag.y = event.clientY;
    draw();
  };

  const onPointerUp = (event) => {
    const drag = dragRef.current;
    if (!drag) return;
    if (!drag.moved && drag.node) {
      hoveredRef.current = drag.node;
      runRef.current?.(graphNodeQuery(drag.node), drag.node.label, drag.node.id);
    }
    if (drag.node) {
      drag.node.fx = null;
      drag.node.fy = null;
    }
    dragRef.current = null;
    if (event.currentTarget.hasPointerCapture(drag.pointerId)) {
      event.currentTarget.releasePointerCapture(drag.pointerId);
    }
  };

  const onWheel = (event) => {
    event.preventDefault();
    const rect = canvasRef.current.getBoundingClientRect();
    const view = viewRef.current;
    const x = event.clientX - rect.left;
    const y = event.clientY - rect.top;
    const factor = event.deltaY < 0 ? 1.12 : 1 / 1.12;
    view.x = x - (x - view.x) * factor;
    view.y = y - (y - view.y) * factor;
    view.scale = Math.max(0.15, Math.min(5, view.scale * factor));
    draw();
  };

  const resetView = () => {
    const nodes = nodesRef.current.filter((node) => !hiddenTypesRef.current.has(node.label));
    if (!nodes.length) return;
    const canvas = canvasRef.current;
    const width = canvas.clientWidth;
    const height = canvas.clientHeight;
    const minX = Math.min(...nodes.map((node) => node.x));
    const maxX = Math.max(...nodes.map((node) => node.x));
    const minY = Math.min(...nodes.map((node) => node.y));
    const maxY = Math.max(...nodes.map((node) => node.y));
    const scale = Math.min(
      (width - 50) / Math.max(maxX - minX, 1),
      (height - 50) / Math.max(maxY - minY, 1),
      2.5,
    );
    viewRef.current = {
      x: width / 2 - ((minX + maxX) / 2) * scale,
      y: height / 2 - ((minY + maxY) / 2) * scale,
      scale,
    };
    draw();
  };

  const toggleFullscreen = async () => {
    const wrapper = wrapperRef.current;
    try {
      if (document.fullscreenElement) await document.exitFullscreen();
      else await wrapper.requestFullscreen();
      window.setTimeout(resetView, 100);
    } catch (fullscreenError) {
      setError(`Could not enter fullscreen: ${fullscreenError.message}`);
    }
  };

  const legend = [...new Set(graph?.nodes?.map((node) => node.label) || [])];
  const hasKinship = (graph?.links || []).some((link) => link.label === "figure_relative_of");
  const centreNode = graph?.nodes?.find((node) => node.id === graph.start_id);
  const hiddenCentreConnections = graph?.hidden_connections?.[graph.start_id] || {};
  const readerHref = (node) => `/read?${new URLSearchParams({
    book: node.book_code,
    chapter: String(node.chapter),
    verse: String(node.verse_number),
    version: node.version_code || "KJV",
  })}`;
  const connectedPlaces = centreNode?.label === "verse"
    ? graph.nodes.filter((node) => node.label === "location" && graph.links.some((link) =>
      (link.source === centreNode.id && link.target === node.id)
      || (link.target === centreNode.id && link.source === node.id)))
    : [];
  const connectedGroups = new Map();
  if (centreNode) {
    const nodesById = new Map(graph.nodes.map((node) => [node.id, node]));
    for (const link of graph.links) {
      const outgoing = link.source === centreNode.id;
      if (!outgoing && link.target !== centreNode.id) continue;
      const node = nodesById.get(outgoing ? link.target : link.source);
      if (!node) continue;
      const group = connectedGroups.get(node.label) || new Map();
      const entry = group.get(node.id) || { node, relationships: new Set() };
      entry.relationships.add(
        relationshipDescription(link.label, outgoing, link.attrs),
      );
      group.set(node.id, entry);
      connectedGroups.set(node.label, group);
    }
  }

  const depthControl = (
    <select aria-label="Graph depth" value={hops} onChange={(event) => {
      setHops(event.target.value);
      setControlRevision((value) => value + 1);
    }}>
      {Array.from({ length: fullPage ? 20 : 3 }, (_, index) => [
        String(index + 1), `${index + 1} hop${index ? "s" : ""}`,
      ]).map(([value, text]) => (
        <option key={value} value={value}>{text}</option>
      ))}
    </select>
  );

  return (
    <div className={`graph-explorer${fullPage ? " graph-explorer-full" : ""}`}>
      <div className="relationship-toolbar">
      <form className="form-row graph-controls" onSubmit={(event) => {
        event.preventDefault();
        const centre = exactCentreRef.current;
        runGraph(query, label, fullPage && centre?.query === query.trim() && centre?.label === label ? centre.id : "");
      }}>
        {!fullPage && <input
          aria-label="Graph centre"
          value={query}
          onChange={(event) => {
            setQuery(event.target.value);
            setControlRevision((value) => value + 1);
          }}
          placeholder="Centre — e.g. JOH"
        />}
        {!fullPage && <select aria-label="Graph node type" value={label} onChange={(event) => {
          setLabel(event.target.value);
          setControlRevision((value) => value + 1);
        }}>
          {["book", "version", "region", "location", "verse", "figure"].map((item) => (
            <option key={item}>{item}</option>
          ))}
        </select>}
        {!fullPage && depthControl}
        {!fullPage && <button className="button button-secondary" disabled={loading}>
          {loading ? "Drawing…" : "Draw graph"}
        </button>}
      </form>
      </div>
      {!fullPage && summary && <p className="result-summary" role="status">{summary}</p>}
      {error && <p className="notice error" role="alert">{error}</p>}
      <div className={fullPage ? `relationship-workspace${centreNode ? " has-details" : ""}` : "graph-workspace"}>
      {fullPage && centreNode && (
        <section className="relationship-detail" aria-label="Selected node details">
          <strong>{centreNode.label}: {nodeName(centreNode)}</strong>
          {centreNode.label === "verse" && (
            <>
              <span>{centreNode.book_code} {centreNode.chapter}:{centreNode.verse_number} · {centreNode.version_code}</span>
              <p>{centreNode.attrs?.text || "Passage text is not available."}</p>
              <a href={readerHref(centreNode)}>Read passage →</a>
              {connectedPlaces.map((place) => (
                <span className="relationship-place-links" key={place.id}>
                  <button type="button" onClick={() => runGraph(graphNodeQuery(place), place.label, place.id)}>
                    Explore {place.name} relationships
                  </button>
                  <a href={`/map?${new URLSearchParams({ location_id: place.id })}`}>Open {place.name} on map →</a>
                </span>
              ))}
            </>
          )}
          {centreNode.label === "location" && (
            <>
              {centreNode.attrs?.region && <span>{centreNode.attrs.region}</span>}
              <a href={`/map?${new URLSearchParams({ location_id: centreNode.id })}`}>Open map →</a>
              {centreNode.book_code && centreNode.chapter && centreNode.verse_number && (
                <a href={`${readerHref(centreNode)}&${new URLSearchParams({ place_id: centreNode.id })}`}>Read passage →</a>
              )}
            </>
          )}
          {centreNode.label === "figure" && (
            <>
              {(centreNode.attrs?.testament || centreNode.attrs?.category) && (
                <span>{[centreNode.attrs?.testament, centreNode.attrs?.category]
                  .filter(Boolean).join(" · ")}</span>
              )}
              {centreNode.attrs?.description && <p>{centreNode.attrs.description}</p>}
            </>
          )}
          <div className="relationship-connections">
            <h3>Connected nodes</h3>
            {graph.truncated && (
              <p className="relationship-connections-note">Showing connections included in this graph. More may exist beyond its node limit.</p>
            )}
            {Object.keys(hiddenCentreConnections).length > 0 && (
              <div className="relationship-hidden-connections">
                <ul>
                  {Object.entries(hiddenCentreConnections).map(([type, count]) => (
                    <li key={type}>{count} more {(nodeTypeNames[type] || type).toLocaleLowerCase()}</li>
                  ))}
                </ul>
                <button type="button" disabled={loading || graph.truncated || graph.branch_expansion >= 20}
                  onClick={() => runGraph(graphNodeQuery(centreNode), centreNode.label, centreNode.id, graph.branch_expansion + 1)}>
                  Show more connections
                </button>
                {graph.truncated && <p className="relationship-connections-note">Node budget reached. Select a connected node to explore its branch.</p>}
              </div>
            )}
            {graph.branch_limited && !Object.keys(hiddenCentreConnections).length && (
              <p className="relationship-connections-note">Other branches have more connections. Select a connected node to see its hidden counts and expand it.</p>
            )}
            {connectedGroups.size === 0 && <p>No direct connections in this graph.</p>}
            {[...connectedGroups].sort(([left], [right]) =>
              (nodeTypeNames[left] || left).localeCompare(nodeTypeNames[right] || right))
              .map(([type, entries]) => (
                <details key={`${centreNode.id}-${type}`} open>
                  <summary>
                    <NodeIcon type={type} />
                    {nodeTypeNames[type] || type} <span>{entries.size}</span>
                  </summary>
                  <ul>
                    {[...entries.values()].sort((left, right) =>
                      nodeDescription(left.node).localeCompare(nodeDescription(right.node), undefined, { numeric: true }))
                      .map(({ node, relationships }) => (
                        <li key={node.id}>
                          <button
                            type="button"
                            disabled={loading}
                            onClick={() => runGraph(graphNodeQuery(node), node.label, node.id)}
                          >
                            <strong>{nodeDescription(node)}</strong>
                            <span>{[...relationships].join(" · ")}</span>
                          </button>
                        </li>
                      ))}
                  </ul>
                </details>
              ))}
          </div>
        </section>
      )}
      <div className="graph-wrap" ref={wrapperRef}>
        {fullPage && summary && <p className="result-summary graph-summary-overlay" role="status">
          {summary}
          {!loading && graph && ` · ${graph.nodes.filter((node) => !hiddenTypes.has(node.label)).length} / ${graph.nodes.length} visible`}
        </p>}
        {fullPage && (
          <div className="graph-search-overlay">
            <GraphNodeSearch node={centreNode} fallback={query} onSelect={(node) => {
              runGraph(graphNodeQuery(node), node.label, node.id);
            }} />
            {depthControl}
            {graph?.nodes?.length > 0 && (
              <details className="relationship-node-menu" ref={nodeMenuRef}
                onKeyDown={(event) => {
                  if (event.key === "Escape") {
                    nodeMenuRef.current.open = false;
                    nodeMenuRef.current.querySelector("summary").focus();
                  }
                }}>
                <summary aria-label="More graph options">
                  <svg width="20" height="20" viewBox="0 0 24 24" aria-hidden="true">
                    <circle cx="5" cy="12" r="2" fill="currentColor" />
                    <circle cx="12" cy="12" r="2" fill="currentColor" />
                    <circle cx="19" cy="12" r="2" fill="currentColor" />
                  </svg>
                </summary>
                <div className="relationship-node-menu-panel">
                  <label className="relationship-names-toggle">
                    <input type="checkbox" checked={showNames}
                      onChange={(event) => setShowNames(event.target.checked)} />
                    Show node names
                  </label>
                  <label className="relationship-node-picker">
                    Explore a node
                    <select
                      aria-label="Explore a relationship node"
                      value=""
                      onChange={(event) => {
                        const node = graph.nodes.find((item) => item.id === event.target.value);
                        if (node) {
                          runGraph(graphNodeQuery(node), node.label, node.id);
                          nodeMenuRef.current.open = false;
                          nodeMenuRef.current.querySelector("summary").focus();
                        }
                      }}
                    >
                      <option value="">Choose a node</option>
                      {graph.nodes.map((node) => (
                        <option key={node.id} value={node.id}>
                          {node.label}: {nodeName(node)}{node.chapter ? ` ${node.chapter}:${node.verse_number} (${node.version_code})` : ""}
                        </option>
                      ))}
                    </select>
                  </label>
                </div>
              </details>
            )}
          </div>
        )}
        <button
          type="button"
          className="graph-fullscreen"
          title="Toggle graph fullscreen"
          aria-label="Toggle graph fullscreen"
          onClick={toggleFullscreen}
        >⛶</button>
        {fullPage && (
          <button className="relationship-refit" type="button" onClick={resetView}>Fit graph</button>
        )}
        <canvas
          ref={canvasRef}
          className="graph-canvas"
          aria-label="Interactive graph; select a node to centre the graph there"
          onPointerDown={onPointerDown}
          onPointerMove={onPointerMove}
          onPointerUp={onPointerUp}
          onPointerCancel={onPointerUp}
          onWheel={onWheel}
          onDoubleClick={resetView}
          onPointerLeave={() => {
            if (!dragRef.current) {
              hoveredRef.current = null;
              draw();
            }
          }}
        />
        {legend.length > 0 && (
          <div className="graph-legend" role={fullPage ? "group" : undefined} aria-label="Node types">
            {legend.map((item) => fullPage ? (
              <label key={item} className="graph-type-toggle">
                <input type="checkbox" checked={!hiddenTypes.has(item)}
                  aria-label={`Show ${nodeTypeNames[item] || item}`}
                  onChange={(event) => {
                    const checked = event.target.checked;
                    setHiddenTypes((current) => {
                      const next = new Set(current);
                      if (checked) next.delete(item);
                      else next.add(item);
                      return next;
                    });
                  }} />
                <NodeIcon type={item} />{nodeTypeNames[item] || item}
              </label>
            ) : <span key={item}><i className={`legend-dot badge-${item}`} />{item}</span>)}
            {hasKinship && <span><i className="legend-dash" />Kinship</span>}
          </div>
        )}
        {!graph && !loading && !error && <div className="graph-placeholder">Your graph will appear here</div>}
      </div>
      </div>
      {!fullPage && <p className="graph-tip">Click a node to explore its connections · double-click to refit</p>}
    </div>
  );
}

export default GraphExplorer;
