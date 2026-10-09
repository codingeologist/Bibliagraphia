import { useCallback, useEffect, useRef, useState } from "react";
import {
  forceCenter,
  forceLink,
  forceManyBody,
  forceSimulation,
  forceX,
  forceY,
} from "d3-force";
import { get } from "./api.js";

const colorVars = {
  book: "--node-book",
  version: "--node-version",
  region: "--node-region",
  location: "--node-location",
  verse: "--node-verse",
};

const nodeName = (node) => node.name || node.id;
const graphNodeQuery = (node) =>
  node.label === "book" ? node.book_code || node.name
    : node.label === "version" ? node.version_code || node.name
      : node.name || node.book_code;

function GraphExplorer({ seed }) {
  const [query, setQuery] = useState("");
  const [label, setLabel] = useState("book");
  const [hops, setHops] = useState("1");
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

  const runGraph = useCallback(async (node = query, nodeLabel = label) => {
    const start = node.trim();
    if (!start) return;
    setLoading(true);
    setError("");
    setSummary("Loading graph…");
    try {
      const params = new URLSearchParams({ node: start, label: nodeLabel, hops });
      const result = await get(`/graph?${params}`);
      setQuery(start);
      setLabel(nodeLabel);
      startIdRef.current = result.start_id;
      setGraph(result);
      setSummary(result.count
        ? `${result.count} nodes · ${result.links.length} connections · ${result.hops} hop${result.hops === 1 ? "" : "s"}`
          + (result.truncated ? " · outer ring capped at 200" : "")
        : "No connected nodes found.");
    } catch (requestError) {
      setGraph(null);
      setSummary("");
      setError(requestError.message);
    } finally {
      setLoading(false);
    }
  }, [hops, label, query]);

  runRef.current = runGraph;

  useEffect(() => {
    if (!seed) return;
    setQuery(seed.node);
    setLabel(seed.label);
    runRef.current?.(seed.node, seed.label);
  }, [seed]);

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

    context.strokeStyle = rootStyles.getPropertyValue("--edge").trim();
    context.lineWidth = 1 / view.scale;
    context.beginPath();
    for (const link of links) {
      if (typeof link.source === "object" && typeof link.target === "object") {
        context.moveTo(link.source.x, link.source.y);
        context.lineTo(link.target.x, link.target.y);
      }
    }
    context.stroke();

    for (const node of nodes) {
      const radius = node.id === startIdRef.current ? 9 : node.label === "verse" ? 3 : 5.5;
      context.beginPath();
      context.arc(node.x || 0, node.y || 0, radius / (node === hoveredRef.current ? 0.78 : 1), 0, Math.PI * 2);
      context.fillStyle = rootStyles.getPropertyValue(colorVars[node.label]).trim() || "#888";
      context.fill();
      if (node === hoveredRef.current || node.id === startIdRef.current || view.scale > 1.5) {
        context.fillStyle = rootStyles.getPropertyValue("--graph-text").trim();
        context.font = `${10 / view.scale}px sans-serif`;
        context.textAlign = "center";
        let text = nodeName(node);
        if (node.label === "verse" && node.chapter) {
          text = `${node.name || node.book_code} ${node.chapter}:${node.verse_number}`
            + (node.version_code ? ` (${node.version_code})` : "");
        } else if (node.label === "version" && node.version_code) {
          text = `${node.name || ""} (${node.version_code})`.trim();
        }
        context.fillText(text, node.x || 0, (node.y || 0) - radius - 4 / view.scale);
      }
    }
    context.restore();
  }, []);

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
      .force("link", forceLink(links).id((node) => node.id).distance(68).strength(0.35))
      .force("charge", forceManyBody().strength(-85))
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
  }, [draw, graph]);

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
      const radius = node.id === startIdRef.current ? 9 : node.label === "verse" ? 3 : 5.5;
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
      runRef.current?.(graphNodeQuery(drag.node), drag.node.label);
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
    const nodes = nodesRef.current;
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

  return (
    <div className="graph-explorer">
      <form className="form-row graph-controls" onSubmit={(event) => { event.preventDefault(); runGraph(); }}>
        <input
          aria-label="Graph centre"
          value={query}
          onChange={(event) => setQuery(event.target.value)}
          placeholder="Centre — e.g. JOH"
        />
        <select aria-label="Graph node type" value={label} onChange={(event) => setLabel(event.target.value)}>
          {["book", "version", "region", "location", "verse"].map((item) => (
            <option key={item}>{item}</option>
          ))}
        </select>
        <select aria-label="Graph depth" value={hops} onChange={(event) => setHops(event.target.value)}>
          {[["1", "1 hop"], ["2", "2 hops"], ["3", "3 hops"]].map(([value, text]) => (
            <option key={value} value={value}>{text}</option>
          ))}
        </select>
        <button className="button button-secondary" disabled={loading}>
          {loading ? "Drawing…" : "Draw graph"}
        </button>
      </form>
      {summary && <p className="result-summary" role="status">{summary}</p>}
      {error && <p className="notice error" role="alert">{error}</p>}
      <div className="graph-wrap" ref={wrapperRef}>
        <button
          type="button"
          className="graph-fullscreen"
          title="Toggle graph fullscreen"
          aria-label="Toggle graph fullscreen"
          onClick={toggleFullscreen}
        >⛶</button>
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
          <div className="graph-legend" aria-label="Node types">
            {legend.map((item) => <span key={item}><i className={`legend-dot badge-${item}`} />{item}</span>)}
          </div>
        )}
        {!graph && !loading && !error && <div className="graph-placeholder">Your graph will appear here</div>}
      </div>
      <p className="graph-tip">Click a node to explore its connections · double-click to refit</p>
    </div>
  );
}

export default GraphExplorer;
