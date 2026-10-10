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
import { nodeTypeName, relationshipDescription } from "./nodeLinks.js";

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
  figure: "People",
};

// Force-layout settings. The Relationships page lets the user tune them
// (not saved); the small home-page panel always uses PANEL_PHYSICS.
const PAGE_PHYSICS = {
  repulsion: 180, linkDistance: 100, linkStrength: 0.35, gravity: 0.025, spacing: 23, dragPull: 1,
};
const PANEL_PHYSICS = { ...PAGE_PHYSICS, repulsion: 85, linkDistance: 68, spacing: 0 };
const PHYSICS_CONTROLS = [
  ["repulsion", "Repulsion (magnetism)", 0, 600, 10, "How strongly nodes push each other apart"],
  ["linkDistance", "Link length", 20, 300, 5, "Preferred length of each connection"],
  ["linkStrength", "Link stiffness", 0, 1, 0.05, "How firmly connections hold their length"],
  ["gravity", "Pull to centre", 0, 0.2, 0.005, "How strongly every node drifts back to the middle"],
  ["spacing", "Spacing", 0, 60, 1, "Minimum gap kept around each node"],
  ["dragPull", "Drag pull", 0, 3, 0.1, "How hard a dragged node tows the nodes linked to it"],
];

const applyPhysics = (simulation, physics) => {
  simulation.force("link")?.distance(physics.linkDistance).strength(physics.linkStrength);
  simulation.force("charge")?.strength(-physics.repulsion);
  simulation.force("collision", physics.spacing > 0 ? forceCollide(physics.spacing) : null);
  simulation.force("x")?.strength(physics.gravity);
  simulation.force("y")?.strength(physics.gravity);
};

// A node is hidden when its type is switched off, or when it belongs to a
// translation that is switched off (the translation node and its passages).
const nodeHidden = (node, hiddenTypes, hiddenVersions) => hiddenTypes.has(node.label)
  || Boolean(node.version_code && hiddenVersions.has(node.version_code));

// Children of each node in a breadth-first tree grown from the graph centre:
// a node's parent is the neighbour that first reached it, so every node has
// at most one parent and dragging a node can carry its whole subtree.
const childTree = (nodes, links, rootId) => {
  const adjacent = new Map(nodes.map((node) => [node.id, []]));
  for (const link of links) {
    const source = link.source.id ?? link.source;
    const target = link.target.id ?? link.target;
    adjacent.get(source)?.push(target);
    adjacent.get(target)?.push(source);
  }
  const children = new Map();
  const seen = new Set([rootId]);
  const queue = [rootId];
  while (queue.length) {
    const id = queue.shift();
    for (const next of adjacent.get(id) || []) {
      if (seen.has(next)) continue;
      seen.add(next);
      queue.push(next);
      if (!children.has(id)) children.set(id, []);
      children.get(id).push(next);
    }
  }
  return children;
};

// While a node is dragged, pull each node linked to the dragged group back to
// link length with a strong spring, so the rest of the graph stretches after
// the drag. d3's own link force splits the pull by node degree, which leaves
// a well-connected centre almost still.
const dragPull = (pairs, distance, strength) => (alpha) => {
  for (const [other, anchor] of pairs) {
    const dx = anchor.x - other.x;
    const dy = anchor.y - other.y;
    const length = Math.hypot(dx, dy) || 1;
    const k = ((length - distance) / length) * strength * alpha;
    other.vx += dx * k;
    other.vy += dy * k;
  }
};

const descendants = (children, id) => {
  const found = [];
  const stack = [...(children.get(id) || [])];
  while (stack.length) {
    const next = stack.pop();
    found.push(next);
    stack.push(...(children.get(next) || []));
  }
  return found;
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
  const [physics, setPhysics] = useState(fullPage ? PAGE_PHYSICS : PANEL_PHYSICS);
  const physicsRef = useRef(physics);
  physicsRef.current = physics;
  // Relationships page only: max nodes to request (not saved between visits).
  const DEFAULT_NODE_LIMIT = 200;
  const nodeLimitRef = useRef(DEFAULT_NODE_LIMIT);
  const [nodeLimitInput, setNodeLimitInput] = useState(String(DEFAULT_NODE_LIMIT));
  const showNamesRef = useRef(showNames);
  showNamesRef.current = showNames;
  const [hiddenTypes, setHiddenTypes] = useState(() => new Set());
  const hiddenTypesRef = useRef(hiddenTypes);
  hiddenTypesRef.current = hiddenTypes;
  const [hiddenVersions, setHiddenVersions] = useState(() => new Set());
  const hiddenVersionsRef = useRef(hiddenVersions);
  hiddenVersionsRef.current = hiddenVersions;
  const [graph, setGraph] = useState(null);
  const [summary, setSummary] = useState("");
  const [error, setError] = useState("");
  const [loading, setLoading] = useState(false);
  const canvasRef = useRef(null);
  const wrapperRef = useRef(null);
  const nodesRef = useRef([]);
  const linksRef = useRef([]);
  const childrenRef = useRef(new Map());
  const simulationRef = useRef(null);
  const viewRef = useRef({ x: 0, y: 0, scale: 1 });
  const startIdRef = useRef("");
  const dragRef = useRef(null);
  const pointersRef = useRef(new Map()); // active pointers, for pinch zoom
  const pinchRef = useRef(null);
  // Phones: the type / translation filters cover much of the graph, so they
  // start folded behind a "Filters" button (CSS ignores this on wider screens).
  const [legendOpen, setLegendOpen] = useState(false);
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
      setError("Choose a passage, person or place to explore.");
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
    setSummary("Loading connections…");
    try {
      const params = new URLSearchParams({ node: start, label: nodeLabel, hops });
      // Balanced mode in both views: passages are capped per branch so
      // figures and their kinship edges stay visible in the node budget.
      params.set("balanced", "true");
      params.set("branch_expansion", String(branchExpansionRef.current));
      if (fullPage) params.set("limit", String(nodeLimitRef.current));
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
        ? `${result.count} items · ${result.links.length} connections · ${result.hops} connection step${result.hops === 1 ? "" : "s"}`
          + (result.truncated ? ` · showing up to ${fullPage ? nodeLimitRef.current : DEFAULT_NODE_LIMIT} items` : "")
          + (result.branch_limited ? " · more connections available" : "")
        : "No recorded connections found.");
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
      && !nodeHidden(link.source, hiddenTypesRef.current, hiddenVersionsRef.current)
      && !nodeHidden(link.target, hiddenTypesRef.current, hiddenVersionsRef.current);

    const isJesusNode = (node) => {
      const name = node.name || node.id || "";
      return name.includes("Jesus") || name === "Jesus Christ" || name === "Jesus";
    };

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
      context.setLineDash([5 / view.scale, 4 / view.scale]);
      
      // Draw Jesus kinship edges in gold
      const jesusKinship = kinship.filter(link => {
        const sourceNode = nodesRef.current.find(n => n.id === link.source.id);
        const targetNode = nodesRef.current.find(n => n.id === link.target.id);
        return isJesusNode(sourceNode) || isJesusNode(targetNode);
      });
      if (jesusKinship.length) {
        context.strokeStyle = "#FFD700";
        context.beginPath();
        for (const link of jesusKinship) {
          context.moveTo(link.source.x, link.source.y);
          context.lineTo(link.target.x, link.target.y);
        }
        context.stroke();
      }
      
      // Draw regular kinship edges in figure color
      const regularKinship = kinship.filter(link => {
        const sourceNode = nodesRef.current.find(n => n.id === link.source.id);
        const targetNode = nodesRef.current.find(n => n.id === link.target.id);
        return !isJesusNode(sourceNode) && !isJesusNode(targetNode);
      });
      if (regularKinship.length) {
        context.strokeStyle = rootStyles.getPropertyValue("--node-figure").trim();
        context.beginPath();
        for (const link of regularKinship) {
          context.moveTo(link.source.x, link.source.y);
          context.lineTo(link.target.x, link.target.y);
        }
        context.stroke();
      }
      context.restore();
    }

    for (const node of nodes) {
      if (nodeHidden(node, hiddenTypesRef.current, hiddenVersionsRef.current)) continue;
      const radius = fullPage ? (node.id === startIdRef.current ? 20 : 15)
        : node.id === startIdRef.current ? 9 : node.label === "verse" ? 3 : 5.5;
      context.beginPath();
      context.arc(node.x || 0, node.y || 0, radius / (node === hoveredRef.current ? 0.78 : 1), 0, Math.PI * 2);
      const isJesus = isJesusNode(node);
      context.fillStyle = isJesus ? "#FFD700" : (rootStyles.getPropertyValue(colorVars[node.label]).trim() || "#888");
      context.fill();
      if (fullPage) {
        context.save();
        context.translate((node.x || 0) - 9, (node.y || 0) - 9);
        context.scale(0.75, 0.75);
        context.strokeStyle = isJesus ? "#FFD700" : rootStyles.getPropertyValue("--panel").trim();
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
          context.strokeStyle = isJesus ? "#FFD700" : rootStyles.getPropertyValue(
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
  }, [hiddenTypes, hiddenVersions, draw]);

  // Slider changes apply to the running layout and give it a nudge to resettle.
  useEffect(() => {
    const simulation = simulationRef.current;
    if (!simulation) return;
    applyPhysics(simulation, physics);
    simulation.alpha(Math.max(simulation.alpha(), 0.3)).restart();
  }, [physics]);

  // Re-centring on a passage or translation of a hidden translation shows it again.
  useEffect(() => {
    const code = graph?.nodes?.find((node) => node.id === graph.start_id)?.version_code;
    if (!code) return;
    setHiddenVersions((current) => {
      if (!current.has(code)) return current;
      const next = new Set(current);
      next.delete(code);
      return next;
    });
  }, [graph]);

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
    childrenRef.current = childTree(nodes, links, graph.start_id || startIdRef.current);

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
      .force("link", forceLink(links).id((node) => node.id))
      .force("charge", forceManyBody())
      .force("x", forceX(0))
      .force("y", forceY(0))
      .force("center", forceCenter(0, 0))
      .on("tick", draw);
    applyPhysics(simulation, physicsRef.current);
    simulationRef.current = simulation;
    const observer = new ResizeObserver(draw);
    observer.observe(canvas);
    draw();
    // No settle timer: d3 cools and stops by itself (~300 ticks), and a fixed
    // timer would kill the physics in the middle of a drag.
    return () => {
      simulation.stop();
      if (simulationRef.current === simulation) simulationRef.current = null;
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
      if (nodeHidden(node, hiddenTypesRef.current, hiddenVersionsRef.current)) return false;
      const radius = fullPage ? (node.id === startIdRef.current ? 20 : 15)
        : node.id === startIdRef.current ? 9 : node.label === "verse" ? 3 : 5.5;
      const dx = point.x - node.x;
      const dy = point.y - node.y;
      // Fingers are imprecise: give touch a larger hit area than the mouse.
      const slop = event.pointerType === "touch" ? 14 : 5;
      return dx * dx + dy * dy < (radius + slop / viewRef.current.scale) ** 2;
    });
  };

  // Zoom by `factor` about a point in canvas pixels, keeping that point still.
  const zoomAt = (x, y, factor) => {
    const view = viewRef.current;
    const scale = Math.max(0.15, Math.min(5, view.scale * factor));
    const applied = scale / view.scale;
    view.x = x - (x - view.x) * applied;
    view.y = y - (y - view.y) * applied;
    view.scale = scale;
  };

  // Ends a node drag without treating it as a click (also used when a second
  // finger turns the gesture into a pinch).
  const endDrag = (drag) => {
    if (!drag?.node) return;
    for (const item of [drag.node, ...drag.group]) {
      item.fx = null;
      item.fy = null;
    }
    if (drag.heated) simulationRef.current?.force("dragPull", null).alphaTarget(0);
  };

  const pinchInfo = () => {
    const [a, b] = [...pointersRef.current.values()];
    const rect = canvasRef.current.getBoundingClientRect();
    return {
      distance: Math.hypot(a.x - b.x, a.y - b.y) || 1,
      x: (a.x + b.x) / 2 - rect.left,
      y: (a.y + b.y) / 2 - rect.top,
    };
  };

  const onPointerDown = (event) => {
    pointersRef.current.set(event.pointerId, { x: event.clientX, y: event.clientY });
    event.currentTarget.setPointerCapture(event.pointerId);
    if (pointersRef.current.size === 2) {
      // Two fingers: pinch to zoom and pan; abandon any drag the first finger began.
      endDrag(dragRef.current);
      dragRef.current = null;
      pinchRef.current = pinchInfo();
      return;
    }
    if (pointersRef.current.size > 2 || pinchRef.current) return;
    const node = nodeAt(event);
    // Dragging a node carries its subtree; Shift-drag moves the node alone.
    const byId = new Map(nodesRef.current.map((item) => [item.id, item]));
    const group = node && !event.shiftKey
      ? descendants(childrenRef.current, node.id).map((id) => byId.get(id)).filter(Boolean)
      : [];
    dragRef.current = {
      x: event.clientX,
      y: event.clientY,
      startX: event.clientX,
      startY: event.clientY,
      node,
      group,
      moved: false,
      pointerId: event.pointerId,
    };
  };

  const onPointerMove = (event) => {
    if (pointersRef.current.has(event.pointerId)) {
      pointersRef.current.set(event.pointerId, { x: event.clientX, y: event.clientY });
    }
    if (pinchRef.current && pointersRef.current.size >= 2) {
      const previous = pinchRef.current;
      const next = pinchInfo();
      viewRef.current.x += next.x - previous.x;
      viewRef.current.y += next.y - previous.y;
      zoomAt(next.x, next.y, next.distance / previous.distance);
      pinchRef.current = next;
      draw();
      return;
    }
    const drag = dragRef.current;
    if (!drag) {
      hoveredRef.current = nodeAt(event) || null;
      event.currentTarget.style.cursor = hoveredRef.current ? "pointer" : "grab";
      draw();
      return;
    }
    const dx = event.clientX - drag.x;
    const dy = event.clientY - drag.y;
    // Distance from where the drag started, not from the last event: pointer
    // events arrive every pixel or two, so a per-event check never passes.
    if (Math.abs(event.clientX - drag.startX) + Math.abs(event.clientY - drag.startY) > 3) {
      drag.moved = true;
    }
    if (drag.node && drag.moved && !drag.heated) {
      // Keep the physics warm only while a node is being dragged, so nearby
      // nodes make room; on release it cools and stops by itself (~4s).
      drag.heated = true;
      const simulation = simulationRef.current;
      if (simulation) {
        const moving = new Set([drag.node, ...drag.group]);
        const pairs = linksRef.current.flatMap(({ source, target }) => (
          moving.has(source) && !moving.has(target) ? [[target, source]]
            : moving.has(target) && !moving.has(source) ? [[source, target]] : []
        ));
        // forceCenter would shift every free node against the drag to keep the
        // graph's mean at the origin; turn it off once the user moves things.
        simulation.force("center")?.strength(0);
        simulation.force("dragPull", dragPull(pairs, physicsRef.current.linkDistance, physicsRef.current.dragPull));
        // Start warm: after a previous drag the simulation has cooled to ~0,
        // and alphaTarget alone would take ~1s to heat it back up.
        simulation.alpha(Math.max(simulation.alpha(), 0.3)).alphaTarget(0.3).restart();
      }
    }
    if (drag.node) {
      const position = toWorld(event);
      const shiftX = position.x - drag.node.x;
      const shiftY = position.y - drag.node.y;
      for (const item of [drag.node, ...drag.group]) {
        item.x += shiftX;
        item.y += shiftY;
        item.fx = item.x;
        item.fy = item.y;
      }
    } else {
      viewRef.current.x += dx;
      viewRef.current.y += dy;
    }
    drag.x = event.clientX;
    drag.y = event.clientY;
    draw();
  };

  const onPointerUp = (event) => {
    pointersRef.current.delete(event.pointerId);
    if (event.currentTarget.hasPointerCapture(event.pointerId)) {
      event.currentTarget.releasePointerCapture(event.pointerId);
    }
    if (pinchRef.current) {
      // Wait for every finger to lift before a new gesture can start.
      if (pointersRef.current.size === 0) pinchRef.current = null;
      return;
    }
    const drag = dragRef.current;
    if (!drag) return;
    if (!drag.moved && drag.node?.label === "verse") {
      // Clicking a passage opens it in the reader (Ctrl/Cmd-click: new tab).
      const href = readerHref(drag.node);
      if (event.ctrlKey || event.metaKey) window.open(href, "_blank", "noopener");
      else window.location.assign(href);
    } else if (!drag.moved && drag.node) {
      hoveredRef.current = drag.node;
      runRef.current?.(graphNodeQuery(drag.node), drag.node.label, drag.node.id);
    }
    endDrag(drag);
    dragRef.current = null;
  };

  const onWheel = (event) => {
    event.preventDefault();
    const rect = canvasRef.current.getBoundingClientRect();
    zoomAt(event.clientX - rect.left, event.clientY - rect.top, event.deltaY < 0 ? 1.12 : 1 / 1.12);
    draw();
  };

  const resetView = () => {
    const nodes = nodesRef.current.filter(
      (node) => !nodeHidden(node, hiddenTypesRef.current, hiddenVersionsRef.current),
    );
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
  // Translations present in the graph (from translation nodes or passages),
  // named from a translation node when one is loaded.
  const translations = [...new Set(graph?.nodes?.map((node) => node.version_code).filter(Boolean) || [])]
    .sort()
    .map((code) => {
      const versionNode = graph.nodes.find((node) => node.label === "version" && node.version_code === code);
      return { code, title: versionNode?.attrs?.full_name || code };
    });
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
    <select aria-label="How far to explore" title="Each step follows one connection. More steps show a wider view." value={hops} onChange={(event) => {
      setHops(event.target.value);
      setControlRevision((value) => value + 1);
    }}>
      {Array.from({ length: fullPage ? 20 : 3 }, (_, index) => [
        String(index + 1), `${index + 1} connection step${index ? "s" : ""}`,
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
          aria-label="Starting point"
          value={query}
          onChange={(event) => {
            setQuery(event.target.value);
            setControlRevision((value) => value + 1);
          }}
          placeholder="Start with a name, such as Ruth"
        />}
        {!fullPage && <select aria-label="Kind of starting point" value={label} onChange={(event) => {
          setLabel(event.target.value);
          setControlRevision((value) => value + 1);
        }}>
          {["book", "version", "region", "location", "verse", "figure"].map((item) => (
            <option key={item} value={item}>{nodeTypeName(item)}</option>
          ))}
        </select>}
        {!fullPage && depthControl}
        {!fullPage && <button className="button button-secondary" disabled={loading}>
          {loading ? "Loading…" : "Show connections"}
        </button>}
      </form>
      </div>
      {!fullPage && summary && <p className="result-summary" role="status">{summary}</p>}
      {error && <p className="notice error" role="alert">{error}</p>}
      <div className={fullPage ? `relationship-workspace${centreNode ? " has-details" : ""}` : "graph-workspace"}>
      {fullPage && centreNode && (
        <section className="relationship-detail" aria-label="Details of your selection">
          <strong>{nodeTypeName(centreNode.label)}: {nodeName(centreNode)}</strong>
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
            <h3>Related passages, people and places</h3>
            {graph.truncated && (
              <p className="relationship-connections-note">This view shows up to 200 items. Select an item to explore more of its connections.</p>
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
                {graph.truncated && <p className="relationship-connections-note">This view is full. Select a related passage, person or place to explore further.</p>}
              </div>
            )}
            {graph.branch_limited && !Object.keys(hiddenCentreConnections).length && (
              <p className="relationship-connections-note">More connections are available around other items. Select one to explore further.</p>
            )}
            {connectedGroups.size === 0 && <p>No directly related items in this view.</p>}
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
          {!loading && graph && ` · showing ${graph.nodes.filter((node) => !nodeHidden(node, hiddenTypes, hiddenVersions)).length} of ${graph.nodes.length} items`}
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
                <summary aria-label="More connection view options">
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
                    Show names
                  </label>
                  <fieldset className="relationship-physics">
                    <legend>Layout physics</legend>
                    {PHYSICS_CONTROLS.map(([key, text, min, max, step, help]) => (
                      <label key={key} title={help}>
                        <span>{text}</span>
                        <input type="range" min={min} max={max} step={step} value={physics[key]}
                          onChange={(event) => {
                            const value = Number(event.target.value);
                            setPhysics((current) => ({ ...current, [key]: value }));
                          }} />
                        <output>{physics[key]}</output>
                      </label>
                    ))}
                    <button type="button" onClick={() => setPhysics(PAGE_PHYSICS)}>Reset physics</button>
                  </fieldset>
                  <label className="relationship-names-toggle"
                    title="Most nodes to load. Large numbers can be slow to load and draw. Press Enter to apply.">
                    Max nodes
                    <input type="number" min="1" step="50" inputMode="numeric"
                      className="relationship-node-limit" value={nodeLimitInput}
                      onChange={(event) => setNodeLimitInput(event.target.value)}
                      onBlur={() => setNodeLimitInput(String(nodeLimitRef.current))}
                      onKeyDown={(event) => {
                        if (event.key !== "Enter") return;
                        event.preventDefault();
                        const value = Math.floor(Number(nodeLimitInput));
                        if (!Number.isFinite(value) || value < 1) {
                          setNodeLimitInput(String(nodeLimitRef.current));
                          return;
                        }
                        setNodeLimitInput(String(value));
                        if (value === nodeLimitRef.current) return;
                        nodeLimitRef.current = value;
                        if (centreNode) runGraph(graphNodeQuery(centreNode), centreNode.label, centreNode.id);
                      }} />
                  </label>
                  <label className="relationship-node-picker">
                    Explore a related item
                    <select
                      aria-label="Choose a related item"
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
                      <option value="">Choose an item</option>
                      {graph.nodes.map((node) => (
                        <option key={node.id} value={node.id}>
                          {nodeTypeName(node.label)}: {nodeName(node)}{node.chapter ? ` ${node.chapter}:${node.verse_number} (${node.version_code})` : ""}
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
          title="Toggle full-screen view"
          aria-label="Toggle full-screen connections view"
          onClick={toggleFullscreen}
        >⛶</button>
        {fullPage && (
          <button className="relationship-refit" type="button" onClick={resetView}>Show whole view</button>
        )}
        <canvas
          ref={canvasRef}
          className="graph-canvas"
          aria-label="Connections view. Select an item to explore its connections or a passage to read it, drag to move around, or scroll to zoom."
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
        {fullPage && legend.length > 0 && (
          <button type="button" className="graph-legend-toggle" aria-expanded={legendOpen}
            aria-controls="graph-legend" onClick={() => setLegendOpen((open) => !open)}>
            {legendOpen ? "Hide filters" : "Filters"}
          </button>
        )}
        {legend.length > 0 && (
          <div id="graph-legend" className={`graph-legend${legendOpen ? "" : " is-collapsed"}`}
            role={fullPage ? "group" : undefined} aria-label="Show or hide kinds of items">
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
            ) : <span key={item}><i className={`legend-dot badge-${item}`} />{nodeTypeNames[item] || item}</span>)}
            {hasKinship && <span><i className="legend-dash" />Family relationships</span>}
            {fullPage && translations.length > 1 && (
              <div className="graph-version-toggles" role="group" aria-label="Translations">
                {translations.map(({ code, title }) => (
                  <label key={code} className="graph-type-toggle" title={title}>
                    <input type="checkbox" checked={!hiddenVersions.has(code)}
                      aria-label={`Show ${title} and its passages`}
                      onChange={(event) => {
                        const checked = event.target.checked;
                        setHiddenVersions((current) => {
                          const next = new Set(current);
                          if (checked) next.delete(code);
                          else next.add(code);
                          return next;
                        });
                      }} />
                    {code}
                  </label>
                ))}
              </div>
            )}
          </div>
        )}
        {!graph && !loading && !error && <div className="graph-placeholder">Choose a starting point to see its connections</div>}
      </div>
      </div>
      {!fullPage && <p className="graph-tip">Select an item to explore its connections · select a passage to read it · double-click to show the whole view</p>}
    </div>
  );
}

export default GraphExplorer;
