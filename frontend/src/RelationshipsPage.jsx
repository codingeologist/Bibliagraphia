import { useEffect, useState } from "react";
import GraphExplorer from "./GraphExplorer.jsx";
import SiteHeader from "./SiteHeader.jsx";
import { loadRelationshipState, validGraphDepth } from "./relationshipState.js";

function RelationshipsPage() {
  const [dark, setDark] = useState(() => {
    const saved = localStorage.getItem("theme");
    return saved ? saved === "dark" : window.matchMedia("(prefers-color-scheme: dark)").matches;
  });
  const [seed] = useState(() => {
    const params = new URLSearchParams(window.location.search);
    const saved = loadRelationshipState();
    const explicitNode = params.has("graph_node") || params.has("graph_node_id");
    const depth = params.get("graph_hops");
    return {
      node: explicitNode ? params.get("graph_node") || "GEN" : saved?.node || "GEN",
      label: explicitNode ? params.get("graph_label") || "book" : saved?.label || "book",
      id: explicitNode ? params.get("graph_node_id") || "" : saved?.id || "",
      hops: validGraphDepth(depth) ? String(Number(depth)) : String(saved?.hops || 3),
      showNames: saved?.showNames === true,
    };
  });

  useEffect(() => {
    document.documentElement.classList.toggle("dark", dark);
    localStorage.setItem("theme", dark ? "dark" : "light");
  }, [dark]);

  return (
    <div className="app-shell">
      <SiteHeader currentPage="/relationships" dark={dark} onToggleTheme={() => setDark((value) => !value)} />
      <main className="relationships-page">
        <h2 className="visually-hidden">Relationships</h2>
        <GraphExplorer seed={seed} fullPage />
      </main>
    </div>
  );
}

export default RelationshipsPage;
