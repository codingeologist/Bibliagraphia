import { useEffect, useState } from "react";
import GraphExplorer from "./GraphExplorer.jsx";
import SiteHeader from "./SiteHeader.jsx";

function RelationshipsPage() {
  const [dark, setDark] = useState(() => {
    const saved = localStorage.getItem("theme");
    return saved ? saved === "dark" : window.matchMedia("(prefers-color-scheme: dark)").matches;
  });
  const [seed] = useState(() => {
    const params = new URLSearchParams(window.location.search);
    return {
      node: params.get("graph_node") || "GEN",
      label: params.get("graph_label") || "book",
      id: params.get("graph_node_id") || "",
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
