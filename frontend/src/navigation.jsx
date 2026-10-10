import { NodeIcon } from "./nodeIcons.jsx";

export const navigationItems = [
  { href: "/", label: "Home", icon: "home" },
  { href: "/read", label: "Read", icon: "book" },
  { href: "/map", label: "Map", icon: "region" },
  { href: "/relationships", label: "Relationships", icon: "relationships" },
  { href: "/explore", label: "Explore", icon: "search" },
  { href: "/connect-mcp", label: "MCP", icon: "plug" },
];

export function NavigationIcon({ type }) {
  const paths = {
    home: "M3 10 12 3l9 7M5 9v12h5v-7h4v7h5V9",
    relationships: "M8 7l8 3M8 9l8 7M18 12v3M8 7a3 3 0 1 1-6 0 3 3 0 0 1 6 0Zm13 3a3 3 0 1 1-6 0 3 3 0 0 1 6 0Zm0 8a3 3 0 1 1-6 0 3 3 0 0 1 6 0",
    search: "M16 16l5 5M18 10a8 8 0 1 1-16 0 8 8 0 0 1 16 0",
    plug: "M9 3v5M15 3v5M6 8h12v3a6 6 0 0 1-12 0V8Zm6 9v4",
  };
  if (!paths[type]) return <NodeIcon type={type} />;
  return (
    <svg width="18" height="18" viewBox="0 0 24 24" aria-hidden="true" focusable="false">
      <path d={paths[type]} fill="none" stroke="currentColor" strokeWidth="1.6" strokeLinecap="round" strokeLinejoin="round" />
    </svg>
  );
}
