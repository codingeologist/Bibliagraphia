export const nodeIconPaths = {
  book: "M12 5C9 3 5 3 2 4v15c3-1 7-1 10 1 3-2 7-2 10-1V4c-3-1-7-1-10 1Zm0 0v15",
  verse: "M5 3h10l4 4v14H5V3Zm10 0v5h4M8 12h8M8 16h8",
  version: "M3 5h12M9 3v2M6 5c0 5 3 8 7 10M12 5c0 5-3 8-7 10M14 21l4-10 4 10M16 17h4",
  location: "M12 22s8-8 8-13a8 8 0 0 0-16 0c0 5 8 13 8 13ZM15 9a3 3 0 1 1-6 0 3 3 0 0 1 6 0",
  region: "M2 5l7-3 6 3 7-3v17l-7 3-6-3-7 3V5Zm7-3v17M15 5v17",
  figure: "M12 11a4 4 0 1 0 0-8 4 4 0 0 0 0 8ZM4 21c0-4 4-7 8-7s8 3 8 7",
};

export function NodeIcon({ type }) {
  return (
    <svg width="18" height="18" viewBox="0 0 24 24" aria-hidden="true">
      <path d={nodeIconPaths[type] || nodeIconPaths.verse} fill="none" stroke="currentColor" strokeWidth="1.6" strokeLinecap="round" strokeLinejoin="round" />
    </svg>
  );
}
