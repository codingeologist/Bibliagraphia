export function previewNeighbours(nodes, limit = 6) {
  const byType = new Map();
  const unique = new Map(nodes.map((node) => [node.id, node]));
  for (const node of unique.values()) {
    if (!byType.has(node.label)) byType.set(node.label, node);
  }
  const selected = [...byType.values()];
  const selectedIds = new Set(selected.map((node) => node.id));
  for (const node of unique.values()) {
    if (selected.length >= Math.max(limit, byType.size)) break;
    if (!selectedIds.has(node.id)) {
      selected.push(node);
      selectedIds.add(node.id);
    }
  }
  return selected;
}
