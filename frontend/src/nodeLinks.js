export const describeNode = (node) => node.label === "verse"
  ? `${node.name || node.book_code} ${node.chapter}:${node.verse_number} · ${node.version_code}`
  : node.label === "location" && node.chapter
    ? `${node.name} · ${node.book_code} ${node.chapter}:${node.verse_number}`
    : node.name || node.id;

export const relationshipHref = (node) => `/relationships?${new URLSearchParams({
  graph_node: node.name || node.book_code || node.version_code || node.id,
  graph_label: node.label,
  graph_node_id: node.id,
  graph_hops: "3",
})}`;

export const pathNode = (node) => typeof node === "string"
  ? { id: node, name: node, label: node.split(":")[0] }
  : node;

export const nodeTypeName = (label) => ({
  book: "Book", verse: "Passage", location: "Place", region: "Region", version: "Translation",
})[label] || label;

const relationshipNames = {
  verse_in_book: ["Contains passage", "Belongs to book"],
  verse_in_version: ["Contains passage", "Available in translation"],
  location_in_verse: ["Mentions place", "Mentioned in passage"],
  location_in_region: ["Contains place", "Located in region"],
};

export const relationshipDescription = (label, outgoing) =>
  relationshipNames[label]?.[outgoing ? 0 : 1] || label.replaceAll("_", " ");

// Path API edges follow the walk, which can run against the stored direction.
export const pathRelationshipDescription = (label, fromType) => relationshipDescription(label, ({
  verse_in_book: "book",
  verse_in_version: "version",
  location_in_verse: "verse",
  location_in_region: "region",
})[label] === fromType);
