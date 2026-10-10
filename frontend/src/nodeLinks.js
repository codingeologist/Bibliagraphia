export const describeNode = (node) => node.label === "verse"
  ? `${node.name || node.book_code} ${node.chapter}:${node.verse_number} · ${node.version_code}`
  : node.label === "location" && node.chapter
    ? `${node.name} · ${node.book_code} ${node.chapter}:${node.verse_number}`
    : node.name || node.id;

export const readerPassageNode = (chapter, verse) => ({
  id: `verse:${chapter.version}:${chapter.book_code}:${chapter.chapter}:${verse.number}`,
  label: "verse", name: chapter.book_name, book_code: chapter.book_code,
  chapter: chapter.chapter, verse_number: verse.number, version_code: chapter.version,
  text: verse.text,
});

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
  book: "Book", verse: "Passage", location: "Place", region: "Region", version: "Translation", figure: "Person",
})[label] || label;

const relationshipNames = {
  verse_in_book: ["Contains passage", "Belongs to book"],
  verse_in_version: ["Contains passage", "Available in translation"],
  location_in_verse: ["Mentions place", "Mentioned in passage"],
  location_in_region: ["Contains place", "Located in region"],
  figure_in_verse: ["Mentions person", "Mentioned in passage"],
  figure_relative_of: ["Relative of", "Relative of"],
};

// Kinship edges (figure_relative_of) name their kind in attrs.relationship.
// father/mother/parent edges are stored parent -> child; sibling and
// partner edges are symmetric, stored once per pair, so both directions
// read the same.
const kinshipNames = {
  father: ["Father of", "Child of"],
  mother: ["Mother of", "Child of"],
  parent: ["Parent of", "Child of"],
  sibling: ["Sibling of", "Sibling of"],
  partner: ["Partner of", "Partner of"],
};

export const relationshipDescription = (label, outgoing, attrs) => {
  const names = label === "figure_relative_of" && attrs?.relationship
    ? kinshipNames[attrs.relationship] || relationshipNames[label]
    : relationshipNames[label];
  return names?.[outgoing ? 0 : 1] || label.replaceAll("_", " ");
};

// Path API edges follow the walk, which can run against the stored
// direction. Kinship kinds read off the stored edge: symmetric kinds read
// the same from either end, and parent kinds run parent -> child, so the
// stored source is always the parent end.
export const pathRelationshipDescription = (label, fromType, edge, fromId) => {
  if (label === "figure_relative_of") {
    const outgoing = edge && fromId ? edge.source === fromId : true;
    return relationshipDescription(label, outgoing, edge?.attrs);
  }
  return relationshipDescription(label, ({
    verse_in_book: "book",
    verse_in_version: "version",
    location_in_verse: "verse",
    location_in_region: "region",
    figure_in_verse: "verse",
  })[label] === fromType);
};
