const storageKey = "relationships-state";
const types = ["book", "verse", "location", "version", "region", "figure"];

export const validGraphDepth = (value) => {
  const depth = Number(value);
  return Number.isInteger(depth) && depth >= 1 && depth <= 20;
};

export function loadRelationshipState() {
  try {
    const raw = localStorage.getItem(storageKey);
    if (!raw) return null;
    const state = JSON.parse(raw);
    if (!state || typeof state.node !== "string" || !state.node.trim()
      || !types.includes(state.label) || typeof state.id !== "string"
      || !validGraphDepth(state.hops)) {
      console.warn("Saved Relationships selection is invalid; using defaults.");
      return null;
    }
    return state;
  } catch (error) {
    console.warn("Could not restore Relationships selection.", error);
    return null;
  }
}

export function saveRelationshipState(state) {
  try {
    localStorage.setItem(storageKey, JSON.stringify(state));
  } catch (error) {
    console.warn("Could not save Relationships selection.", error);
  }
}
