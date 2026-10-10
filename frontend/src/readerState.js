const storageKey = "reader-state";

export function validReaderState(state) {
  return Boolean(state && typeof state.book === "string" && /^[A-Z0-9]{3}$/.test(state.book)
    && Number.isInteger(state.chapter) && state.chapter > 0
    && typeof state.bookName === "string" && state.bookName.trim()
    && typeof state.version === "string" && state.version.trim()
    && Array.isArray(state.comparisons)
    && state.comparisons.every((code) => typeof code === "string" && code.trim()));
}

export function loadReaderState() {
  try {
    const raw = localStorage.getItem(storageKey);
    if (!raw) return null;
    const state = JSON.parse(raw);
    if (!validReaderState(state)) {
      console.warn("Saved reading position is invalid; using the default starting point.");
      return null;
    }
    return state;
  } catch (error) {
    console.warn("Could not restore reading position.", error);
    return null;
  }
}

export function saveReaderState(state) {
  if (!validReaderState(state)) {
    console.warn("Could not save an invalid reading position.");
    return;
  }
  try {
    localStorage.setItem(storageKey, JSON.stringify(state));
  } catch (error) {
    console.warn("Could not save reading position.", error);
  }
}

export function readerStateHref(state) {
  const params = new URLSearchParams({
    book: state.book, chapter: String(state.chapter), version: state.version,
  });
  state.comparisons.forEach((code) => params.append("compare", code));
  return `/read?${params}`;
}
