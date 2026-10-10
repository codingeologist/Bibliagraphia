export function mentionSegments(text, locations = [], figures = []) {
  const candidates = new Map();
  for (const [kind, items] of [["locations", locations], ["figures", figures]]) {
    for (const item of items) {
      for (const alias of new Set([item.name, ...(item.aliases || [])])) {
        const phrase = (kind === "locations" ? alias.replace(/\s+\d+$/, "") : alias).trim();
        if (!phrase) continue;
        const key = phrase.toLocaleLowerCase();
        const candidate = candidates.get(key) || { phrase, kind, items: [] };
        // Preserve place actions when a name can describe both a person and a place.
        if (candidate.kind !== kind) continue;
        if (!candidate.items.some((match) => match.id === item.id)) candidate.items.push(item);
        candidates.set(key, candidate);
      }
    }
  }
  if (!candidates.size) return [{ text }];
  const phrases = [...candidates.values()].sort((a, b) => b.phrase.length - a.phrase.length);
  const pattern = phrases.map(({ phrase }) => phrase.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")).join("|");
  const matcher = new RegExp(`(?<![\\p{L}\\p{N}])(${pattern})(?![\\p{L}\\p{N}])`, "giu");
  const segments = [];
  let cursor = 0;
  for (const match of text.matchAll(matcher)) {
    if (match.index > cursor) segments.push({ text: text.slice(cursor, match.index) });
    const candidate = candidates.get(match[0].toLocaleLowerCase());
    segments.push({ text: match[0], [candidate.kind]: candidate.items });
    cursor = match.index + match[0].length;
  }
  if (!cursor) return [{ text }];
  if (cursor < text.length) segments.push({ text: text.slice(cursor) });
  return segments;
}
