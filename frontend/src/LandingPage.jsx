import { useEffect, useState } from "react";
import { loadReaderState, readerStateHref } from "./readerState.js";
import { loadSuggestedBook, saveSuggestedBook } from "./landingSuggestion.js";

const destinations = [
  {
    href: "/read",
    title: "Read Bible",
    description: "Read a passage, compare translations, and discover its people and places.",
    kind: "reading",
    action: "Start reading",
  },
  {
    href: "/map",
    title: "Map",
    description: "Follow biblical places across an interactive map and discover their verses.",
    kind: "mapping",
    action: "Explore places",
  },
  {
    href: "/relationships",
    title: "Relationships",
    description: "See how passages, people and places connect. Select anything to explore its connections.",
    kind: "relationships",
    action: "Explore relationships",
  },
  {
    href: "/explore",
    title: "Explore",
    description: "Search people, places, and passages, then trace connections through scripture.",
    kind: "exploring",
    action: "Start exploring",
  },
];

function Preview({ kind }) {
  if (kind === "reading") {
    return (
      <span className="landing-preview reading-preview" aria-hidden="true">
        <span className="preview-book">
          <span className="preview-book-title" />
          <span className="preview-line" />
          <span className="preview-line short" />
          <span className="preview-line" />
          <span className="preview-line medium" />
          <span className="preview-highlight" />
        </span>
      </span>
    );
  }

  if (kind === "mapping") {
    return (
      <span className="landing-preview mapping-preview" aria-hidden="true">
        <svg viewBox="0 0 180 108" role="presentation">
          <path className="map-land" d="M12 23 46 11l24 12 24-8 28 15 29-5 16 19-17 19 7 25-37 8-23-13-25 8-23-18-25 3-13-20z" />
          <path className="map-route" d="M34 67c18-29 38 12 57-15s35 17 56-14" />
          <circle className="map-point point-one" cx="34" cy="67" r="4" />
          <circle className="map-point point-two" cx="91" cy="52" r="4" />
          <circle className="map-point point-three" cx="147" cy="38" r="4" />
        </svg>
      </span>
    );
  }

  return (
    <span className="landing-preview exploring-preview" aria-hidden="true">
      <svg viewBox="0 0 180 108" role="presentation">
        <path className="graph-edge edge-one" d="M38 54 82 27M38 54l52 39M82 27l58 22M90 93l50-44M82 27l8 66" />
        <circle className="graph-node graph-node-one" cx="38" cy="54" r="9" />
        <circle className="graph-node graph-node-two" cx="82" cy="27" r="7" />
        <circle className="graph-node graph-node-three" cx="90" cy="93" r="8" />
        <circle className="graph-node graph-node-four" cx="140" cy="49" r="10" />
      </svg>
    </span>
  );
}

export default function LandingPage() {
  const [readingPosition] = useState(loadReaderState);
  const [suggestion] = useState(loadSuggestedBook);
  useEffect(() => {
    saveSuggestedBook(suggestion.book);
  }, [suggestion]);
  const experiences = destinations.map((destination) => destination.kind === "reading" && readingPosition
    ? { ...destination, href: readerStateHref(readingPosition), action: "Continue reading",
      description: `Return to ${readingPosition.bookName} ${readingPosition.chapter} · ${readingPosition.version}.` }
    : destination);
  return (
    <main className="landing-page">
      <div className="landing-glow landing-glow-one" aria-hidden="true" />
      <div className="landing-glow landing-glow-two" aria-hidden="true" />
      <section className="landing-content" aria-labelledby="landing-title">
        <a className="landing-brand-mark" href="/" aria-label="Biblia Graphia home">
          <img src="/favicon.svg" alt="" />
        </a>
        <p className="landing-eyebrow">A living map of scripture</p>
        <h1 id="landing-title">Biblia <em>Graphia</em></h1>
        <p className="landing-subtitle">Read the Bible. Discover how it connects.</p>
        <p className="landing-description">
          Compare translations and explore the people and places behind each passage.
        </p>

        <nav className="landing-destinations" aria-label="Choose your experience">
          {experiences.map(({ href, title, description, kind, action }, index) => (
            <a
              className={`landing-card landing-card-${kind}`}
              href={href}
              key={kind}
              aria-describedby={`landing-description-${kind}`}
              style={{ "--card-order": index }}
            >
              <Preview kind={kind} />
              <span className="landing-card-title">{title}</span>
              <span className="landing-card-description" id={`landing-description-${kind}`}>
                {description}
              </span>
              <span className="landing-card-action">
                {action}<span aria-hidden="true"> ↗</span>
              </span>
            </a>
          ))}
        </nav>
        <section className="landing-start" aria-labelledby="landing-start-title">
          <h2 id="landing-start-title">Try it with {suggestion.name}</h2>
          <p>Read a chapter, discover its people and places, and compare translations.</p>
          <a href={readerStateHref({ book: suggestion.book, chapter: 1, version: "KJV", comparisons: ["DRB"] })}>
            Explore {suggestion.name} 1 <span aria-hidden="true">→</span>
          </a>
        </section>
        <footer className="landing-footer">
          <nav aria-label="About and resources">
            <a href="/about">About</a>
            <a href="/connect-mcp">Connect your AI assistant</a>
            <a href="https://github.com/codingeologist/Bibliagraphia" target="_blank" rel="noreferrer">GitHub</a>
          </nav>
        </footer>
      </section>
    </main>
  );
}
