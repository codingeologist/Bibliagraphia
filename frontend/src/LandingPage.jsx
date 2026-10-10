import { useEffect, useState } from "react";
import SiteHeader from "./SiteHeader.jsx";
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

  if (kind === "exploring") {
    return (
      <span className="landing-preview exploring-preview" aria-hidden="true">
        <svg viewBox="0 0 180 108" role="presentation">
          <g className="telescope-icon">
            <path className="telescope-tube" d="m48 43 53-28 18 32-53 28z" />
            <path d="m101 15 11-6 20 35-13 3M48 43l-9-4-5 9 12 6m49-39-5-9m24 41 8 7" />
            <path className="telescope-finder" d="m73 30 23-13 5 9-23 13z" />
            <path d="m84 62 7 11m-7-11-31 29m31-29 33 28m-33-28 3 35m-40 0h78" />
            <circle cx="84" cy="62" r="4" />
            <circle cx="128" cy="43" r="9" />
          </g>
        </svg>
      </span>
    );
  }

  return (
    <span className="landing-preview relationships-preview" aria-hidden="true">
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

function LandingAmbience() {
  return (
    <div className="landing-ambience" aria-hidden="true">
      <svg className="landing-ambience-artwork ambience-reading" viewBox="0 0 1200 800">
        <g className="ambience-scripture">
          <text x="110" y="270" className="ambience-scripture-reference">MATTHEW · CHAPTER 7</text>
          <text x="110" y="322" className="ambience-verse-number ambience-verse-highlight">8</text>
          <text x="140" y="322" className="ambience-scripture-line ambience-scripture-highlight">For every one that asketh receiveth;</text>
          <text x="140" y="352" className="ambience-scripture-line ambience-scripture-highlight">and he that seeketh findeth; and to</text>
          <text x="140" y="382" className="ambience-scripture-line ambience-scripture-highlight">him that knocketh it shall be opened.</text>
          <text x="110" y="452" className="ambience-verse-number">9</text>
          <text x="140" y="452" className="ambience-scripture-line">Or what man is there of you, whom if</text>
          <text x="140" y="482" className="ambience-scripture-line">his son ask bread, will he give him a</text>
          <text x="140" y="512" className="ambience-scripture-line">stone?</text>

          <text x="660" y="270" className="ambience-scripture-reference">MATTHEW · CHAPTER 7</text>
          <text x="660" y="322" className="ambience-verse-number">10</text>
          <text x="694" y="322" className="ambience-scripture-line">Or if he ask a fish, will he give him</text>
          <text x="694" y="352" className="ambience-scripture-line">a serpent?</text>
          <text x="660" y="422" className="ambience-verse-number">11</text>
          <text x="694" y="422" className="ambience-scripture-line">If ye then, being evil, know how to</text>
          <text x="694" y="452" className="ambience-scripture-line">give good gifts unto your children,</text>
          <text x="694" y="482" className="ambience-scripture-line">how much more shall your Father</text>
          <text x="694" y="512" className="ambience-scripture-line">which is in heaven give good things</text>
          <text x="694" y="542" className="ambience-scripture-line">to them that ask him?</text>
        </g>
      </svg>
      <svg className="landing-ambience-artwork ambience-mapping" viewBox="0 0 1200 800">
        <g className="ambience-map">
          <g className="ambience-world-grid">
            <path d="M170 400h860M215 330h770M215 470h770" />
            <path d="M600 210c-130 90-130 300 0 390M600 210c130 90 130 300 0 390M600 210v390" />
          </g>
          <g className="ambience-world-land">
            <path d="M166 280c2-17 21-24 35-42 17-11 42-4 59-12 16-7 18-24 34-27 19-4 31 15 46 10 20-6 24 20 40 26 18 7 37-1 50 13 11 13 3 23-11 29-14 7-13 17-1 25 14 10 15 23 2 32-12 9-29 5-40 17-14 13-12 29-27 41-16 12-16 34-31 46-16 12-19 34-29 47-10 15-26 22-37 17-12-7-7-30-15-44-9-16-27-26-28-46-2-17-19-25-19-43 0-17 11-26 1-39-9-12-19-20-8-34 8-10 7-19-1-26z" />
            <path d="M348 482c10-8 22 7 33 15 13 9 12 21 17 34 6 17-3 25-8 34-8 15-15 24-24 35-11 14-7 35-11 52-4 16-10 30-21 36-9-12-12-25-15-39-4-14-1-28-5-43-5-16-20-27-20-42 1-17 13-26 9-39-4-15 12-22 24-23z" />
            <path d="M516 278c5-10 14-14 23-20 11-6 20 2 32 4 9 2 9-13 20-14 12 0 18 12 15 22-2 9-10 11-18 11-9 1-11 9-21 10-11 1-18-6-26-5-10 1-15 11-25 12-8-4-8-12 0-20z" />
            <path d="M563 310c12-9 25-17 39-21 15-4 25 5 38 9 16 5 18 17 20 25 4 14 20 12 34 12 12 0 14 15 3 24-9 8-21 8-27 13-10 9-9 23-14 32-6 11-18 16-25 25-11 14-8 28-12 41-4 15-13 21-25 24-11 2-18-5-22-10-8-12 5-25 8-39 3-15-12-17-19-28-8-13-1-22-2-32-1-12-18-14-23-25-5-10 7-20 12-29 7-11-2-15-10-21-8-7-4-17 5-20z" />
            <path d="M638 273c9-10 15-20 24-28 12-11 34-7 53-10 18-3 22 8 36 14 15 6 27-8 43-9 18-1 35 12 55 19 15 5 28-4 44-2 18 2 31 19 51 29 15 8 29-6 42-5 19 1 26 15 40 24 11 7 2 20-9 27-12 7-26-2-42 2-17 3-21 17-27 22-13 11-25-2-40-7-15-5-16 17-21 27-5 9-22 3-39-10-16-11-20 8-31 20-11 12-21 0-31-16-10-15-23 13-32 19-14 8-21-11-31-20-11-10-17 6-28 11-13 5-22-13-34-24-10-10-23 15-32 9-12-8-8-18-22-22-13-4-26 7-34 2-13-7-15-16-23-23-10-10 8-15 15-20 8-6-7-9-23-14-12-4-8-15 0-24z" />
            <path d="M907 488c12-10 24-16 35-19 16-5 29 8 45 10 17 2 19 17 25 29 7 13-8 23-17 35-10 12-25 8-40 8-14 0-21-14-32-21-12-8-14-20-16-30-2-6-6-8 0-12z" />
            <path d="M1027 549c4 0 9 4 13 7 5 4 0 10-4 15-4 4-9 0-12-5-3-5-1-12 3-17z" />
            <path d="m460 247 8-18 16 2 5 17-13 11zM576 237l7-19 15 4 4 17-13 9z" />
          </g>
          <path className="ambience-route" d="M316 346c117-72 173 53 280 4s199 17 295-44" />
          <circle cx="316" cy="346" r="7" />
          <circle cx="596" cy="350" r="7" />
          <circle cx="891" cy="306" r="7" />
        </g>
      </svg>
      <svg className="landing-ambience-artwork ambience-exploring" viewBox="0 0 1200 800">
        <g className="ambience-graph">
          <path className="ambience-synapses" d="m150 354 174-122 176 62 180-113 184 67 183-113M150 354l191 148 159-208 180 178 184-224 183 108M324 232l17 270 159-208 184-113 180 291M500 294l174 176 180-291M680 181l4 291 180-224M324 232l350 239 363-131M150 354l350-60 364 228 363-131" />
          <circle className="ambience-node" cx="150" cy="354" r="13" />
          <circle className="ambience-node" cx="324" cy="232" r="11" />
          <circle className="ambience-node" cx="341" cy="502" r="11" />
          <circle className="ambience-node" cx="500" cy="294" r="14" />
          <circle className="ambience-node ambience-node-core" cx="500" cy="294" r="5" />
          <circle className="ambience-node" cx="674" cy="181" r="12" />
          <circle className="ambience-node" cx="680" cy="472" r="12" />
          <circle className="ambience-node" cx="854" cy="248" r="14" />
          <circle className="ambience-node ambience-node-core" cx="854" cy="248" r="5" />
          <circle className="ambience-node" cx="854" cy="539" r="11" />
          <circle className="ambience-node" cx="1037" cy="135" r="12" />
          <circle className="ambience-node" cx="1037" cy="379" r="14" />
          <circle className="ambience-node ambience-node-core" cx="1037" cy="379" r="5" />
        </g>
      </svg>
      <svg className="landing-ambience-artwork ambience-telescope" viewBox="0 0 1200 800">
        <g className="ambience-telescope-art">
          <circle className="ambience-telescope-orbit" cx="600" cy="400" r="246" />
          <path className="ambience-telescope-star" d="M316 243v26m-13-13h26m499 251v24m-12-12h24m-404-345v18m-9-9h18" />
          <path className="ambience-telescope-body" d="m426 353 246-137 74 129-246 138z" />
          <path className="ambience-telescope-detail" d="m672 216 43-24 83 144-42 24m-256 123 63 10 45 77m-45-77-109 82m109-82 12 153m158-426 60-34 27 47-60 34m-7-13 42-24" />
          <circle className="ambience-telescope-joint" cx="553" cy="483" r="12" />
          <path className="ambience-telescope-detail" d="m553 483 163-1m-163 1-91 98m91-98 31 107m-246 25h476" />
        </g>
      </svg>
    </div>
  );
}

export default function LandingPage() {
  const [dark, setDark] = useState(() => {
    const saved = localStorage.getItem("theme");
    return saved ? saved === "dark" : window.matchMedia("(prefers-color-scheme: dark)").matches;
  });
  const [readingPosition] = useState(loadReaderState);
  const [suggestion] = useState(loadSuggestedBook);
  const suggestedChapter = suggestion.chapter || 1;
  const suggestedReference = `${suggestion.name} ${suggestedChapter}${suggestion.verse ? `:${suggestion.verse}` : ""}`;

  useEffect(() => {
    document.documentElement.classList.toggle("dark", dark);
    localStorage.setItem("theme", dark ? "dark" : "light");
  }, [dark]);

  useEffect(() => {
    saveSuggestedBook(suggestion.book);
  }, [suggestion]);

  const experiences = destinations.map((destination) => destination.kind === "reading" && readingPosition
    ? {
      ...destination,
      href: readerStateHref(readingPosition),
      action: "Continue reading",
      description: `Return to ${readingPosition.bookName} ${readingPosition.chapter} · ${readingPosition.version}.`,
    }
    : destination);

  return (
    <div className="app-shell">
      <SiteHeader currentPage="/" dark={dark} onToggleTheme={() => setDark((value) => !value)} />
      <main className="landing-page">
        <LandingAmbience />
        <div className="landing-glow landing-glow-one" aria-hidden="true" />
        <div className="landing-glow landing-glow-two" aria-hidden="true" />
        <section className="landing-content" aria-labelledby="landing-title">
          <a className="landing-brand-mark" href="/" aria-label="Biblia Graphia home">
            <img src="/favicon.svg" alt="" />
          </a>
          <p className="landing-eyebrow">A living map of scripture</p>
          <h1 id="landing-title">Biblia <em>Graphia</em></h1>
          <p className="landing-subtitle">Immersive Bible reading experience</p>
          <p className="landing-description">
            Discover the Bible through people, pages and passages.
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
            <h2 id="landing-start-title">Try it with {suggestion.chapter ? suggestedReference : suggestion.name}</h2>
            <p>Read a chapter, discover its people and places, and compare translations.</p>
            <a href={readerStateHref({ book: suggestion.book, chapter: suggestedChapter, verse: suggestion.verse, version: "KJV", comparisons: ["DRB"] })}>
              Explore {suggestedReference} <span aria-hidden="true">→</span>
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
    </div>
  );
}
