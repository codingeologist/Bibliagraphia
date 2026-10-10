import { useEffect, useMemo, useRef, useState } from "react";
import L from "leaflet";
import { get } from "./api.js";

function PlaceMap({ location }) {
  const mapElementRef = useRef(null);

  useEffect(() => {
    const latitude = Number(location.attrs?.latitude);
    const longitude = Number(location.attrs?.longitude);
    if (!Number.isFinite(latitude) || !Number.isFinite(longitude)) return undefined;

    const map = L.map(mapElementRef.current, {
      zoomControl: true,
      scrollWheelZoom: false,
      dragging: true,
    }).setView([latitude, longitude], 8);
    L.tileLayer(
      "https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}",
      {
        attribution: "Esri, Maxar, Earthstar Geographics",
        maxZoom: 19,
      },
    ).addTo(map);
    L.circleMarker([latitude, longitude], {
      radius: 8,
      weight: 2,
      color: "#fff",
      fillColor: getComputedStyle(document.documentElement)
        .getPropertyValue("--node-location").trim(),
      fillOpacity: 1,
    }).addTo(map);
    const frame = window.requestAnimationFrame(() => map.invalidateSize());

    return () => {
      window.cancelAnimationFrame(frame);
      map.remove();
    };
  }, [location]);

  if (!Number.isFinite(Number(location.attrs?.latitude))
    || !Number.isFinite(Number(location.attrs?.longitude))) {
    return <p className="empty-result">Map coordinates are not available for this place.</p>;
  }

  return (
    <div
      className="place-map-canvas"
      ref={mapElementRef}
      role="img"
      aria-label={`Map showing ${location.name.replace(/\s+\d+$/, "")}`}
    />
  );
}

function placeSegments(text, locations) {
  const candidates = new Map();
  for (const location of locations) {
    const aliases = new Set(location.aliases || [location.name]);
    for (const alias of aliases) {
      const phrase = alias.replace(/\s+\d+$/, "").trim();
      if (!phrase) continue;
      const key = phrase.toLocaleLowerCase();
      const matches = candidates.get(key) || { phrase, locations: [] };
      if (!matches.locations.some((item) => item.id === location.id)) {
        matches.locations.push(location);
      }
      candidates.set(key, matches);
    }
  }
  if (!candidates.size) return [{ text }];

  const phrases = [...candidates.values()].sort((left, right) => right.phrase.length - left.phrase.length);
  const pattern = phrases.map(({ phrase }) => phrase.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")).join("|");
  const matcher = new RegExp(`(?<![\\p{L}\\p{N}])(${pattern})(?![\\p{L}\\p{N}])`, "giu");
  const segments = [];
  let cursor = 0;
  for (const match of text.matchAll(matcher)) {
    if (match.index > cursor) segments.push({ text: text.slice(cursor, match.index) });
    const place = candidates.get(match[1].toLocaleLowerCase());
    segments.push({
      text: match[0],
      locations: place.locations,
    });
    cursor = match.index + match[0].length;
  }
  if (!cursor) return [{ text }];
  if (cursor < text.length) segments.push({ text: text.slice(cursor) });
  return segments;
}

function ReaderPage() {
  const [dark, setDark] = useState(() => {
    const saved = localStorage.getItem("theme");
    return saved ? saved === "dark" : window.matchMedia("(prefers-color-scheme: dark)").matches;
  });
  const [catalog, setCatalog] = useState(null);
  const [version, setVersion] = useState("KJV");
  const [book, setBook] = useState("");
  const [chapter, setChapter] = useState("");
  const [selectedVerse, setSelectedVerse] = useState("");
  const [selectedLocation, setSelectedLocation] = useState(null);
  const [placeGraph, setPlaceGraph] = useState(null);
  const [placeGraphError, setPlaceGraphError] = useState("");
  const [placeGraphLoading, setPlaceGraphLoading] = useState(false);
  const closeDrawerRef = useRef(null);
  const placeDrawerRef = useRef(null);
  const [chapterData, setChapterData] = useState(null);
  const [catalogError, setCatalogError] = useState("");
  const [chapterError, setChapterError] = useState("");
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    document.documentElement.classList.toggle("dark", dark);
    localStorage.setItem("theme", dark ? "dark" : "light");
  }, [dark]);

  useEffect(() => {
    let active = true;
    setCatalogError("");
    get(`/reader/catalog?version_code=${encodeURIComponent(version)}`)
      .then((result) => {
        if (!active) return;
        setCatalog(result);
        const chaptersByBook = result.chapters || {};
        const availableBooks = result.books.filter((item) => chaptersByBook[item.code]?.length);
        const nextBook = availableBooks.some((item) => item.code === book)
          ? book
          : availableBooks[0]?.code || "";
        const nextChapters = chaptersByBook[nextBook] || [];
        setBook(nextBook);
        setChapter((current) => nextChapters.includes(Number(current))
          ? String(current)
          : String(nextChapters[0] || ""));
        if (!nextBook) setLoading(false);
      })
      .catch((error) => active && setCatalogError(error.message));
    return () => { active = false; };
  }, [version]);

  useEffect(() => {
    if (!book || !chapter) return undefined;
    let active = true;
    setLoading(true);
    setChapterError("");
    setChapterData(null);
    setSelectedVerse("");
    setSelectedLocation(null);
    const params = new URLSearchParams({
      book_code: book,
      chapter,
      version_code: version,
    });
    get(`/chapter?${params}`)
      .then((result) => {
        if (!active) return;
        setChapterData(result);
        setLoading(false);
      })
      .catch((error) => {
        if (!active) return;
        setChapterError(error.message);
        setLoading(false);
      });
    return () => { active = false; };
  }, [book, chapter, version]);

  useEffect(() => {
    if (!selectedLocation) return undefined;
    let active = true;
    const previouslyFocused = document.activeElement;
    setPlaceGraph(null);
    setPlaceGraphError("");
    setPlaceGraphLoading(true);
    get(`/graph?${new URLSearchParams({
      node: selectedLocation.name,
      label: "location",
      hops: "1",
      node_id: selectedLocation.id,
    })}`)
      .then((result) => {
        if (!active) return;
        setPlaceGraph(result);
        setPlaceGraphError(result.error || "");
      })
      .catch((error) => active && setPlaceGraphError(error.message))
      .finally(() => active && setPlaceGraphLoading(false));

    const handleKeyDown = (event) => {
      if (event.key === "Escape") setSelectedLocation(null);
      if (event.key === "Tab") {
        const focusable = placeDrawerRef.current?.querySelectorAll(
          "a[href], button:not(:disabled)",
        );
        if (!focusable?.length) return;
        const first = focusable[0];
        const last = focusable[focusable.length - 1];
        if (event.shiftKey && document.activeElement === first) {
          event.preventDefault();
          last.focus();
        } else if (!event.shiftKey && document.activeElement === last) {
          event.preventDefault();
          first.focus();
        }
      }
    };
    document.addEventListener("keydown", handleKeyDown);
    closeDrawerRef.current?.focus();
    return () => {
      active = false;
      document.removeEventListener("keydown", handleKeyDown);
      if (previouslyFocused instanceof HTMLElement) previouslyFocused.focus();
    };
  }, [selectedLocation]);

  const availableBooks = useMemo(
    () => catalog?.books.filter((item) => catalog.chapters[item.code]?.length) || [],
    [catalog],
  );
  const availableChapters = catalog?.chapters[book] || [];
  const currentIndex = availableBooks.findIndex((item) => item.code === book);
  const chapterIndex = availableChapters.indexOf(Number(chapter));
  const atStart = currentIndex === 0 && chapterIndex <= 0;
  const atEnd = currentIndex === availableBooks.length - 1
    && chapterIndex === availableChapters.length - 1;

  const moveChapter = (direction) => {
    if (direction < 0 && chapterIndex > 0) {
      setChapter(String(availableChapters[chapterIndex - 1]));
      return;
    }
    if (direction > 0 && chapterIndex < availableChapters.length - 1) {
      setChapter(String(availableChapters[chapterIndex + 1]));
      return;
    }
    const nextBook = availableBooks[currentIndex + direction];
    if (!nextBook) return;
    const nextChapters = catalog.chapters[nextBook.code];
    setBook(nextBook.code);
    setChapter(String(direction < 0 ? nextChapters[nextChapters.length - 1] : nextChapters[0]));
  };

  const jumpToVerse = (value) => {
    setSelectedVerse(value);
    if (value) {
      document.getElementById(`reader-verse-${value}`)?.scrollIntoView({
        behavior: "smooth",
        block: "center",
      });
    }
  };

  const getMapHref = (location) => {
    const params = new URLSearchParams();
    if (location.region) params.set("map_region", location.region);
    else params.set("map_book", book);
    return `/?${params}#map-explorer`;
  };

  const testamentBooks = (testament) => availableBooks.filter((item) => item.testament === testament);

  return (
    <div className="app-shell">
      <header className="topbar">
        <div className="brand">
          <img className="brand-mark" src="/favicon.svg" alt="" aria-hidden="true" />
          <div>
            <h1>Bibliagraphia</h1>
            <p>A living map of scripture</p>
          </div>
        </div>
        <div className="header-actions">
          <nav className="site-nav" aria-label="Main navigation">
            <a href="/">Explore</a>
            <a href="/read" aria-current="page">Read Bible</a>
          </nav>
          <button className="theme-toggle" type="button" onClick={() => setDark((value) => !value)}>
            {dark ? "☀️" : "🌙"} <span>{dark ? "Light" : "Dark"}</span>
          </button>
        </div>
      </header>

      <main>
        <section className="reader-heading">
          <div>
            <p className="eyebrow">Scripture reader</p>
            <h2>Read the Bible</h2>
            <p>Choose a passage. Underlined place names open their graph and map links.</p>
          </div>
        </section>

        <section className="panel reader-panel" aria-label="Bible reader">
          {catalogError && <p className="notice error" role="alert">{catalogError}</p>}
          {catalog && (
            <>
              {!availableBooks.length && (
                <p className="empty-result">No Bible text is available for this translation.</p>
              )}
              <div className="reader-controls">
                <label className="reader-control">
                  Book
                  <select
                    aria-label="Book"
                    value={book}
                    onChange={(event) => {
                      const nextBook = event.target.value;
                      setBook(nextBook);
                      setChapter(String(catalog.chapters[nextBook]?.[0] || ""));
                    }}
                  >
                    {["Old Testament", "New Testament"].map((testament) => (
                      <optgroup key={testament} label={testament}>
                        {testamentBooks(testament).map((item) => (
                          <option key={item.code} value={item.code}>{item.name}</option>
                        ))}
                      </optgroup>
                    ))}
                  </select>
                </label>
                <label className="reader-control">
                  Chapter
                  <select
                    aria-label="Chapter"
                    value={chapter}
                    onChange={(event) => setChapter(event.target.value)}
                  >
                    {availableChapters.map((item) => (
                      <option key={item} value={item}>{item}</option>
                    ))}
                  </select>
                </label>
                <label className="reader-control">
                  Translation
                  <select
                    aria-label="Translation"
                    value={version}
                    onChange={(event) => setVersion(event.target.value)}
                  >
                    {catalog.versions.map((item) => (
                      <option key={item.code} value={item.code}>{item.full_name || item.name}</option>
                    ))}
                  </select>
                </label>
                <label className="reader-control">
                  Passage
                  <select
                    aria-label="Jump to verse"
                    value={selectedVerse}
                    onChange={(event) => jumpToVerse(event.target.value)}
                    disabled={!chapterData?.verses.length}
                  >
                    <option value="">Jump to verse</option>
                    {chapterData?.verses.map((item) => (
                      <option key={item.number} value={String(item.number)}>Verse {item.number}</option>
                    ))}
                  </select>
                </label>
              </div>

              <div className="reader-nav">
                <button type="button" onClick={() => moveChapter(-1)} disabled={!availableBooks.length || atStart}>
                  ← Previous
                </button>
                <span>
                  {chapterData
                    ? `${chapterData.book_name} ${chapterData.chapter} · ${version}`
                    : "Select a book and chapter"}
                </span>
                <button type="button" onClick={() => moveChapter(1)} disabled={!availableBooks.length || atEnd}>
                  Next →
                </button>
              </div>

              {loading && <div className="reader-loading" role="status">Loading passage…</div>}
              {chapterError && <p className="notice error" role="alert">{chapterError}</p>}
              {!loading && chapterData && !chapterData.verses.length && (
                <p className="empty-result">No text available for this passage in this translation.</p>
              )}
              {chapterData?.verses.length > 0 && !loading && (
                <article
                  className="reader-text"
                  aria-label={`${chapterData.book_name} chapter ${chapterData.chapter}`}
                  onClick={(event) => {
                    if (event.target.closest(".place-highlight")) return;
                    setSelectedLocation(null);
                  }}
                >
                  {selectedLocation && (
                    <>
                      <button
                        className="place-drawer-backdrop"
                        type="button"
                        aria-label="Close place relations"
                        onClick={() => setSelectedLocation(null)}
                      />
                      <aside
                        ref={placeDrawerRef}
                        className="place-drawer"
                        role="dialog"
                        aria-modal="true"
                        aria-labelledby="place-drawer-title"
                      >
                        <header className="place-drawer-header">
                          <div>
                            <p className="eyebrow">Graph location</p>
                            <h3 id="place-drawer-title">{selectedLocation.name.replace(/\s+\d+$/, "")}</h3>
                            {selectedLocation.region && <p>{selectedLocation.region}</p>}
                          </div>
                          <button
                            ref={closeDrawerRef}
                            className="place-details-close"
                            type="button"
                            aria-label="Close place relations"
                            onClick={() => setSelectedLocation(null)}
                          >×</button>
                        </header>
                        <div className="place-drawer-content">
                          <h4>Place on the map</h4>
                          {!placeGraphLoading && !placeGraphError && placeGraph && (
                            <PlaceMap
                              key={selectedLocation.id}
                              location={placeGraph.nodes.find((node) => node.id === selectedLocation.id)
                                || selectedLocation}
                            />
                          )}
                          <h4>Connected nodes</h4>
                          {placeGraphLoading && <p className="reader-loading" role="status">Loading graph relations…</p>}
                          {placeGraphError && <p className="notice error" role="alert">{placeGraphError}</p>}
                          {!placeGraphLoading && !placeGraphError && placeGraph?.links.length === 0 && (
                            <p className="empty-result">No connected graph nodes found.</p>
                          )}
                          {!placeGraphLoading && placeGraph?.links.length > 0 && (
                            <ul className="place-relations">
                              {placeGraph.links.map((link) => {
                                const relatedId = link.source === selectedLocation.id ? link.target : link.source;
                                const node = placeGraph.nodes.find((item) => item.id === relatedId);
                                if (!node) return null;
                                const relation = link.label === "location_in_region"
                                  ? "Located in"
                                  : link.label === "location_in_verse" ? "Mentioned in" : link.label;
                                const details = node.label === "verse"
                                  ? `${node.name || node.book_code} ${node.chapter}:${node.verse_number}`
                                    + (node.version_code ? ` · ${node.version_code}` : "")
                                  : node.name || node.id;
                                return (
                                  <li key={`${link.label}-${node.id}`}>
                                    <span>{relation}</span>
                                    <strong>{details}</strong>
                                    <small>{link.label}</small>
                                  </li>
                                );
                              })}
                            </ul>
                          )}
                          <a
                            className="place-map-link"
                            href={getMapHref(selectedLocation)}
                            target="_blank"
                            rel="noreferrer"
                          >View this place on the map ↗</a>
                        </div>
                      </aside>
                    </>
                  )}
                  {chapterData.verses.map((item) => (
                    <VerseText
                      key={item.number}
                      verse={item}
                      selected={selectedVerse === String(item.number)}
                      locations={chapterData.locations.filter((location) => location.verse_number === item.number)}
                      onSelectLocation={setSelectedLocation}
                    />
                  ))}
                </article>
              )}
            </>
          )}
        </section>
      </main>
    </div>
  );
}

function VerseText({ verse, locations, selected, onSelectLocation }) {
  const segments = placeSegments(verse.text, locations);
  return (
    <p
      className={`reader-verse${selected ? " selected" : ""}`}
      id={`reader-verse-${verse.number}`}
    >
      <sup>{verse.number}</sup>
      {segments.map((segment, index) => segment.locations
        ? (
          <button
            className="place-highlight"
            type="button"
            key={`${segment.text}-${index}`}
            title={`Graph location: ${segment.locations.map((item) => item.region ? `${item.name}, ${item.region}` : item.name).join("; ")}`}
            onClick={() => onSelectLocation(segment.locations[0])}
          >{segment.text}</button>
        )
        : <span key={`${segment.text}-${index}`}>{segment.text}</span>)}
    </p>
  );
}

export default ReaderPage;
