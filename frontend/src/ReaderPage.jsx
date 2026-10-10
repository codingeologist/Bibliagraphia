import { useEffect, useMemo, useRef, useState } from "react";
import L from "leaflet";
import { get } from "./api.js";

function PlaceMap({ location, href, onExpand }) {
  const mapElementRef = useRef(null);
  const mapRef = useRef(null);

  useEffect(() => {
    const latitude = Number(location.attrs?.latitude);
    const longitude = Number(location.attrs?.longitude);
    if (!Number.isFinite(latitude) || !Number.isFinite(longitude)) return undefined;

    const map = L.map(mapElementRef.current, {
      zoomControl: true,
      scrollWheelZoom: false,
      dragging: true,
    }).setView([latitude, longitude], 5);
    mapRef.current = map;
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
      mapRef.current = null;
    };
  }, [location]);

  if (!Number.isFinite(Number(location.attrs?.latitude))
    || !Number.isFinite(Number(location.attrs?.longitude))) {
    return <p className="empty-result">Map coordinates are not available for this place.</p>;
  }

  return (
    <div className="place-map-preview">
      <div
        className="place-map-canvas"
        ref={mapElementRef}
        role="img"
        aria-label={`Map showing ${location.name.replace(/\s+\d+$/, "")}`}
      />
      <a
        className="place-map-preview-link"
        href={href}
        aria-label={`Expand map to ${location.name.replace(/\s+\d+$/, "")}`}
        title="Open on full map"
        onClick={(event) => onExpand?.(event, {
          center: mapRef.current.getCenter(),
          zoom: mapRef.current.getZoom(),
        })}
      >⛶</a>
    </div>
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

function ReaderPage({ onExpandMap }) {
  const initialParams = new URLSearchParams(window.location.search);
  const [dark, setDark] = useState(() => {
    const saved = localStorage.getItem("theme");
    return saved ? saved === "dark" : window.matchMedia("(prefers-color-scheme: dark)").matches;
  });
  const [catalog, setCatalog] = useState(null);
  const [version, setVersion] = useState(initialParams.get("version") || "KJV");
  const [book, setBook] = useState(initialParams.get("book") || "");
  const [chapter, setChapter] = useState(initialParams.get("chapter") || "");
  const [selectedVerse, setSelectedVerse] = useState(initialParams.get("verse") || "");
  const [selectedLocation, setSelectedLocation] = useState(null);
  const [placeRelations, setPlaceRelations] = useState(null);
  const [placeRelationsError, setPlaceRelationsError] = useState("");
  const [placeRelationsLoading, setPlaceRelationsLoading] = useState(false);
  const closeDrawerRef = useRef(null);
  const placeDrawerRef = useRef(null);
  const pendingVerseRef = useRef(initialParams.get("verse") || "");
  const pendingScrollVerseRef = useRef(initialParams.get("verse") || "");
  const pendingPlaceIdRef = useRef(initialParams.get("place_id") || "");
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
        if (pendingVerseRef.current) {
          setSelectedVerse(pendingVerseRef.current);
          pendingVerseRef.current = "";
        }
        if (pendingPlaceIdRef.current) {
          setSelectedLocation(
            result.locations.find((location) => location.id === pendingPlaceIdRef.current) || null,
          );
          pendingPlaceIdRef.current = "";
        }
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
    const verseNumber = pendingScrollVerseRef.current;
    if (!chapterData || !verseNumber || selectedVerse !== verseNumber) return;

    pendingScrollVerseRef.current = "";
    document.getElementById(`reader-verse-${verseNumber}`)?.scrollIntoView({
      behavior: "smooth",
      block: "center",
    });
  }, [chapterData, selectedVerse]);

  useEffect(() => {
    if (!selectedLocation) return undefined;
    let active = true;
    const previouslyFocused = document.activeElement;
    setPlaceRelations(null);
    setPlaceRelationsError("");
    setPlaceRelationsLoading(true);
    get(`/place/relations?${new URLSearchParams({
      location_id: selectedLocation.id,
    })}`)
      .then((result) => {
        if (!active) return;
        setPlaceRelations(result);
        setPlaceRelationsError(result.error || "");
      })
      .catch((error) => active && setPlaceRelationsError(error.message))
      .finally(() => active && setPlaceRelationsLoading(false));

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

  const goToReference = (reference, translation, locationId) => {
    const nextBook = reference.book_code;
    const nextChapter = String(reference.chapter);
    const nextVerse = String(reference.verse_number);
    const nextVersion = translation || version;
    const isCurrentPassage = book === nextBook
      && chapter === nextChapter
      && version === nextVersion;
    const params = new URLSearchParams({
      book: nextBook,
      chapter: nextChapter,
      verse: nextVerse,
      version: nextVersion,
    });
    if (locationId) params.set("place_id", locationId);
    window.history.replaceState(null, "", `/read?${params}`);
    setSelectedLocation(null);
    setBook(nextBook);
    setChapter(nextChapter);
    setVersion(nextVersion);
    if (isCurrentPassage) {
      setSelectedVerse(nextVerse);
      pendingPlaceIdRef.current = "";
      window.requestAnimationFrame(() => {
        document.getElementById(`reader-verse-${nextVerse}`)?.scrollIntoView({
          behavior: "smooth",
          block: "center",
        });
      });
    } else {
      pendingVerseRef.current = nextVerse;
      pendingScrollVerseRef.current = nextVerse;
      pendingPlaceIdRef.current = locationId || "";
    }
  };

  const getMapHref = (location) => {
    return `/map?${new URLSearchParams({ location_id: location.id })}`;
  };

  const testamentBooks = (testament) => availableBooks.filter((item) => item.testament === testament);
  const mentionsByBook = useMemo(() => {
    const groups = new Map();
    for (const reference of placeRelations?.mentions || []) {
      const group = groups.get(reference.book_code) || {
        bookCode: reference.book_code,
        bookName: reference.book_name,
        references: [],
      };
      group.references.push(reference);
      groups.set(reference.book_code, group);
    }
    return [...groups.values()];
  }, [placeRelations]);

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
            <a href="/">Home</a>
            <a href="/explore">Explore</a>
            <a href="/read" aria-current="page">Read Bible</a>
            <a href="/map">Map</a>
          </nav>
          <button className="theme-toggle" type="button" onClick={() => setDark((value) => !value)}>
            {dark ? "☀️" : "🌙"} <span>{dark ? "Light" : "Dark"}</span>
          </button>
        </div>
      </header>

      <main>
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
                            <p className="eyebrow">Location</p>
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
                          {!placeRelationsLoading && !placeRelationsError && placeRelations && (
                            <PlaceMap
                              key={selectedLocation.id}
                              location={placeRelations.place}
                              href={getMapHref(selectedLocation)}
                              onExpand={(event, viewport) => onExpandMap?.(event, {
                                ...viewport,
                                place: placeRelations.place,
                                relations: placeRelations,
                                readerHref: `/read?${new URLSearchParams({
                                  book,
                                  chapter,
                                  verse: String(selectedLocation.verse_number || selectedVerse),
                                  version,
                                  place_id: selectedLocation.id,
                                })}`,
                              })}
                            />
                          )}
                          {placeRelationsLoading && (
                            <p className="reader-loading" role="status">Loading place references…</p>
                          )}
                          {placeRelationsError && <p className="notice error" role="alert">{placeRelationsError}</p>}
                          {!placeRelationsLoading && !placeRelationsError && placeRelations && (
                            <>
                              <h4>Also mentioned in</h4>
                              {placeRelations.mentions.length === 0 ? (
                                <p className="empty-result">No other references for this place were found.</p>
                              ) : (
                                <ul className="place-relations">
                                  {mentionsByBook.map((group) => (
                                    <li className="mention-book-group" key={group.bookCode}>
                                      <details>
                                        <summary>
                                          <strong>{group.bookName}</strong>
                                          <span>
                                            {group.references.length} {group.references.length === 1
                                              ? "passage"
                                              : "passages"}
                                          </span>
                                        </summary>
                                        <ul className="mention-passage-list">
                                          {group.references.map((reference) => (
                                            <li key={`${reference.book_code}-${reference.chapter}-${reference.verse_number}`}>
                                              <strong>{reference.book_name} {reference.chapter}:{reference.verse_number}</strong>
                                              <button
                                                type="button"
                                                onClick={() => goToReference(
                                                  reference,
                                                  version,
                                                  reference.location_ids.find((item) =>
                                                    item.id.includes(`:${reference.book_code}:${reference.chapter}:${reference.verse_number}:`),
                                                  )?.id,
                                                )}
                                              >Go to passage →</button>
                                            </li>
                                          ))}
                                        </ul>
                                      </details>
                                    </li>
                                  ))}
                                </ul>
                              )}

                              <h4 className="also-mentioned-heading">Other translations</h4>
                              {placeRelations.reference.translations.filter(
                                (translation) => translation.code !== version,
                              ).length === 0 ? (
                                <p className="empty-result">No other translations are available for this verse.</p>
                              ) : (
                                <ul className="place-relations">
                                  {placeRelations.reference.translations
                                    .filter((translation) => translation.code !== version)
                                    .map((translation) => (
                                      <li key={translation.code}>
                                        <span>{placeRelations.reference.book_name} {placeRelations.reference.chapter}:{placeRelations.reference.verse_number}</span>
                                        <button
                                          type="button"
                                          onClick={() => goToReference(
                                            placeRelations.reference,
                                            translation.code,
                                            selectedLocation.id,
                                          )}
                                        >Read in {translation.code} →</button>
                                      </li>
                                    ))}
                                </ul>
                              )}
                            </>
                          )}
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
            title={`Location: ${segment.locations.map((item) => item.region ? `${item.name}, ${item.region}` : item.name).join("; ")}`}
            onClick={() => onSelectLocation(segment.locations[0])}
          >{segment.text}</button>
        )
        : <span key={`${segment.text}-${index}`}>{segment.text}</span>)}
    </p>
  );
}

export default ReaderPage;
