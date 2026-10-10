import { useCallback, useEffect, useRef, useState } from "react";
import L from "leaflet";
import { get } from "./api.js";
import PlaceSearch from "./PlaceSearch.jsx";
import { createStarMarker } from "./mapMarkers.js";
import SiteHeader from "./SiteHeader.jsx";

function MapPage({ initialMap, onMapReady }) {
  const [dark, setDark] = useState(() => {
    const saved = localStorage.getItem("theme");
    return saved ? saved === "dark" : window.matchMedia("(prefers-color-scheme: dark)").matches;
  });
  const [places, setPlaces] = useState([]);
  const [selectedPlace, setSelectedPlace] = useState(() => initialMap ? {
    ...initialMap.place,
    lat: Number(initialMap.place.attrs.latitude),
    lng: Number(initialMap.place.attrs.longitude),
  } : null);
  const [placeRelations, setPlaceRelations] = useState(initialMap?.relations || null);
  const [loading, setLoading] = useState(true);
  const [detailLoading, setDetailLoading] = useState(false);
  const [error, setError] = useState("");
  const [detailError, setDetailError] = useState("");
  const mapElementRef = useRef(null);
  const mapRef = useRef(null);
  const markersRef = useRef(null);
  const markersByIdRef = useRef(new Map());
  const selectedMarkerRef = useRef(null);
  const requestIdRef = useRef(0);
  const initialLocationIdRef = useRef(new URLSearchParams(window.location.search).get("location_id"));

  useEffect(() => {
    document.documentElement.classList.toggle("dark", dark);
    localStorage.setItem("theme", dark ? "dark" : "light");
  }, [dark]);

  const selectPlace = useCallback(async (place) => {
    const requestId = ++requestIdRef.current;
    selectedMarkerRef.current?.setStyle({
      radius: 6,
      fillColor: getComputedStyle(document.documentElement)
        .getPropertyValue("--node-location").trim(),
    });
    const marker = markersByIdRef.current.get(place.id);
    marker?.setStyle({
      radius: 9,
      fillColor: getComputedStyle(document.documentElement)
        .getPropertyValue("--accent").trim(),
    });
    selectedMarkerRef.current = marker || null;
    setSelectedPlace(place);
    setPlaceRelations(null);
    setDetailError("");
    setDetailLoading(true);
    try {
      const params = new URLSearchParams({ location_id: place.id });
      const result = await get(`/place/relations?${params}`);
      if (requestId === requestIdRef.current) {
        setPlaceRelations(result);
        setDetailError(result.error || "");
      }
    } catch (requestError) {
      if (requestId === requestIdRef.current) setDetailError(requestError.message);
    } finally {
      if (requestId === requestIdRef.current) setDetailLoading(false);
    }
  }, []);

  useEffect(() => {
    const map = L.map(mapElementRef.current, {
      maxZoom: 13,
      scrollWheelZoom: false,
      zoomControl: false,
    }).setView(initialMap?.center || [33, 40], initialMap?.zoom ?? 4);
    L.control.zoom({ position: "bottomleft" }).addTo(map);
    const basemap = L.tileLayer(
      "https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}",
      {
        attribution: "Esri, Maxar, Earthstar Geographics",
        maxZoom: 19,
      },
    ).addTo(map);
    const romanOverlay = L.tileLayer("https://dare.ht.lu.se/tiles/imperium/{z}/{x}/{y}.png", {
      attribution: "DARE: Digital Atlas of the Roman Empire",
      maxZoom: 11,
      opacity: 0.85,
    }).addTo(map);
    romanOverlay.once("tileerror", () => {
      console.warn("Could not load the Roman-era map overlay.");
    });
    const markers = L.layerGroup().addTo(map);
    if (initialMap) {
      createStarMarker([
        Number(initialMap.place.attrs.latitude),
        Number(initialMap.place.attrs.longitude),
      ], {
        fillColor: getComputedStyle(document.documentElement).getPropertyValue("--accent").trim(),
        size: 18,
      }).addTo(markers);
    }
    mapRef.current = map;
    markersRef.current = markers;
    const resizeObserver = new ResizeObserver(() => map.invalidateSize());
    resizeObserver.observe(mapElementRef.current);
    let readyTimer;
    const finishReady = () => {
      window.clearTimeout(readyTimer);
      basemap.off("load", finishReady);
      map.invalidateSize();
      onMapReady?.();
    };
    if (onMapReady) {
      basemap.once("load", finishReady);
      // Unavailable tiles must not hold navigation indefinitely.
      readyTimer = window.setTimeout(finishReady, 800);
    }
    return () => {
      window.clearTimeout(readyTimer);
      basemap.off("load", finishReady);
      resizeObserver.disconnect();
      map.remove();
      mapRef.current = null;
      markersRef.current = null;
      markersByIdRef.current.clear();
    };
  }, []);

  useEffect(() => {
    let active = true;
    get("/map/places")
      .then((result) => {
        if (!active) return;
        setPlaces(result.places);
        markersRef.current.clearLayers();
        const bounds = [];
        for (const place of result.places) {
          const marker = createStarMarker([place.lat, place.lng], {
            fillColor: getComputedStyle(document.documentElement)
              .getPropertyValue("--node-location").trim(),
            size: 14,
          });
          marker.bindTooltip(place.name);
          marker.on("click", () => selectPlace(place));
          marker.addTo(markersRef.current);
          markersByIdRef.current.set(place.id, marker);
          bounds.push([place.lat, place.lng]);
        }
        if (bounds.length && !initialMap) mapRef.current.fitBounds(bounds, { padding: [28, 28], maxZoom: 7 });

        const initialId = initialLocationIdRef.current;
        if (initialId) {
          initialLocationIdRef.current = "";
          (initialMap
            ? Promise.resolve(initialMap.relations)
            : get(`/place/relations?${new URLSearchParams({ location_id: initialId })}`))
            .then((relations) => {
              if (!active || relations.error) return;
              const aliases = new Set(relations.place.aliases.map((alias) => alias.toLocaleLowerCase()));
              const region = (relations.place.region || "").toLocaleLowerCase();
              const initialPlace = result.places.find((place) =>
                (place.region || "").toLocaleLowerCase() === region
                && place.aliases.some((alias) => aliases.has(alias.toLocaleLowerCase())),
              );
              if (initialPlace) {
                const marker = markersByIdRef.current.get(initialPlace.id);
                marker?.setStyle({
                  radius: 9,
                  fillColor: getComputedStyle(document.documentElement)
                    .getPropertyValue("--accent").trim(),
                });
                selectedMarkerRef.current = marker || null;
                setSelectedPlace(initialPlace);
                setPlaceRelations(relations);
                if (!initialMap) {
                  mapRef.current?.flyTo([initialPlace.lat, initialPlace.lng], 9, {
                    duration: 0.8,
                    animate: !window.matchMedia("(prefers-reduced-motion: reduce)").matches,
                  });
                }
              }
            })
            .catch((requestError) => active && setDetailError(requestError.message));
        }
      })
      .catch((requestError) => active && setError(requestError.message))
      .finally(() => active && setLoading(false));
    return () => { active = false; };
  }, [selectPlace]);

  const passageHref = (reference) => {
    const locationId = reference.location_ids?.[0]?.id || selectedPlace?.id;
    const params = new URLSearchParams({
      book: reference.book_code,
      chapter: String(reference.chapter),
      verse: String(reference.verse_number),
      version: "KJV",
    });
    if (locationId) params.set("place_id", locationId);
    return `/read?${params}`;
  };

  const references = placeRelations
    ? [placeRelations.reference, ...placeRelations.mentions]
    : [];

  return (
    <div className="app-shell">
      <SiteHeader currentPage="/map" dark={dark} onToggleTheme={() => setDark((value) => !value)} />

      <main className="map-page-root">
        <section className="panel map-page-panel" aria-label="Map of biblical places" tabIndex={-1}>
          <div className={`map-page-layout${selectedPlace ? " has-selection" : ""}`}>
            <div className="map-page-map-wrap">
              <div className="map-page-heading">
                <PlaceSearch
                  places={places}
                  disabled={loading || Boolean(error)}
                  onSelect={(place) => {
                    selectPlace(place);
                    mapRef.current?.flyTo([place.lat, place.lng], 9, {
                      duration: 0.8,
                      animate: !window.matchMedia("(prefers-reduced-motion: reduce)").matches,
                    });
                  }}
                />
                <p role="status">{loading ? "Loading places…" : `${places.length} places · Select a marker to see passages`}</p>
                {error && <p className="notice error" role="alert">{error}</p>}
              </div>
              <div className="map-page-canvas" ref={mapElementRef} aria-label="Map of biblical places" />
              {loading && <div className="map-page-placeholder">Loading map locations…</div>}
            </div>

            {selectedPlace && (
              <aside className="map-page-detail" aria-live="polite">
                <div className="map-page-detail-header">
                  <div>
                    <span className="eyebrow">Biblical place</span>
                    <h3>{placeRelations?.place.name || selectedPlace.name}</h3>
                    {selectedPlace.region && <p>{selectedPlace.region}</p>}
                  </div>
                  <button
                    className="place-details-close"
                    type="button"
                    aria-label="Close place details"
                    onClick={() => {
                      requestIdRef.current += 1;
                      setSelectedPlace(null);
                      setPlaceRelations(null);
                      setDetailError("");
                      selectedMarkerRef.current?.setStyle({
                        radius: 6,
                        fillColor: getComputedStyle(document.documentElement)
                          .getPropertyValue("--node-location").trim(),
                      });
                      selectedMarkerRef.current = null;
                    }}
                  >×</button>
                </div>
                <div className="map-page-reference-list">
                  <h4>Passages mentioning this place</h4>
                  {detailLoading && <p className="reader-loading" role="status">Loading passages…</p>}
                  {detailError && <p className="notice error" role="alert">{detailError}</p>}
                  {!detailLoading && !detailError && placeRelations && (
                    references.length ? references.map((reference) => (
                      <article
                        className="map-page-reference"
                        key={`${reference.book_code}-${reference.chapter}-${reference.verse_number}`}
                      >
                        <div>
                          <strong>{reference.book_name} {reference.chapter}:{reference.verse_number}</strong>
                          <p>
                            {reference.translations.find((translation) => translation.code === "KJV")?.text
                              || reference.translations[0]?.text
                              || "Passage text unavailable."}
                          </p>
                        </div>
                        <a href={passageHref(reference)}>Read →</a>
                      </article>
                    )) : <p className="empty-result">No passages found for this place.</p>
                  )}
                </div>
              </aside>
            )}
          </div>
        </section>
      </main>
    </div>
  );
}

export default MapPage;
