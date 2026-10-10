import { useEffect, useRef, useState } from "react";
import L from "leaflet";
import { get } from "./api.js";
import { createCircleMarker } from "./mapMarkers.js";

function popupContent(point) {
  const popup = L.DomUtil.create("div", "map-popup");
  const title = L.DomUtil.create("strong", "map-popup-title", popup);
  title.textContent = point.secondary_name
    ? `${point.name} (${point.secondary_name})`
    : point.name;
  const reference = L.DomUtil.create("div", "map-popup-reference", popup);
  reference.textContent = `${point.region} · ${point.book_code} ${point.chapter}:${point.verse_number}`;
  if (point.text) {
    const text = L.DomUtil.create("p", "map-popup-text", popup);
    text.textContent = point.text;
  }
  return popup;
}

function MapExplorer({ seed }) {
  const mapElementRef = useRef(null);
  const mapRef = useRef(null);
  const markersRef = useRef(null);
  const [scope, setScope] = useState("region");
  const [query, setQuery] = useState("");
  const [summary, setSummary] = useState("");
  const [error, setError] = useState("");
  const [loading, setLoading] = useState(false);

  useEffect(() => {
    const map = L.map(mapElementRef.current, { maxZoom: 12, scrollWheelZoom: false }).setView([33, 40], 5);
    L.tileLayer(
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
    mapRef.current = map;
    markersRef.current = markers;
    const enableScrollZoom = () => {
      if (mapElementRef.current?.contains(document.activeElement)) map.scrollWheelZoom.enable();
    };
    map.on("click", () => map.scrollWheelZoom.enable());
    map.on("mouseout", enableScrollZoom);
    const resizeObserver = new ResizeObserver(() => map.invalidateSize());
    resizeObserver.observe(mapElementRef.current);
    return () => {
      resizeObserver.disconnect();
      map.remove();
      mapRef.current = null;
      markersRef.current = null;
    };
  }, []);

  const plot = async (requestedScope = scope, requestedQuery = query) => {
    const value = requestedQuery.trim();
    if (!value) return;
    setScope(requestedScope);
    setQuery(value);
    setLoading(true);
    setError("");
    setSummary("Loading places…");
    try {
      const params = new URLSearchParams({ [requestedScope]: value });
      const result = await get(`/map/points?${params}`);
      markersRef.current.clearLayers();
      const bounds = [];
      for (const point of result.points) {
        const marker = createCircleMarker([point.lat, point.lng], {
          fillColor: getComputedStyle(document.documentElement)
            .getPropertyValue("--node-location").trim(),
          size: 12,
        });
        marker.bindPopup(() => popupContent(point), { maxWidth: 300 });
        marker.addTo(markersRef.current);
        bounds.push([point.lat, point.lng]);
      }
      setSummary(`${result.count} place mention${result.count === 1 ? "" : "s"} in ${result.region || result.book}`
        + (result.truncated ? " · showing up to 1,000 markers" : ""));
      if (bounds.length) mapRef.current.fitBounds(bounds, { padding: [28, 28], maxZoom: 9 });
      else mapRef.current.setView([33, 40], 5);
    } catch (requestError) {
      markersRef.current?.clearLayers();
      setSummary("");
      setError(requestError.message);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    if (seed) plot(seed.scope, seed.query);
  }, [seed]);

  const toggleFullscreen = async () => {
    try {
      if (document.fullscreenElement) await document.exitFullscreen();
      else await mapElementRef.current.requestFullscreen();
      window.setTimeout(() => mapRef.current?.invalidateSize(), 100);
    } catch (fullscreenError) {
      setError(`Could not enter fullscreen: ${fullscreenError.message}`);
    }
  };

  return (
    <div className="map-explorer">
      <form className="form-row map-controls" onSubmit={(event) => { event.preventDefault(); plot(); }}>
        <select aria-label="Find places by region or Bible book" value={scope} onChange={(event) => setScope(event.target.value)}>
          <option value="region">Region</option>
          <option value="book">Bible book</option>
        </select>
        <input
          aria-label="Region name or Bible book abbreviation"
          value={query}
          onChange={(event) => setQuery(event.target.value)}
          placeholder="Try Syria or GEN (Genesis)"
        />
        <button className="button button-secondary" disabled={loading}>
          {loading ? "Loading…" : "Show places"}
        </button>
      </form>
      {summary && <p className="result-summary" role="status">{summary}</p>}
      {error && <p className="notice error" role="alert">{error}</p>}
      <div className="map-wrap">
        <div className="map-canvas" ref={mapElementRef} aria-label="Map of biblical places" />
        <button
          className="map-fullscreen"
          type="button"
          aria-label="Toggle map fullscreen"
          title="Toggle map fullscreen"
          onClick={toggleFullscreen}
        >⛶</button>
        {!summary && !loading && <div className="map-placeholder">Choose a region or book to plot places</div>}
      </div>
    </div>
  );
}

export default MapExplorer;
