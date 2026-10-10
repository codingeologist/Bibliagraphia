import { useEffect, useRef, useState } from "react";
import { get } from "./api.js";
import PlaceGraphPreview from "./PlaceGraphPreview.jsx";
import { describeNode, relationshipDescription, relationshipHref } from "./nodeLinks.js";

export default function ReaderDetailsDrawer({ item, onClose, onSelectItem }) {
  const isPerson = item.label === "figure";
  const kind = isPerson ? "person" : "passage";
  const title = isPerson ? item.name : describeNode(item);
  const drawerRef = useRef(null);
  const closeRef = useRef(null);
  const [graph, setGraph] = useState(null);
  const [error, setError] = useState("");
  const [family, setFamily] = useState(null);
  const [familyError, setFamilyError] = useState("");

  useEffect(() => {
    if (!isPerson) return undefined;
    let active = true;
    setFamily(null);
    setFamilyError("");
    get(`/reader/person-relations?${new URLSearchParams({ person_id: item.id })}`)
      .then((result) => { if (active) setFamily(result); })
      .catch((requestError) => { if (active) setFamilyError(requestError.message); });
    return () => { active = false; };
  }, [item.id, isPerson]);

  useEffect(() => {
    let active = true;
    setGraph(null);
    setError("");
    get(`/graph?${new URLSearchParams({
      node: item.name, label: item.label, node_id: item.id, hops: "1",
    })}`).then((result) => {
      if (!active) return;
      if (result.error) setError(result.error);
      else setGraph(result);
    }).catch((requestError) => {
      if (active) setError(requestError.message);
    });
    return () => { active = false; };
  }, [item.id, item.name, item.label]);

  useEffect(() => {
    const previousFocus = document.activeElement;
    closeRef.current?.focus();
    const handleKeyDown = (event) => {
      if (event.key === "Escape") onClose();
      if (event.key !== "Tab") return;
      const items = drawerRef.current?.querySelectorAll("a[href], button:not(:disabled)");
      if (!items?.length) return;
      const first = items[0];
      const last = items[items.length - 1];
      if (event.shiftKey && document.activeElement === first) {
        event.preventDefault();
        last.focus();
      } else if (!event.shiftKey && document.activeElement === last) {
        event.preventDefault();
        first.focus();
      }
    };
    document.addEventListener("keydown", handleKeyDown);
    return () => {
      document.removeEventListener("keydown", handleKeyDown);
      if (previousFocus instanceof HTMLElement) previousFocus.focus();
    };
  }, [onClose]);

  const details = family?.person || graph?.nodes.find((node) => node.id === item.id);

  return (
    <>
      <button className="place-drawer-backdrop" type="button" aria-label={`Close ${kind} details`} onClick={onClose} />
      <aside ref={drawerRef} className="place-drawer" role="dialog" aria-modal="true" aria-labelledby="reader-details-title">
        <header className="place-drawer-header">
          <div>
            <p className="eyebrow">{isPerson ? "Person" : "Passage"}</p>
            <h3 id="reader-details-title">{title}</h3>
          </div>
          <button ref={closeRef} className="place-details-close" type="button" aria-label={`Close ${kind} details`} onClick={onClose}>×</button>
        </header>
        <div className="place-drawer-content">
          {!graph && !error && <p className="reader-loading" role="status">Loading related items…</p>}
          {error && <p className="notice error" role="alert">{error}</p>}
          {(graph || family) && (
            <>
              <h4>{isPerson ? `About ${item.name}` : "Passage text"}</h4>
              <p>{isPerson
                ? details?.attrs?.description || "No description is available for this person yet."
                : item.text || details?.attrs?.text || "Passage text is not available."}</p>
              {[details?.attrs?.testament, details?.attrs?.category].filter(Boolean).length > 0 && (
                <p className="text-muted text-[12px]">{[details?.attrs?.testament, details?.attrs?.category].filter(Boolean).join(" · ")}</p>
              )}
              {graph && <PlaceGraphPreview location={item} suppliedGraph={graph} heading="Related passages, people and places" />}
            </>
          )}
          {isPerson && (
            <section aria-label="Family relationships">
              <h4>Family relationships</h4>
              {!family && !familyError && <p className="reader-loading" role="status">Loading family relationships…</p>}
              {familyError && <p className="notice error" role="alert">{familyError}</p>}
              {family && (family.relationships.length ? (
                <ul className="place-relations">
                  {family.relationships.map((relationship) => (
                    <li key={`${relationship.source}-${relationship.target}`}>
                      <span>{relationshipDescription(relationship.label, relationship.source === item.id, relationship.attrs)}</span>
                      <button type="button" onClick={() => onSelectItem(relationship.person)}>
                        {describeNode(relationship.person)}
                      </button>
                    </li>
                  ))}
                </ul>
              ) : <p className="empty-result">No family relationships are recorded for this person yet.</p>)}
            </section>
          )}
          <a href={relationshipHref(item)}>Explore all connections →</a>
        </div>
      </aside>
    </>
  );
}
