import { useId, useState } from "react";

function PlaceSearch({ places, disabled, onSelect }) {
  const listId = useId();
  const [query, setQuery] = useState("");
  const [open, setOpen] = useState(false);
  const [activeIndex, setActiveIndex] = useState(-1);
  const term = query.trim().toLocaleLowerCase();
  const matches = term ? places.filter((place) =>
    [place.name, ...place.aliases].some((name) => name.toLocaleLowerCase().includes(term)),
  ).sort((a, b) => {
    const startsWith = (place) => [place.name, ...place.aliases]
      .some((name) => name.toLocaleLowerCase().startsWith(term));
    return Number(startsWith(b)) - Number(startsWith(a)) || a.name.localeCompare(b.name);
  }) : [];
  const suggestions = matches.slice(0, 8);
  const expanded = open && Boolean(term);

  const choose = (place) => {
    setQuery(place.name);
    setOpen(false);
    setActiveIndex(-1);
    onSelect(place);
  };

  return (
    <form
      className="place-search"
      role="search"
      onSubmit={(event) => {
        event.preventDefault();
        if (expanded && suggestions.length) choose(suggestions[activeIndex < 0 ? 0 : activeIndex]);
      }}
      onBlur={(event) => {
        if (!event.currentTarget.contains(event.relatedTarget)) {
          setOpen(false);
          setActiveIndex(-1);
        }
      }}
    >
      <input
        type="text"
        role="combobox"
        name="place"
        aria-label="Search places"
        aria-autocomplete="list"
        aria-expanded={expanded}
        aria-controls={listId}
        aria-activedescendant={expanded && activeIndex >= 0 ? `${listId}-${activeIndex}` : undefined}
        autoComplete="off"
        placeholder="Search places…"
        disabled={disabled}
        value={query}
        onChange={(event) => {
          setQuery(event.target.value);
          setActiveIndex(-1);
          setOpen(true);
        }}
        onFocus={() => setOpen(true)}
        onKeyDown={(event) => {
          if (event.key === "Escape") {
            event.preventDefault();
            setOpen(false);
            setActiveIndex(-1);
          } else if (suggestions.length && (event.key === "ArrowDown" || event.key === "ArrowUp")) {
            event.preventDefault();
            setOpen(true);
            setActiveIndex((index) => {
              if (!expanded || index < 0) return event.key === "ArrowDown" ? 0 : suggestions.length - 1;
              return (index + (event.key === "ArrowDown" ? 1 : -1) + suggestions.length)
                % suggestions.length;
            });
          }
        }}
      />
      <ul id={listId} className="place-search-results" role="listbox" aria-label="Matching places" hidden={!expanded || !suggestions.length}>
        {suggestions.map((place, index) => (
          <li
            id={`${listId}-${index}`}
            key={place.id}
            role="option"
            aria-selected={index === activeIndex}
            onPointerDown={(event) => event.preventDefault()}
            onClick={() => choose(place)}
          >
            <strong>{place.name}</strong>
            {place.region && <span>{place.region}</span>}
          </li>
        ))}
      </ul>
      {expanded && (
        <p className="place-search-status" role="status">
          {matches.length
            ? `${matches.length} matching place${matches.length === 1 ? "" : "s"}${matches.length > 8 ? " · Showing first 8" : ""}`
            : "No matching places."}
        </p>
      )}
    </form>
  );
}

export default PlaceSearch;
