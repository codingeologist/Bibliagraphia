import { useEffect, useId, useRef, useState } from "react";

export default function BookPicker({ books, value, onSelect }) {
  const id = useId();
  const triggerRef = useRef(null);
  const inputRef = useRef(null);
  const optionRefs = useRef([]);
  const [open, setOpen] = useState(false);
  const [query, setQuery] = useState("");
  const [activeIndex, setActiveIndex] = useState(-1);
  const term = query.trim().toLocaleLowerCase();
  const matches = books.filter((item) =>
    item.name.toLocaleLowerCase().includes(term) || item.code.toLocaleLowerCase().includes(term));
  const selected = books.find((item) => item.code === value);

  useEffect(() => {
    setOpen(false);
    setQuery("");
    setActiveIndex(-1);
  }, [value, books]);

  useEffect(() => {
    if (open) inputRef.current?.focus();
  }, [open]);

  useEffect(() => {
    if (open && activeIndex >= 0) {
      optionRefs.current[activeIndex]?.scrollIntoView({ block: "nearest" });
    }
  }, [open, activeIndex]);

  const close = () => {
    setOpen(false);
    triggerRef.current?.focus();
  };
  const choose = (item) => {
    close();
    onSelect(item.code);
  };

  return (
    <div className="reader-control reader-book-picker" onBlur={(event) => {
      if (!event.currentTarget.contains(event.relatedTarget)) setOpen(false);
    }}>
      <label htmlFor={`${id}-trigger`}>Book</label>
      <button id={`${id}-trigger`} type="button" ref={triggerRef}
        className="reader-book-trigger" aria-label="Book"
        aria-describedby={`${id}-selection`} aria-expanded={open}
        aria-controls={`${id}-panel`} disabled={!books.length}
        onClick={() => {
          setQuery("");
          setActiveIndex(-1);
          setOpen((current) => !current);
        }}>
        <span id={`${id}-selection`}>{selected?.name || "Choose a book"}</span>
        <span aria-hidden="true">⌄</span>
      </button>
      {open && (
        <div id={`${id}-panel`} className="reader-book-panel">
          <input ref={inputRef} type="text" role="combobox" name="book-search"
            aria-label="Search books" aria-autocomplete="list" aria-expanded="true"
            aria-controls={`${id}-results`}
            aria-activedescendant={activeIndex >= 0 ? `${id}-option-${activeIndex}` : undefined}
            autoComplete="off" placeholder="Search books…" value={query}
            onChange={(event) => {
              setQuery(event.target.value);
              setActiveIndex(-1);
            }}
            onKeyDown={(event) => {
              if (event.nativeEvent.isComposing) return;
              if (event.key === "Escape") {
                event.preventDefault();
                event.stopPropagation();
                close();
              } else if (event.key === "ArrowDown" || event.key === "ArrowUp") {
                event.preventDefault();
                if (matches.length) setActiveIndex((index) =>
                  index < 0 ? (event.key === "ArrowDown" ? 0 : matches.length - 1)
                    : (index + (event.key === "ArrowDown" ? 1 : -1) + matches.length) % matches.length);
              } else if (event.key === "Enter") {
                event.preventDefault();
                if (matches.length) choose(matches[activeIndex < 0 ? 0 : activeIndex]);
              }
            }} />
          <div id={`${id}-results`} className="reader-book-results" role="listbox" aria-label="Books">
            {["Old Testament", "New Testament"].map((testament) => {
              const items = matches.filter((item) => item.testament === testament);
              return items.length > 0 && (
                <div key={testament} role="group" aria-label={testament}>
                  <div className="reader-book-group" aria-hidden="true">{testament}</div>
                  {items.map((item) => {
                    const index = matches.indexOf(item);
                    return (
                      <div key={item.code} id={`${id}-option-${index}`}
                        ref={(element) => { optionRefs.current[index] = element; }}
                        role="option" aria-selected={item.code === value}
                        className={index === activeIndex ? "active" : undefined}
                        onPointerDown={(event) => event.preventDefault()}
                        onClick={() => choose(item)}>
                        {item.name}
                      </div>
                    );
                  })}
                </div>
              );
            })}
          </div>
          <p className="reader-book-status" role="status">
            {matches.length ? `${matches.length} matching books` : "No matching books."}
          </p>
        </div>
      )}
    </div>
  );
}
