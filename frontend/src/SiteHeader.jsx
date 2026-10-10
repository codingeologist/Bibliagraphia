import { useEffect, useRef, useState } from "react";

function SiteHeader({ currentPage, dark, onToggleTheme }) {
  const [open, setOpen] = useState(false);
  const menuRef = useRef(null);
  const triggerRef = useRef(null);

  useEffect(() => {
    if (!open) return undefined;
    const dismiss = (event) => {
      if (!menuRef.current.contains(event.target)) setOpen(false);
    };
    const handleKey = (event) => {
      if (event.key === "Escape") {
        setOpen(false);
        triggerRef.current.focus();
      }
    };
    document.addEventListener("pointerdown", dismiss);
    document.addEventListener("focusin", dismiss);
    document.addEventListener("keydown", handleKey);
    return () => {
      document.removeEventListener("pointerdown", dismiss);
      document.removeEventListener("focusin", dismiss);
      document.removeEventListener("keydown", handleKey);
    };
  }, [open]);

  return (
    <header className="topbar">
      <a className="brand" href="/" aria-label="Bibliagraphia home">
        <img className="brand-mark" src="/favicon.svg" alt="" aria-hidden="true" />
        <div>
          <h1>Bibliagraphia</h1>
          <p>A living map of scripture</p>
        </div>
      </a>
      <div className="header-actions">
        <nav className="site-nav" aria-label="Main navigation">
          {[["/", "Home"], ["/explore", "Explore"], ["/read", "Read Bible"], ["/map", "Map"]].map(([href, label]) => (
            <a key={href} href={href} aria-current={currentPage === href ? "page" : undefined}>{label}</a>
          ))}
        </nav>
        <div className="site-menu" ref={menuRef}>
          <button
            ref={triggerRef}
            className="menu-toggle"
            type="button"
            aria-label="Settings"
            aria-expanded={open}
            aria-controls="site-settings"
            onClick={() => setOpen((value) => !value)}
          >
            <svg width="20" height="20" viewBox="0 0 24 24" aria-hidden="true">
              <path d="M4 6h16M4 12h16M4 18h16" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" />
            </svg>
          </button>
          {open && (
            <div className="site-menu-panel" id="site-settings">
              <button
                className="theme-toggle"
                type="button"
                onClick={() => {
                  onToggleTheme();
                  setOpen(false);
                  triggerRef.current.focus();
                }}
              >
                Switch to {dark ? "light" : "dark"} mode
              </button>
            </div>
          )}
        </div>
      </div>
    </header>
  );
}

export default SiteHeader;
