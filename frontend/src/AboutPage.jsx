import { useEffect, useState } from "react";
import SiteHeader from "./SiteHeader.jsx";

const team = [
  { name: "Ally", icon: "🚀", github: "alasdairmackenzie" },
  { name: "Jonah", icon: "🐋", github: "mogerjonah-cmyk" },
  { name: "Elle", icon: "🦋", github: "SyntaxPrincess26" },
  { name: "Sidd", icon: "🦉", github: "codingeologist" },
  { name: "Jonathon", icon: "🦊" },
];

const sources = [
  ["books.json", "Bible books and their names across translations.", "73 books"],
  ["versions.json", "Bible version information for Vulgate (VUL), Douay-Rheims (DRB) and King James (KJV).", "3 original translations"],
  ["verses.json", "Complete verse text across the original three translations.", "102,722 verses"],
  ["location_regions.json", "Geographical location mentions with coordinates.", "7,460 mentions"],
  ["regions.json", "Regional descriptions and keywords.", "36 regions"],
  ["figures.json", "Biblical figures and their STEP Bible identifiers.", "238 figures"],
];

function TeamAvatar({ name, icon, github }) {
  const [failed, setFailed] = useState(false);
  return (
    <div className="mb-3">
      {github && !failed ? (
        <img className="mx-auto h-20 w-20 rounded-full object-cover border border-line"
          src={`https://github.com/${github}.png?size=160`} alt={`${name}'s GitHub avatar`}
          width="80" height="80" loading="lazy" referrerPolicy="no-referrer"
          onError={() => setFailed(true)} />
      ) : <span className="block text-[42px]" aria-hidden="true">{icon}</span>}
      {failed && <p className="text-[11px] text-muted mt-2" role="status">GitHub image unavailable.</p>}
    </div>
  );
}

export default function AboutPage() {
  const [dark, setDark] = useState(() => {
    const saved = localStorage.getItem("theme");
    return saved ? saved === "dark" : window.matchMedia("(prefers-color-scheme: dark)").matches;
  });
  useEffect(() => {
    document.documentElement.classList.toggle("dark", dark);
    localStorage.setItem("theme", dark ? "dark" : "light");
  }, [dark]);

  return (
    <div className="app-shell min-h-screen">
      <SiteHeader currentPage="/about" dark={dark} onToggleTheme={() => setDark((value) => !value)} />
      <main className="pt-7 pb-8 space-y-5">
        <section className="panel">
          <h2>About Bibliagraphia</h2>
          <p className="panel-copy">A living map of scripture. Bibliagraphia connects Bible passages, translations, places and regions so you can read, compare and discover how they fit together.</p>
          <p className="panel-copy">Originally built with TypeDB, the project now uses a single-file DuckDB graph, a FastAPI backend and a React frontend.</p>
          <a className="text-accent text-[13px]" href="https://github.com/codingeologist/Bibliagraphia" target="_blank" rel="noreferrer">Explore the project on GitHub</a>
        </section>

        <section className="panel" aria-labelledby="about-team">
          <h2 id="about-team">Meet the team</h2>
          <ul className="grid gap-4 mt-5 p-0 list-none [grid-template-columns:repeat(auto-fit,minmax(130px,1fr))]">
            {team.map(({ name, icon, github }) => (
              <li key={name} className="rounded-lg border border-line bg-accent-soft p-5 text-center">
                <TeamAvatar name={name} icon={icon} github={github} />
                <h3 className="font-display text-[20px] font-semibold">{name}</h3>
                {github && (
                  <a className="inline-block mt-3 text-accent text-[12px] break-all" href={`https://github.com/${github}`} target="_blank" rel="noreferrer" aria-label={`${name} on GitHub`}>@{github}</a>
                )}
              </li>
            ))}
          </ul>
        </section>

        <section className="panel" aria-labelledby="about-credits">
          <h2 id="about-credits">Attributions and thanks</h2>
          <ul className="pl-5 mt-4 space-y-4 text-[13px] leading-relaxed">
            <li><a className="text-accent" href="https://faithtech.com" target="_blank" rel="noreferrer">FaithTech</a> and <a className="text-accent" href="https://ftbuild.org.uk/projects.html#bibliagraphia" target="_blank" rel="noreferrer">FaithTech BUILD 26</a> — Bibliagraphia was selected as one of fifteen project briefs for the UK hackathon. Thanks to the BUILD team for including an independent project.</li>
            <li><a className="text-accent" href="https://www.esri.com/" target="_blank" rel="noreferrer">Esri</a>, Maxar and Earthstar Geographics — imagery credits used by the satellite basemap.</li>
            <li><a className="text-accent" href="https://dare.ht.lu.se/" target="_blank" rel="noreferrer">DARE: Digital Atlas of the Roman Empire</a> — the Roman-era map layer.</li>
            <li><a className="text-accent" href="https://leafletjs.com/" target="_blank" rel="noreferrer">Leaflet</a> — interactive maps. Map-provider attribution remains visible on the maps themselves.</li>
            <li><a className="text-accent" href="https://www.STEPBible.org" target="_blank" rel="noreferrer">STEP Bible</a> — TIPNR and TVTMS datasets from Tyndale House, Cambridge, used for biblical figure references and verse numbering under CC BY 4.0.</li>
            <li><a className="text-accent" href="https://duckdb.org/" target="_blank" rel="noreferrer">DuckDB</a>, <a className="text-accent" href="https://fastapi.tiangolo.com/" target="_blank" rel="noreferrer">FastAPI</a>, <a className="text-accent" href="https://react.dev/" target="_blank" rel="noreferrer">React</a>, <a className="text-accent" href="https://vite.dev/" target="_blank" rel="noreferrer">Vite</a>, <a className="text-accent" href="https://tailwindcss.com/" target="_blank" rel="noreferrer">Tailwind CSS</a>, <a className="text-accent" href="https://d3js.org/" target="_blank" rel="noreferrer">D3</a> and <a className="text-accent" href="https://gofastmcp.com/" target="_blank" rel="noreferrer">FastMCP</a> — open-source tools behind the app.</li>
          </ul>
          <p className="panel-copy mt-4">Source data and third-party services retain their respective rights and terms. These credits do not replace their licence notices.</p>
        </section>
        <section className="panel" aria-labelledby="about-sources">
          <h2 id="about-sources">Data sources</h2>
          <p className="panel-copy">The graph is built from the canonical JSON datasets in the project’s data directory. These descriptions and baseline counts come from the project README; the running database may include additional translations.</p>
          <dl className="grid gap-4 mt-4 [grid-template-columns:repeat(auto-fit,minmax(min(100%,280px),1fr))]">
            {sources.map(([file, description, count]) => (
              <div key={file} className="rounded-md border border-line p-4 min-w-0">
                <dt className="font-semibold text-[13px]">
                  <a className="text-accent break-words" href={`https://github.com/codingeologist/Bibliagraphia/blob/main/data/${file}`} target="_blank" rel="noreferrer">{file}</a>
                </dt>
                <dd className="m-0 mt-2 text-[13px] leading-relaxed text-muted">{description}</dd>
                <dd className="m-0 mt-2 text-[12px] font-semibold">{count}</dd>
              </div>
            ))}
          </dl>
          <a className="inline-block mt-4 text-accent text-[12px]" href="https://github.com/codingeologist/Bibliagraphia#data-sources" target="_blank" rel="noreferrer">Read the data source documentation</a>
        </section>
      </main>
    </div>
  );
}
