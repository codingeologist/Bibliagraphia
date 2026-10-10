<p align="center">
  <a href="https://faithtech.com">
    <img src="https://ftbuild.org.uk/assets/faithtech-icon.svg" alt="FaithTech" height="60">
  </a>
  &nbsp;&nbsp;&nbsp;
  <a href="https://ftbuild.org.uk/projects.html#bibliagraphia">
    <img src="https://ftbuild.org.uk/assets/BUILD.svg" alt="FaithTech BUILD 26" height="60">
  </a>
</p>

# Bibliagraphia

__Biblia__ & __Graphia__ — a study of different versions of the Holy Bible
using a **single-file DuckDB graph** to map versions and verses of the Bible
together with locations and regions.

This is a refactor of the original
[TypeDB](https://github.com/typedb/typedb) version of Bibliagraphia. The
graph is now a single-file DuckDB database (one `nodes` table, one `edges`
table) queried with parameterized recursive SQL, served by a FastAPI API
with a React (Vite) frontend.
The original TypeDB loader (`bible_loader.py`) and schema
(`bible_schema.tql`) are kept in the repo for reference.

## What and Why?

The Bible exists in many translations, and most tools treat it as a flat book: search by keyword, look up by reference.
Bibliagraphia instead stores open-licensed translations (KJV, Latin Vulgate, Douay–Rheims and hopefully more...) in a single queryable graph: 
- verses connect to their books and versions
- every place mentioned connects to a geocoded location and its region.
The result is a non-linear way to explore the scriptures: walk the graph around a verse, compare one verse side by side across translations,
trace the shortest path from a book to a region, or plot every place a book mentions on a Roman Empire–era map.

![Example Graph](docs/example_graph.png)

![Example Map](docs/example_map.png)

## Acknowledgements

Bibliagraphia has been selected as one of the fifteen project briefs for [BUILD 26](https://ftbuild.org.uk/projects.html#bibliagraphia), the [FaithTech](https://faithtech.com) UK hackathon, where the focus will be exploring how the graph could power a personal knowledge graph — mapping reflections onto verses — and finding new ways to visualise the connections. Thanks to the BUILD team for including an independent project among the briefs.

## Overview

The graph database maps Bible verses with:
- Multiple translations (Vulgate, Douay-Rheims, King James)
- Geographical location references
- Regional information
- Full book and chapter structure

## Data sources

The database is built from the canonical JSON files in `/data`:

- `books.json` — Bible books with translation names (73 books)
- `versions.json` — Bible version information (VUL, DRB, KJV)
- `verses.json` — Complete verse text (102,722 verses across 3 versions)
- `location_regions.json` — Geographical locations with coordinates (7,460 mentions)
- `regions.json` — Regional descriptions and keywords (36 regions)

## Schema design

The graph is heterogeneous, stored in two tables:

### `nodes` — one row per graph vertex

| column                                                    | purpose                                                             |
|-----------------------------------------------------------|---------------------------------------------------------------------|
| `id`                                                      | synthetic key, e.g. `book:GEN`, `verse:DRB:GEN:1:1`, `region:Syria` |
| `label`                                                   | node type: `version` / `book` / `verse` / `region` / `location`     |
| `name`                                                    | human-readable name (for search & display)                          |
| `version_code`, `book_code`, `chapter`, `verse_number`    | pulled-out query columns for verses/locations                       |
| `attrs` | JSON blob of all other type-specific attributes |

### `edges` — one row per directed relation

| label                | from -> to         | meaning                      |
|----------------------|--------------------|------------------------------|
| `verse_in_book`      | book -> verse      | a book contains a verse      |
| `verse_in_version`   | version -> verse   | a version contains a verse   |
| `location_in_verse`  | verse -> location  | a verse mentions a location  |
| `location_in_region` | region -> location | a region contains a location |

There are deliberately **no self-referencing foreign keys**: edges are only created between
nodes actually present (referential integrity), which keeps the graph clean.

## Quick start

Python 3.9 or newer is supported.

```bash
# Either install from pyproject (creates the `bibliagraphia` package + entrypoints):
uv venv venv && source venv/bin/activate && uv pip install -e ".[dev]"

# …or just the runtime deps:
# uv pip install -r requirements.txt

# 1. Build the graph from the JSON sources (a few seconds):
venv/bin/bibliagraphia-build     # -> data/bible.db
# (equivalent to: venv/bin/python scripts/build_db.py)

# 2. Run the API (port 8000):
venv/bin/uvicorn app.api:app --reload

# 3. Run the React frontend (see "Vite + React frontend" below) — or the
#    full two-container stack with `docker compose up -d --build`, which
#    serves the app at http://localhost:8080.
```

The API auto-builds `data/bible.db` from the JSON on first run if it's
missing, so step 1 is optional for local play.

### Vite + React frontend

The standalone React frontend lives in `frontend/` and is the sole UI for
the API. Start the API in one terminal, then run:

```bash
cd frontend
npm install
npm run dev
```

The frontend uses Vite, React, and Tailwind CSS. Tailwind utility classes can be
used directly in the JSX components under `frontend/src/`.
Open `/read` in the Vite app to read a Bible book. Choose a book, chapter,
translation, or jump directly to a verse; previous/next controls move between
chapters. Underlined place mentions that match graph locations open place
details with links to the graph and map.

Open the Vite URL shown in the terminal (normally `http://localhost:5173`).
Vite proxies API requests to `http://127.0.0.1:8000`. For a deployed frontend
or an API on another host, set `VITE_API_BASE_URL` before building, for example
`VITE_API_BASE_URL=https://api.example.org npm run build`.

## API

All endpoints use parameterized SQL (no string interpolation), so there is
no SQL-injection surface.

| method & path                                                 | body / params                                  | returns                                          |
|---------------------------------------------------------------|------------------------------------------------|--------------------------------------------------|
| `GET /health`                                                 | —                                              | `{status, db, empty}`                            |
| `GET /search?q=&label=`                                       | `q` prefix, optional `label` filter            | autocomplete node list                           |
| `POST /traverse`                                              | `{start_node, label, edge}`                    | recursive descendants along `edge` + their edges |
| `POST /path`                                                  | `{source, source_label, target, target_label}` | shortest path (BFS) between two nodes            |
| `GET /graph?node=&label=&node_id=`                            | node name and label; optional exact node ID     | nearby nodes and relationship edges              |
| `GET /verse?book_code=&chapter=&verse_number=`                | verse reference                                | the same verse across all versions               |
| `GET /reader/catalog?version_code=`                           | optional version code (defaults to `KJV`)      | books, translations, and available chapters      |
| `GET /chapter?book_code=&chapter=&version_code=`              | passage reference                              | ordered verse text and linked location nodes     |

Examples:

```bash
# all locations in Syria
curl -XPOST localhost:8000/traverse -H 'content-type: application/json' \
     -d '{"start_node":"Syria","label":"region","edge":"location_in_region"}'

# cross-version John 3:16
curl "localhost:8000/verse?book_code=JOH&chapter=3&verse_number=16"

# autocomplete
curl "localhost:8000/search?q=Jer&label=location"
```

## Project structure

```
Bibliagraphia/
├── data/                                      # canonical JSON sources (+ generated bible.db)
│   ├── books.json versions.json verses.json
│   ├── location_regions.json  regions.json
│   └── bible.db                               # generated — do not edit
├── scripts/
│   └── build_db.py                            # JSON -> data/bible.db (idempotent, bulk-loaded)
├── sql/
│   ├── init_duckdb.sql                        # schema (reference)
│   ├── load_data.sql                          # load steps (reference)
│   └── queries.sql                            # recursive CTEs (reference)
├── app/
│   └── api.py                                 # FastAPI /search /traverse /path /verse /reader /chapter /health
├── frontend/                                  # Vite + React frontend (the UI; built + served by frontend/Dockerfile)
├── tests/
│   └── test_graph.py                          # build + counts + traverse + path + verse
├── bible_loader.py                            # legacy TypeDB loader (kept for reference)
├── bible_schema.tql                           # legacy TypeDB schema (kept for reference)
├── stopwords.py                               # legacy NLP helper
├── requirements.txt                           # Dependencies
└── README.md                                  # README
```

## Design: why a single-file DuckDB graph?

This refactor replaces the TypeDB server with a **single DuckDB file** +
recursive SQL. The Bible graph is ~110k nodes and fits trivially in RAM, so
a single in-process database is the right shape here — no server, no
sharding, no network.

Key choices:

- **Two tables, `nodes` + `edges`**, with a `label` on each (the Bible
  graph is heterogeneous, so a single shared `nodes` table needs a type
  column).
- **`scripts/build_db.py`** — one idempotent script that reads the flat
  JSON sources and bulk-builds the DB with `INSERT ... SELECT` via
  `read_json_auto`.
- **Parameterized recursive SQL** for traversal (`WITH RECURSIVE`), and a
  bounded BFS for shortest path. The edges table is heterogeneous, so
  traversal filters on `label`.
- **`app/api.py`** — FastAPI exposing `/traverse`, `/path`, `/search`,
  `/health`, `/verse`, `/reader/catalog` and `/chapter` endpoints. The UI
  is the separate React frontend in `frontend/` (nginx proxies its API
  calls to this service).
- **No self-referencing foreign keys** — edges only ever connect nodes we
  have (referential integrity), which keeps partial views clean.

The win over the original TypeDB setup: **no database server to run**, a
~3-second build, parameterized queries, and a real API + frontend.

## Caveats

- `/path` finds *a* shortest path through the heterogeneous graph; because
  locations link to both verses and regions, BFS may hop through
  topically-unrelated nodes that share a region. That is a property of the
  graph, not a bug — and exactly the kind of walk the model enables.
- `data/bible.db` is generated; rebuild it with `scripts/build_db.py`.

## Tests & lint

```bash
venv/bin/python -m pytest     # 5 tests, all green
venv/bin/ruff check .          # lint (legacy TypeDB files excluded)
```

Tool config (pytest paths, ruff rules) lives in `pyproject.toml`.

The API startup compatibility check needs no source dataset:

```bash
python -m unittest discover -s tests -p test_api_startup.py
```

## License

[GNU General Public License v3.0](LICENSE)

__Ave Christus Rex__

✝️
