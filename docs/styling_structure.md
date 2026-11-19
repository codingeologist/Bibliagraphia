# Styling structure

[`frontend/src/styles.css`](frontend/src/styles.css) is the import entry point,
not a component stylesheet. Styles are organised under
[`frontend/src/styles/`](frontend/src/styles/):

- `tokens.css` maps light/dark theme variables to Tailwind colours and fonts.
  The underlying CSS variables remain available to the graph canvas and Leaflet.
- `base.css` contains document defaults and keyboard focus styling.
- `shared.css` contains reusable panel, form, button and result-row recipes.
- `landing.css`, `header.css`, `explore.css`, `graph.css`, `reader.css` and
  `map.css` own their respective feature styles. `widgets.css` owns the Explore
  dashboard and dedicated discovery-tool layouts.
- `leaflet.css` contains third-party map and popup overrides. These intentionally
  stay outside cascade layers so they can override Leaflet's unlayered stylesheet.
  Leaflet's core stylesheet is imported centrally by `styles.css` before these
  overrides, so standalone and reader maps never depend on another widget loading.
- `motion.css` contains animations and reduced-motion rules.

Prefer Tailwind utilities directly in JSX for simple elements. Use `@apply`
recipes in the relevant feature's `@layer components` for shared selectors,
responsive layouts and complex component states. Use theme utilities such as
`bg-panel`, `text-ink`, `text-muted` and `border-line` rather than duplicating
theme values. Keep semantic class hooks used by JavaScript and tests.

Utilities in one `@apply` may be reordered by Tailwind. Keep overlapping
shorthand and longhand declarations in separate `@apply` statements when their
order matters (for example `font` followed by `font-size`).
The header logo links to the home page. Open the settings menu (three-line
icon) to switch between light and dark mode; the choice is saved across pages.
Main navigation is ordered **Home, Read, Map, Relationships, Explore, MCP, About**, with
decorative icons alongside each label. It scrolls horizontally on small screens.
Home offers a **Try it with…** suggestion rotating through Ruth, Genesis, Jonah,
Matthew 7, Exodus, Luke and Joshua 3:16 on each homepage visit. The previous suggestion is saved
in browser storage to avoid consecutive repeats. Links open chapter 1, Matthew
chapter 7, or Joshua chapter 3 with verse 16 selected, in KJV and DRB side by side,
plus links to About, AI assistant setup and GitHub. After a chapter with text
loads successfully, its book, chapter, translation and comparison choices are
saved locally in the browser. The home Read card then offers **Continue reading**;
first-time visitors see **Start reading**. No account is needed.
The reader's **Book** picker shows available books grouped by testament. Type a
book name or code to filter immediately, then click a result or use arrow keys
and Enter to select it. Escape dismisses the picker without changing the passage.
Selecting a book opens its first available chapter and preserves translations.
Open `/about` for the team (Ally, Jonah, Elle, Sidd and Jonathon), playful
team icons, canonical data sources and project/map/software acknowledgements.
Dataset counts are explicitly labelled as the README baseline, not live totals.

Open `/explore` for a fixed dashboard of live preview widgets. Book suggestions,
Genesis 1:1 translations, a Genesis relationship graph, mapped place mentions
and a Genesis-to-Syria path load automatically. Each widget links to its full
tool and displays loading, empty and error states; failed previews can be retried.
`/explore/search` opens the search tool with Genesis results already loaded.
Search includes biblical figures with an **About** button for their details
and an exact-item link to Relationships.
`/explore/connections` asks "What's connected?" and "How are these connected?",
initially showing Genesis passages and its connection to Syria. Searchable
pickers offer three suggestions per type and use exact IDs, so selecting a
passage or place mention preserves its identity. The first tool accepts books,
regions and translations; the second connects any two items. Results explain
recorded links in plain English, respecting their direction, and open exact
items in Relationships. Changing a choice clears outdated results.
The `/traverse` and `/path` APIs accept exact IDs as well as names and codes.
Read/compare, graph and map widgets open `/read`, `/relationships` and `/map`.
Place preview links use `/map?place=...` to select a matching place or alias.

Open `/relationships` for a full-viewport relationship graph. It starts at
Genesis by default. The centre selector and depth control sit together at the
graph's top left. Click the selector to search across books, passages,
places, translations and regions, with up to three suggestions per type.
Choosing a suggestion automatically sets its type and exact node ID.
The three-dot menu includes **Show names** to display all graph labels.
This preference is saved in the browser; hover and zoom labels still work when
the toggle is off. Graph labels use rounded, theme-aware backgrounds, with a
node-coloured border for the centre and hovered node.
The **How far to explore** selector offers 1–20 connection steps and defaults to 3.
Each step follows one recorded connection. The selected node, type and depth are
saved in browser storage and restored when returning to Relationships.
The URL includes depth as well as node identity for refreshes and sharing;
explicit URL values take priority over saved selections. The 200-node cap still
applies, so larger depths may show a truncated neighbourhood.
Relationships uses passage-first, branch-limited expansion: each branch initially
explores up to 10 verse nodes, 5 place nodes and 5 nodes of each other type.
The sidebar lists hidden direct connections; **Show more connections** widens
the selected centre's branch while retaining the total 200-node budget.
Select another node to explore its branch. Hidden counts are distinct neighbours
not loaded in this graph, not a total count of all paths or passages.
Verse translations and repeated place mentions remain separate exact-ID nodes.
The node-type key at the bottom right includes checkboxes to hide or show each
type and its incident edges. Filtering only affects the view: loaded data,
layout positions and sidebar connections remain intact, and hidden nodes cannot
be clicked on the canvas. **Show whole view** fits visible nodes. Selecting a new
centre reveals its type if hidden. The bottom-left summary reports visible versus loaded items.

Choose a centre and depth to explore another neighbourhood. Selecting a node
redraws immediately; depth changes redraw after a short pause. No Draw graph
button is needed on this page. Node
symbols identify books, verses, translations, places and regions. Drag to pan,
scroll to zoom, click a node to explore its neighbours, or use the node picker
inside the three-dot options control beside the depth selector
for keyboard navigation. **Show whole view** and fullscreen controls sit at the top
right; the node-type key sits at the bottom right. **Show whole view** restores the view.
Node/connection counts and truncation status sit at the bottom left.
The graph includes people, labelled **People** in filters and **Person** in details.
Kinship edges between figures are drawn dashed
in the figure colour (keyed **Family relationships** in the node-type key), and the sidebar
names the recorded kind — Father of, Mother of, Child of, Sibling of, Partner of.
Underlined places in the reader open details with a **Related passages and places**
preview showing the place and up to six directly connected nodes. Its **See more**
control opens the full graph centred on that exact place mention. Selecting a graph node preserves its
identity, including passages sharing a book name. Verse details show passage
text, a **Read passage** link, and links to directly connected places and their
maps. Place details link back to the reader and map. Graph centres are saved in
the Relationships URL for reloads and sharing.
Selected node details appear beside the graph in a right-hand sidebar, stacking
below the graph on narrow screens.
The sidebar groups directly connected nodes into collapsible sections by type.
Each entry describes its relationship to the selected node and opens that exact
node when clicked. Counts describe the loaded graph, not the entire database;
capped graphs show a reminder that additional connections may exist.
The bottom-left graph summary includes the live visible/loaded node count.
Map markers are circles across the map, explorer and reader previews.
Open `/read` in the Vite app to read a Bible book. Choose a book, chapter,
translation, or jump directly to a verse; previous/next controls move between
chapters.
Book, chapter, verse and translation controls sit between Previous and Next
in one navigation bar, with grouped rows on mobile. Use the **+** button
(Compare translations) beside the translation selector
to compare versions side by side. In comparison mode, each column has its own
translation selector; selecting an already displayed version swaps columns
without duplicates. Shared book, chapter and verse controls keep columns in sync;
verses align by number (numbering can differ between translations). Unavailable
text is labelled rather than replaced with another passage. Remove columns
with their **×** buttons; comparison selections are preserved in the reader URL.
On narrow screens, scroll the comparison horizontally.
Underlined place mentions in every column open place
details with links to the graph and map. Open `/map` to browse all mappable
places; select a marker to read the passages that mention it.
Every item in the reader's Related passages and places preview links to Relationships
centred on that exact item, retaining passage translation and place-mention IDs.
People named in a verse have a pink, solid-underlined button opening person
details beside the passage. The drawer shows the recorded description, a related
items preview and **Family relationships** (kinship) when available. A separate
`/reader/person-relations` lookup returns all direct relatives, independently of
the graph preview's node cap, with stored relationship kinds and directions.
Select a relative to open their details in the same drawer. An explicit
empty state appears when no family relationships are recorded; no
relationships are invented. Places retain their dotted highlight and drawer.
Selecting a verse number opens passage text and related items in the same
details drawer, retaining the exact translation of that comparison column.
Preview items and **Explore all connections** link to Relationships.
Reader previews include at least one item of every directly connected type
available, then fill remaining spaces up to six. Balanced graph loading prevents
passages from crowding out other types. The **+ See more** control and each item's
plus-marked link open Relationships centred on its exact ID. The full view retains
its normal limits and **Show more connections** controls; missing types are not invented.
People highlights use recorded STEP Bible verse links and canonical names or
explicit spelling aliases, never general search keywords. Only names present
in the people dataset are highlighted; pronouns are not. Each comparison
column uses its own translation's verse links.
Use the map's search box to find places by name or alternative name. Choose an
autocomplete suggestion (or use the arrow keys and Enter) to zoom to that place
and open its passages.
The expand control on a place's preview map smoothly grows it into the full
map, preserving its centre and zoom. Browser Back restores the passage and
place sidebar. The animation uses the View Transitions API; unsupported
browsers and reduced-motion preferences retain normal page navigation.

Open the Vite URL shown in the terminal (normally `http://localhost:5173`).
Vite proxies API requests to `http://127.0.0.1:8000`. For a deployed frontend
or an API on another host, set `VITE_API_BASE_URL` before building, for example
`VITE_API_BASE_URL=https://api.example.org npm run build`.