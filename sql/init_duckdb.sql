-- Bibliagraphia single-file DuckDB graph schema.
--
-- The graph is heterogeneous (5 node types, 4 edge types), so both tables
-- carry a `label`/`type` column. We deliberately do NOT add self-
-- referencing or cross-table foreign keys: edges are only ever created
-- between nodes actually present (see load_data.sql / build_db.py).

-- Nodes: one row per graph vertex. `id` is a stable synthetic key of the
-- form "<label>:<natural-key>" (e.g. "book:GEN", "verse:DRB:GEN:1:1",
-- "location:Abana:2KI:5:12", "region:Syria"). `label` is the node type.
-- `attrs` holds the type-specific columns as a JSON blob so a single
-- table can store all five node types. The common query columns
-- (name, book_code, chapter, verse_number, version_code) are also pulled
-- out as real columns for fast indexed lookup without deserialising JSON.
CREATE TABLE IF NOT EXISTS nodes (
    id            VARCHAR PRIMARY KEY,
    label         VARCHAR NOT NULL,
    name          VARCHAR,            -- human-readable name for search/display
    version_code  VARCHAR,            -- verses only
    book_code     VARCHAR,            -- verses + locations
    chapter       INTEGER,            -- verses + locations
    verse_number  INTEGER,            -- verses + locations
    attrs         JSON                -- all other type-specific attributes
);

-- Edges: directed from->to with a relation label. The four relations from
-- the original TypeDB schema become four edge labels:
--   verse_in_book       book -> verse        (a book contains a verse)
--   verse_in_version    version -> verse     (a version contains a verse)
--   location_in_verse   verse -> location     (a verse mentions a location)
--   location_in_region  region -> location    (a region contains a location)
-- (direction chosen so that recursive traversals fan out from the
-- "container" side — book/version/region — toward the leaves.)
CREATE TABLE IF NOT EXISTS edges (
    from_id   VARCHAR NOT NULL,
    to_id     VARCHAR NOT NULL,
    label     VARCHAR NOT NULL,
    PRIMARY KEY (from_id, to_id, label)
);

-- Indexes for the traversal patterns we actually run.
CREATE INDEX IF NOT EXISTS idx_nodes_label       ON nodes(label);
CREATE INDEX IF NOT EXISTS idx_nodes_book_code   ON nodes(book_code);
CREATE INDEX IF NOT EXISTS idx_nodes_version     ON nodes(version_code);
CREATE INDEX IF NOT EXISTS idx_nodes_name        ON nodes(name);
CREATE INDEX IF NOT EXISTS idx_edges_from        ON edges(from_id);
CREATE INDEX IF NOT EXISTS idx_edges_to          ON edges(to_id);
CREATE INDEX IF NOT EXISTS idx_edges_label       ON edges(label);
CREATE INDEX IF NOT EXISTS idx_edges_from_label  ON edges(from_id, label);
