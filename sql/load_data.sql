-- Reference SQL for loading the Bibliagraphia graph from the canonical
-- JSON sources. In practice this is done in Python by scripts/build_db.py
-- (which can INSERT directly); this file documents the same steps as SQL
-- for reference and reproducibility. Run after init_duckdb.sql against an
-- empty data/bible.db.

-- The loader writes one row per node into `nodes`, computing the synthetic
-- id, pulling common query columns out, and putting the rest in `attrs`:
--
--   versions  -> id 'version:<code>',           attrs {name, full_name}
--   books     -> id 'book:<code>',              attrs {testament, vulgate, rheims, kjv, note}
--   verses    -> id 'verse:<ver>:<bk>:<ch>:<vs>', attrs {book, text}
--   regions   -> id 'region:<name>',            attrs {keywords, description}
--   locations -> id 'location:<name>:<bk>:<ch>:<vs>', attrs {secondary_name, testament,
--                                                          rheims, vulgate, kjv,
--                                                          rheims_text, vulgate_text, kjv_text,
--                                                          note, latitude, longitude}
--
-- Edges (only between nodes that exist — referential integrity):
--   book          --verse_in_book-->    verse        (matching book_code)
--   version       --verse_in_version--> verse        (matching version_code)
--   region        --location_in_region--> location   (matching region name)
--   verse         --location_in_verse--> location    (location mentions this verse;
--                                                      created per-version for each
--                                                      version whose verse exists)

-- Verify counts after loading (run from Python or the DuckDB CLI):
SELECT 'versions',  COUNT(*) FROM nodes WHERE label='version';
SELECT 'books',     COUNT(*) FROM nodes WHERE label='book';
SELECT 'verses',    COUNT(*) FROM nodes WHERE label='verse';
SELECT 'regions',   COUNT(*) FROM nodes WHERE label='region';
SELECT 'locations', COUNT(*) FROM nodes WHERE label='location';
SELECT 'verse_in_book',    COUNT(*) FROM edges WHERE label='verse_in_book';
SELECT 'verse_in_version', COUNT(*) FROM edges WHERE label='verse_in_version';
SELECT 'location_in_region', COUNT(*) FROM edges WHERE label='location_in_region';
SELECT 'location_in_verse',  COUNT(*) FROM edges WHERE label='location_in_verse';
