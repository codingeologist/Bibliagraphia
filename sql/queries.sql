-- Recursive traversal & path queries for the Bibliagraphia single-file
-- DuckDB graph. All queries are parameterized (bind `?` placeholders) in
-- the FastAPI layer; here they are shown with illustrative literals.

-- 1. All descendants of a node along a given edge label (recursive fan-out
--    from a container toward leaves). Example: every verse in a book, or
--    every location in a region.
WITH RECURSIVE descendants AS (
    SELECT to_id AS node_id, 1 AS level
    FROM edges
    WHERE from_id = 'book:GEN' AND label = 'verse_in_book'
    UNION ALL
    SELECT e.to_id, d.level + 1
    FROM edges e
    JOIN descendants d ON e.from_id = d.node_id
    WHERE e.label = 'verse_in_book'
)
SELECT n.* FROM descendants d
JOIN nodes n ON n.id = d.node_id
ORDER BY d.level, n.chapter, n.verse_number;

-- 2. Shortest path between two nodes. NOTE: a true shortest-path with a
--    visited set is expensive in a recursive CTE over 235k edges (the
--    per-step path-array check is exponential). In the FastAPI layer
--    (app/api.py) /path loads the adjacency once and runs a plain Python
--    BFS instead. The SQL below is the *one-hop* neighbour lookup that
--    the BFS expands with.
SELECT e.label, CASE WHEN e.from_id = 'verse:DRB:GEN:1:1' THEN e.to_id ELSE e.from_id END AS neighbour
FROM edges e
WHERE e.from_id = 'verse:DRB:GEN:1:1' OR e.to_id = 'verse:DRB:GEN:1:1';

-- 3. Autocomplete: nodes of a given label whose name starts with a prefix.
SELECT id, label, name FROM nodes
WHERE label = 'location' AND name ILIKE 'Jer%'
ORDER BY name LIMIT 20;

-- 4. Cross-version verse comparison: given a (book, chapter, verse),
--    return all three versions' verse nodes + text.
SELECT n.id, n.version_code, n.chapter, n.verse_number, n.attrs->>'text' AS text
FROM nodes n
WHERE n.label = 'verse' AND n.book_code = 'JOH'
  AND n.chapter = 3 AND n.verse_number = 16
ORDER BY n.version_code;

-- 5. Locations mentioned in a region with their verse references.
SELECT l.id, l.name, l.book_code, l.chapter, l.verse_number,
       l.attrs->>'latitude' AS latitude, l.attrs->>'longitude' AS longitude
FROM nodes r
JOIN edges e ON e.from_id = r.id AND e.label = 'location_in_region'
JOIN nodes l ON l.id = e.to_id
WHERE r.label = 'region' AND r.name = 'Syria'
ORDER BY l.book_code, l.chapter, l.verse_number;

-- 6. Person-to-person co-occurrence ("person-passage-person"): figures
-- that appear in the same passage. The link is a *path* through the
-- shared verses (verse -> both figures), not an edge — join on the VERSE
-- end (from_id) of figure_in_verse. Joining on the figure end explodes
-- into tens of millions of pairs and OOMs the build. The verse ids are
-- stripped of their version prefix so counts are deduplicated across
-- versions.
SELECT n1.name AS figure_a, n2.name AS figure_b,
       COUNT(DISTINCT regexp_replace(e1.from_id, '^verse:[A-Z]+:', '')) AS shared_verses
FROM edges e1
JOIN edges e2 ON e1.from_id = e2.from_id AND e1.to_id < e2.to_id
JOIN nodes n1 ON n1.id = e1.to_id
JOIN nodes n2 ON n2.id = e2.to_id
WHERE e1.label = 'figure_in_verse' AND e2.label = 'figure_in_verse'
GROUP BY 1, 2
ORDER BY shared_verses DESC
LIMIT 10;

-- 7. Person-to-person kinship (figure_relative_of edges from TIPNR
-- genealogy): relatives of a figure, with the relationship kind.
-- father/mother/parent edges run parent -> child; sibling/partner edges
-- are undirected (either end may be the figure). Note: with a JSON-typed
-- attrs column the `->>'field'` operator can be shadowed by column name
-- resolution in join scopes - use json_extract_string() instead.
SELECT CASE WHEN e.from_id = 'figure:David' THEN n2.name ELSE n1.name END AS relative,
       json_extract_string(e.attrs, 'relationship') AS relationship,
       CASE WHEN e.from_id = 'figure:David' THEN 'down' ELSE 'up/sideways' END AS direction
FROM edges e
JOIN nodes n1 ON n1.id = e.from_id
JOIN nodes n2 ON n2.id = e.to_id
WHERE e.label = 'figure_relative_of'
  AND 'figure:David' IN (e.from_id, e.to_id)
ORDER BY direction, relative;

-- 8. Descendants of a figure: recursive walk down the family tree along
-- parent -> child edges. (Skip the DISTINCT trick for the general case;
-- TIPNR's tree has no cycles.)
WITH RECURSIVE descendants AS (
    SELECT to_id AS node_id, 1 AS level
    FROM edges
    WHERE from_id = 'figure:Abraham' AND label = 'figure_relative_of'
      AND json_extract_string(attrs, 'relationship') IN ('father', 'mother', 'parent')
    UNION ALL
    SELECT e.to_id, d.level + 1
    FROM edges e
    JOIN descendants d ON e.from_id = d.node_id
    WHERE e.label = 'figure_relative_of'
      AND json_extract_string(e.attrs, 'relationship') IN ('father', 'mother', 'parent')
)
SELECT n.name, d.level
FROM descendants d
JOIN nodes n ON n.id = d.node_id
ORDER BY d.level, n.name;
