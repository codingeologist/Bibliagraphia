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
