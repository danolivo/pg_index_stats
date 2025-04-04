CREATE EXTENSION pg_index_stats;

CREATE TABLE sc_a(x integer, y text) WITH (autovacuum_enabled = off);

-- Without data no statistics created
EXPLAIN (COSTS OFF, STAT ON)
SELECT * FROM sc_a WHERE x=1 AND y LIKE 'a';
EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, BUFFERS OFF, STAT ON, SUMMARY OFF)
SELECT * FROM sc_a WHERE x=1 AND y LIKE 'a';
VACUUM ANALYZE sc_a;
EXPLAIN (COSTS OFF, STAT ON)
SELECT * FROM sc_a WHERE x=1 AND y LIKE 'a';
EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, BUFFERS OFF, STAT ON, SUMMARY OFF)
SELECT * FROM sc_a WHERE x=1 AND y LIKE 'a';

-- Add some data and watch statistics - but only after an analyse.
INSERT INTO sc_a(x,y) (SELECT gs, 'abc'||gs%10 FROM generate_series(1,100) AS gs);
EXPLAIN (COSTS OFF, STAT ON)
SELECT * FROM sc_a WHERE x=1 AND y LIKE 'a';
VACUUM ANALYZE sc_a;
EXPLAIN (COSTS OFF, STAT ON)
SELECT * FROM sc_a WHERE x=1 AND y LIKE 'a';
EXPLAIN (COSTS OFF, STAT ON)
SELECT * FROM sc_a s1 JOIN sc_a s2 ON true
WHERE s1.x=1 AND s2.y LIKE 'a';

-- Check format
EXPLAIN (COSTS OFF, STAT ON, FORMAT JSON)
SELECT * FROM sc_a WHERE x=1 AND y LIKE 'a';

DROP EXTENSION pg_index_stats;
