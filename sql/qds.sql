CREATE EXTENSION pg_index_stats;

CREATE TABLE qds1 (x integer, y integer);
INSERT INTO qds1 (x,y)
  SELECT value % 72, value % 36 FROM generate_series(1, 10000) AS value;
INSERT INTO qds1 (x,y)
  SELECT value % 250 + 100, value % 250 + 100
  FROM generate_series(1, 1000) AS value;
VACUUM ANALYZE qds1;

SELECT * FROM check_estimated_rows('
  SELECT x,y FROM qds1 WHERE x = 1 AND y = 1;');
EXPLAIN (COSTS OFF, EXTSTAT_CANDIDATES ON)
SELECT x,y FROM qds1 WHERE x = 1 AND y = 1; -- ERROR, needs ANALYZE

EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF, BUFFERS OFF, EXTSTAT_CANDIDATES ON)
SELECT x,y FROM qds1 WHERE x = 1 AND y = 1;

SELECT * FROM check_estimated_rows('
  SELECT x,y FROM qds1 WHERE x = 1 AND y = 2;');
EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF,
		 BUFFERS OFF, EXTSTAT_CANDIDATES ON)
SELECT x,y FROM qds1 WHERE x = 1 AND y = 2
; -- Nothing to recommend - too little tuples produced

SELECT * FROM check_estimated_rows('
  SELECT x,y FROM qds1 WHERE x = 101 AND y = 101;');
EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF,
		 BUFFERS OFF, EXTSTAT_CANDIDATES ON)
SELECT x,y FROM qds1 WHERE x = 101 AND y = 101;

SELECT * FROM check_estimated_rows('
  SELECT x,y FROM qds1 WHERE x = 71 AND y % 35 = 0');
EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF,
		 BUFFERS OFF, EXTSTAT_CANDIDATES ON)
SELECT x,y FROM qds1 WHERE x = 72 AND y % 36 = 0
; -- Combine expression and column

SELECT * FROM check_estimated_rows('
  SELECT x,y FROM qds1 WHERE x % 71 = 0 AND x = 71 AND y % 35 = 0');
EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF,
		 BUFFERS OFF, EXTSTAT_CANDIDATES ON)
SELECT x,y FROM qds1 WHERE x % 71 = 0 AND x = 71 AND y % 35 = 0
; -- there are couple of expressions with a column

SELECT * FROM check_estimated_rows('
  SELECT x,y FROM qds1 WHERE x % 71 = 0 AND x = 71');
EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF,
		 BUFFERS OFF, EXTSTAT_CANDIDATES ON)
SELECT x,y FROM qds1 WHERE x % 71 = 0 AND x = 71
; -- Same column, but expression and column itself

SELECT * FROM check_estimated_rows('
  SELECT x,y FROM qds1 WHERE x IN (71) AND y > 33 AND y < 37');
EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF,
		 BUFFERS OFF, EXTSTAT_CANDIDATES ON)
SELECT x,y FROM qds1 WHERE x IN (71) AND y > 33 AND y < 37
; -- Two columns, but one of them is inequality. XXX: is this OK for all stat types?

SELECT * FROM check_estimated_rows('
  SELECT x,y FROM qds1 WHERE x > 70 AND x < 72 AND x = 71');
EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF,
		 BUFFERS OFF, EXTSTAT_CANDIDATES ON)
SELECT x,y FROM qds1 WHERE x > 70 AND x < 72 AND x = 71
; -- The same column only, couple inequalities, no extended statistics

-- TODO: Need to detect that we gather candidates recursively too
CREATE FUNCTION recursive_execution()
RETURNS bool
AS $func$
BEGIN
  PERFORM x,y FROM qds1 WHERE x IN (71) AND y > 33 AND y < 37;
  RETURN true;
END;
$func$ LANGUAGE PLPGSQL;
EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF,
		 BUFFERS OFF, EXTSTAT_CANDIDATES ON)
SELECT * FROM recursive_execution();

-- Before we had a recommendation to build statistics on the (x,y).
-- Now we have a stat, so no recommendations should be provided.
 -- Unfortunately, it only works after an analyse. We may avoid it - see the
-- get_relation_statistics routine, but will add more overhead. XXX: Is this
-- worth it?
CREATE STATISTICS qds1_x_y ON x,y FROM qds1;
ANALYZE qds1;
EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF,
		 BUFFERS OFF, EXTSTAT_CANDIDATES ON)
SELECT x,y FROM qds1 WHERE x IN (71) AND y > 33 AND y < 37;

DROP FUNCTION recursive_execution;
DROP EXTENSION pg_index_stats;
