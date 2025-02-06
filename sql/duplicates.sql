SET pg_index_stats.stattypes = 'mcv, distinct, deps'; -- Set before the lib has loaded, just warn
SET pg_index_stats.columns_limit = 5;

-- Use check_estimated_rows from previous test

CREATE TABLE is_test(x1 integer, x2 integer, x3 integer, x4 integer);
INSERT INTO is_test (x1,x2,x3,x4)
  SELECT x%10,x%10,x%10,x%10 FROM generate_series(1,1E3) AS x;
VACUUM ANALYZE is_test;

CREATE INDEX ist_idx0 on is_test (x1,x2,x3,x4);
SHOW pg_index_stats.stattypes; --incorrect value still exists
CREATE EXTENSION pg_index_stats;
SHOW pg_index_stats.stattypes; -- should be default value
-- Generate statistics on already existed indexes. Mind default values.
SELECT pg_index_stats_build('ist_idx0');

CREATE INDEX ist_idx1 on is_test (x1,x2,x3,x4);
CREATE INDEX ist_idx2 on is_test (x2,x1,x3,x4);
\dX
SELECT pg_index_stats_rebuild(); -- must not have more duplicated stats
\dX

-- TODO: Should not create statistics bacause of duplicates
SELECT pg_index_stats_build('ist_idx0', 'mcv');
\dX

DROP TABLE is_test CASCADE;
DROP EXTENSION pg_index_stats;

