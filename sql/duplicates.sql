SET pg_index_stats.stattypes = 'mcv, distinct, deps'; -- Set before the lib has loaded, just warn
SET pg_index_stats.columns_limit = 5;

-- Use check_estimated_rows from previous test

CREATE TABLE is_test(x1 integer, x2 integer, x3 integer, x4 integer);
INSERT INTO is_test (x1,x2,x3,x4)
  SELECT x%10,x%10,x%10,x%10 FROM generate_series(1,1E3) AS x;
VACUUM ANALYZE is_test;

CREATE INDEX ist_idx0 on is_test (x1,x2,x3,x4);
CREATE INDEX ist_idx_1 on is_test (x2, (x3*x3));
SHOW pg_index_stats.stattypes; --incorrect value still exists
CREATE EXTENSION pg_index_stats;
SHOW pg_index_stats.stattypes; -- should be default value
-- Generate statistics on already existed indexes. Mind default values.
SELECT pg_index_stats_build('ist_idx0');
SELECT pg_index_stats_build('ist_idx_1', 'mcv, dependencies');

CREATE INDEX ist_idx1 on is_test (x1,x2,x3,x4);
CREATE INDEX ist_idx2 on is_test (x2,x1,x3,x4);
\dX
SELECT pg_index_stats_rebuild(); -- must not have more duplicated stats
\dX

-- TODO: Should not create statistics bacause of duplicates
SELECT pg_index_stats_build('ist_idx0', 'mcv');
SELECT pg_index_stats_build('ist_idx_1', 'mcv'); -- reject, already have same MCV
\dX

-- Test covering statistics
SET pg_index_stats.columns_limit = 3;
SELECT pg_index_stats_build('ist_idx0', 'mcv, ndistinct, dependencies');
-- Create one new statistic (MCV, dependencies)
\dX
SET pg_index_stats.columns_limit = 2;
SELECT pg_index_stats_build('ist_idx0', 'mcv, ndistinct, dependencies');
-- Create one new statistic with a MCV only
\dX

-- New stat covers old one
SELECT pg_index_stats_remove();
SET pg_index_stats.columns_limit = 2;
SELECT pg_index_stats_build('ist_idx0', 'ndistinct, dependencies');
SELECT pg_index_stats_build('ist_idx0', 'mcv');
\dX
SET pg_index_stats.columns_limit = 3;
SELECT pg_index_stats_build('ist_idx0', 'mcv, ndistinct, dependencies');
-- Create one new statistic with all the types and remove an old one
\dX

-- Change existed stats on arrival of a covering one
SELECT * FROM pg_index_stats_remove();
SET pg_index_stats.columns_limit = 3;
CREATE INDEX ist_idx3 on is_test (x3,x4);
SELECT pg_index_stats_build('ist_idx0', 'mcv, ndistinct, dependencies');
\dX
SET pg_index_stats.columns_limit = 5;
SELECT pg_index_stats_build('ist_idx0', 'mcv, ndistinct, dependencies');
\dX

-- Check compactifying feature
SET pg_index_stats.compactify = 'off';
SET pg_index_stats.columns_limit = 2; -- Just do it quickly
SELECT pg_index_stats_rebuild(); -- must create duplicated stats
\dX

RESET pg_index_stats.compactify;
RESET pg_index_stats.columns_limit;
DROP TABLE is_test CASCADE;
DROP EXTENSION pg_index_stats;

