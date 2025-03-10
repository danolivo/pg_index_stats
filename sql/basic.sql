CREATE EXTENSION pg_index_stats;
SHOW pg_index_stats.stattypes;
SET pg_index_stats.columns_limit = 0;
SET pg_index_stats.stattypes = ''; -- ERROR

CREATE TABLE test(x int, y numeric);
CREATE INDEX abc1 ON test(x,(y*y));
CREATE INDEX abc2 ON test(x,y);

RESET pg_index_stats.columns_limit;
SELECT pg_index_stats_build('abc2');
\d test
SELECT pg_index_stats_build('abc1');
\d test
REINDEX INDEX abc2;
REINDEX INDEX CONCURRENTLY abc1;
\d test
DROP INDEX abc2;
\d test

DROP EXTENSION pg_index_stats;
\d test

CREATE TABLE abc(x1 integer, x2 text, x3 name, x4 bigint, x5 text);
INSERT INTO abc SELECT x, 'abc' || x, 'def' || -x, x*100, 'constant'
FROM generate_series(1,1000) AS x;

-- Check limits
SET pg_index_stats.stattypes = 'all';
SET pg_index_stats.columns_limit = 5;
CREATE INDEX abc_idx ON abc(x2, x4, (x1*x1/x4), x5, x4, x3, x2, x1);
-- Must generate all multivariate statistics without preliminary execution of
-- CREATE EXTENSION statement - it just provides some service routines.
-- Must be only on the first five index expressions
\d abc
CREATE EXTENSION pg_index_stats;
SET pg_index_stats.columns_limit = 8;
SELECT pg_index_stats_build('abc_idx');
-- Must see one more statistic because of new limit (including x1)
-- Also, redundant ndistinct statistic will be removed because of covering stat
\dX
SELECT count(*) FROM pg_description
WHERE description LIKE 'pg_index_stats%';

-- Check removing only auto-generated statistics.
CREATE STATISTICS manually_built ON x1,x3,(x2||'2.') FROM abc;
SELECT * FROM pg_index_stats_remove();
\dX
SELECT count(*) FROM pg_description
WHERE description LIKE 'pg_index_stats%';

-- Test for duplicated statistics. We must create extstat on the same list of
-- expressions but with MCV excluded.
CREATE STATISTICS manually_built_1 (mcv) ON x2, x4, (x1*x1/x4), x5, x3, x1 FROM abc;
SELECT pg_index_stats_build('abc_idx');
\dX

DROP TABLE abc, test CASCADE;
RESET pg_index_stats.columns_limit;
RESET pg_index_stats.stattypes;
DROP EXTENSION pg_index_stats;
