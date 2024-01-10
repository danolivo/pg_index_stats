CREATE EXTENSION pg_index_stats;
SET pg_index_stats.mode = 'disabled';

CREATE TABLE test(x int, y numeric);
CREATE INDEX abc1 ON test(x,(y*y));
CREATE INDEX abc2 ON test(x,y);
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

CREATE TEMP TABLE abc(x1 integer, x2 text, x3 name, x4 bigint, x5 text);
INSERT INTO abc SELECT x, 'abc' || x, 'def' || -x, x*100, 'constant'
FROM generate_series(1,1000) AS x;

-- Check limits
SET pg_index_stats.mode = 'all';
CREATE INDEX abc_idx ON abc(x2, x4, (x1*x1/x4), x5, x4, x3, x2, x1);
-- Must generate both multivariate and univariate statistics
-- Multivariate one should be only on the first five index expressions
\d abc
CREATE EXTENSION pg_index_stats;
SET pg_index_stats.columns_limit = 8;
SELECT pg_index_stats_build('abc_idx');
 -- Must see one more statistic because of new limit (including x1)
 -- Don't need new univariate statistics because we already have generated it
\d abc

DROP TABLE abc, test CASCADE;
DROP EXTENSION pg_index_stats;
