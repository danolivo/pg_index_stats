CREATE EXTENSION pg_index_stats;
SET pg_index_stats.mode = 'disabled';

CREATE TABLE test(x int, y numeric);
CREATE INDEX abc1 ON test(x,(y*y));
CREATE INDEX abc2 ON test(x,y);
SELECT build_extended_statistic('abc2');
\d test
SELECT build_extended_statistic('abc1');
\d test
REINDEX INDEX abc2;
REINDEX INDEX CONCURRENTLY abc1;
\d test
DROP INDEX abc2;
\d test

DROP EXTENSION pg_index_stats;
\d test
