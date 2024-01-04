/* contrib/pg_index_stats/pg_index_stats--0.1.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION pg_index_stats" to load this file. \quit

--
-- Manually create extended statistics using an index as a definition of
-- expression
--
CREATE FUNCTION pg_index_stats_build(idxname text,
									 mode text DEFAULT 'multivariate')
RETURNS boolean
AS 'MODULE_PATHNAME', 'pg_index_stats_build'
LANGUAGE C VOLATILE STRICT;
