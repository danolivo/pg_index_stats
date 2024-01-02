/* contrib/pg_index_stats/pg_index_stats--0.1.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION pg_index_stats" to load this file. \quit

--
-- Manually create extended statistics using an index as a definition of
-- expression
--
CREATE FUNCTION pg_catalog.build_extended_statistic(
											idxname text,
											mode text DEFAULT 'multivariate')
RETURNS boolean
AS 'MODULE_PATHNAME', 'build_extended_statistic'
LANGUAGE C VOLATILE STRICT;
