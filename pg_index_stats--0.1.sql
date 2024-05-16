/* contrib/pg_index_stats/pg_index_stats--0.1.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION pg_index_stats" to load this file. \quit

--
-- Manually create extended statistics using an index as a definition of
-- expression
--
CREATE FUNCTION pg_index_stats_build(idxname text,
									 mode text DEFAULT 'ndistinct')
RETURNS boolean
AS 'MODULE_PATHNAME', 'pg_index_stats_build'
LANGUAGE C VOLATILE STRICT;

--
-- Remove all auto-generated statistics
-- Return number of deleted statistics
--
CREATE FUNCTION pg_index_stats_remove() RETURNS integer AS $$
WITH deleted AS (
  DELETE FROM pg_statistic_ext s
  WHERE
    s.oid IN (SELECT d.objid FROM pg_depend d
      JOIN pg_class c ON (d.refobjid = c.oid)
	    JOIN pg_namespace n ON (c.relnamespace = n.oid)
    WHERE
      c.relkind = 'i' AND -- XXX: Should be rewritten if statistics will be allowed over indexes
      n.nspname NOT IN ('pg_catalog', 'pg_toast', 'information_schema')
    )
  RETURNING s.oid
),
delobjs AS (
  DELETE FROM pg_description USING deleted WHERE objoid = deleted.oid RETURNING *
),
deldeps AS (
  DELETE FROM pg_depend USING deleted WHERE objid = deleted.oid RETURNING *
) SELECT count(*) FROM deleted;
$$ LANGUAGE SQL PARALLEL SAFE STRICT;

--
-- Generate extended statistics on all non-system indexes
-- Return number of rebuilt statistics
--
CREATE FUNCTION pg_index_stats_rebuild() RETURNS integer AS $$
DECLARE
 result integer;
BEGIN
  -- Pre-cleanup
  PERFORM pg_index_stats_remove();

  SELECT count(*) FROM (
    SELECT pg_index_stats_build((c.oid::regclass)::text)
	FROM pg_class c, pg_namespace n
    WHERE
      c.relkind = 'i' AND
      c.relnamespace = n.oid AND
      n.nspname NOT IN ('pg_catalog', 'pg_toast', 'information_schema')
  ) AS q1
  INTO result;

  RETURN result;
END;
$$ LANGUAGE PLPGSQL PARALLEL SAFE STRICT;
