
-- Pass through all the relations in the database and
CREATE FUNCTION pg_catalog.build_extended_statistic(idxname text)
RETURNS boolean
AS 'MODULE_PATHNAME', 'build_extended_statistic'
LANGUAGE C VOLATILE STRICT;