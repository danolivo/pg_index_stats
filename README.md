# pg_index_stats
Lightweight extension to PostgreSQL. Provide manual and automatic machinery for generating extended statistics based on definition of indexes.

## Interface
* Function `build_extended_statistic(idxname)` - manually create extended statistics (multivariate so far) on an expression defined by formula of the index `idxname`.
* Boolean GUC `pg_index_stats.auto` - enable (by default) auto generation of extended statistics on each new index.

## Notes
Each created statistics depends on the index and the `pg_index_stats` extension. Hence, dropping an index you remove corresponding auto-generated extended statistics. Dropping `pg_index_stats` extension you will remove all auto-generated statistics in the database.
