# pg_index_stats
Lightweight extension to PostgreSQL. Provide manual and automatic machinery for generating extended statistics based on definition of indexes.
According to the [postgres docs](https://www.postgresql.org/docs/current/planner-stats.html#PLANNER-STATS-EXTENDED) it is impractical to compute multivariate statistics automatically. Our conjecture here is that index structure reflects that a specific set of columns and extensions is most frequently used for extracting data, and it is critical to build optimal query plans when combinations of these columns are involved.

## Interface
* Function `build_extended_statistic(idxname)` - manually create extended statistics (multivariate so far) on an expression defined by formula of the index `idxname`.
* Boolean GUC `pg_index_stats.auto` - enable (by default) auto generation of extended statistics on each new index.

## Notes
Each created statistics depends on the index and the `pg_index_stats` extension. Hence, dropping an index you remove corresponding auto-generated extended statistics. Dropping `pg_index_stats` extension you will remove all auto-generated statistics in the database.

# TODO
* Univariate statistics on the index tuple descriptor.
* In the case of indexes intersection, combine their **multivariate** statistics.
* After CREATE EXTENSION scan the database and create statistics whenever possible.
* Restrict the shared library activity by databases where the extension was created.
