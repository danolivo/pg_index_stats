# pg_index_stats
Lightweight extension to PostgreSQL. Provide manual and automatic machinery for generating extended statistics based on definition of indexes.
According to the [postgres docs](https://www.postgresql.org/docs/current/planner-stats.html#PLANNER-STATS-EXTENDED) it is impractical to compute multivariate statistics automatically. Our conjecture here is that index structure reflects that a specific set of columns and extensions is most frequently used for extracting data, and it is critical to build optimal query plans when combinations of these columns are involved.

## Interface
* Function `build_extended_statistic(idxname)` - manually create extended statistics on an expression defined by formula of the index `idxname`.
* Enum GUC `pg_index_stats.mode` - can be set to `all`, `disabled`, `multivariate` or `univariate` values. By default, set into generation of multivariate statistics.
* Integer GUC `pg_index_stats.columns_limit` - number of first columns of an index which will be involved in multivariate statistics creation (**default 5**).

## Notes
* Each created statistics depends on the index and the `pg_index_stats` extension. Hence, dropping an index you remove corresponding auto-generated extended statistics. Dropping `pg_index_stats` extension you will remove all auto-generated statistics in the database.
* Although multivariate case is trivial (it will be used be the core after an ANALYZE finished), univariate one (histogram and MCV on the ROW()) isn't used by the core and we should invent something - can we implement some code under the get_relation_stats_hook and/or get_index_stats_hook ?

# TODO
* In the case of indexes intersection, combine their **multivariate** statistics.
* ? After CREATE EXTENSION scan the database and create statistics whenever possible.
* ? Restrict the shared library activity by databases where the extension was created.
* ? Extend modes: maybe user wants only ndistincts or relatively lightweight column dependencies?

# Second Thoughts
* Could we introduce some automatization here? For example, generate functional dependencies only for cases when real dependency factor on columns more than a predefined value?
* As I can see, univariate statistics on a ROW(Index Tuple Descriptor) look cheaper and contain a whole set of columns covered by histogram and MCV. So, when the user creates an index because he knows he would use queries with clauses utilizing the index, it would be more profitable to use such statistics. Unfortunately, core PostgreSQL doesn't allow estimations on a group of columns; it is possible only for extended statistics. So, univariate statistics could be utilized only in **PostgreSQL forks** for now.
