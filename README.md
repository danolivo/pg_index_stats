# pg_index_stats
Lightweight extension to PostgreSQL. Provide manual and automatic machinery for generating extended statistics based on definition of indexes.
According to the [postgres docs](https://www.postgresql.org/docs/current/planner-stats.html#PLANNER-STATS-EXTENDED) it is impractical to compute multivariate statistics automatically. Our conjecture here is that index structure reflects that a specific set of columns and extensions is most frequently used for extracting data, and it is critical to build optimal query plans when combinations of these columns are involved.

## Interface
* Enum GUC `pg_index_stats.mode` - can be set to `all`, `disabled`, `multivariate` or `univariate` values. By default, set into generation of multivariate statistics.
* Integer GUC `pg_index_stats.columns_limit` - number of first columns of an index which will be involved in multivariate statistics creation (**default 5**).
* Function `pg_index_stats_build(idxname, mode DEFAULT 'multivariate')` - manually create extended statistics on an expression defined by formula of the index `idxname`.
* Function `pg_index_stats_remove()` - remove all previously automatically generated statistics.
* Function `pg_index_stats_rebuild()` - remove old and create new extended statistics over non-system indexes existed in the database. 

## Notes
* Each created statistics depends on the index and the `pg_index_stats` extension. Hence, dropping an index you remove corresponding auto-generated extended statistics. Dropping `pg_index_stats` extension you will remove all auto-generated statistics in the database.
* Although multivariate case is trivial (it will be used by the core natively after an ANALYZE finished), univariate one (histogram and MCV on the ROW()) isn't used by the core and we should invent something - can we implement some code under the get_relation_stats_hook and/or get_index_stats_hook ?
* For clarity, the extension adds to auto-geenrated statistics comments likewise 'pg_index_stats - multivariate statistics'. To explore all generated statistics an user can execute simple query like:
```
SELECT s.oid,s.stxname,d.description
FROM  pg_statistic_ext s JOIN pg_description d ON (s.oid = d.objoid)
WHERE description LIKE 'pg_index_stats%';
```

## Research
The main goal of this work is to understand the real benefits of extended statistics. Here we have two directions:
* Is it possible to make it more compact? Adding new statistics we can have different relationships: duplicated, overlapping, intersecting, including. So, we should try to look into all these cases and find the best solution to optimize storage and ANALYZE time.
* How to use extended statistics. Do we need additional core tweaks to make it more effective?

# TODO
* In the case of indexes intersection, combine their **multivariate** statistics into some meta statistics.
* ? Restrict the shared library activity by databases where the extension was created.
* ? Extend modes: maybe user wants only ndistincts or relatively lightweight column dependencies?

# Second Thoughts
* Could we introduce some automatization here? For example, generate functional dependencies only for cases when real dependency factor on columns more than a predefined value?
* As I can see, univariate statistics on a ROW(Index Tuple Descriptor) look cheaper and contain a whole set of columns covered by histogram and MCV. So, when the user creates an index because he knows he would use queries with clauses utilizing the index, it would be more profitable to use such statistics. Unfortunately, core PostgreSQL doesn't allow estimations on a group of columns; it is possible only for extended statistics. So, univariate statistics could be utilized only in **PostgreSQL forks** for now.
