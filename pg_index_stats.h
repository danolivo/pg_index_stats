#ifndef PG_INDEX_STATS_H
#define PG_INDEX_STATS_H

#include "postgres.h"

#include "access/relation.h"

#define EXTENSION_NAME "pg_index_stats"

extern int extstat_autogen_mode;

extern List * analyze_relation_statistics(Oid heapOid,
										  Bitmapset *columns,
										  List *exprs);
extern bool is_duplicate_stat(List *statList);

extern List *get_all_multivariate_stmts(Relation heaprel,
								   Bitmapset *attrs,
								   List *exprlst);

#endif							/* PG_INDEX_STATS_H */
