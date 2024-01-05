#ifndef PG_INDEX_STATS_H
#define PG_INDEX_STATS_H

#include "postgres.h"

#include "access/relation.h"

#define EXTENSION_NAME "pg_index_stats"

extern bool lookup_relation_statistics(Oid heapOid,
									   Bitmapset *columns,
									   List *exprs);

#endif							/* PG_INDEX_STATS_H */
