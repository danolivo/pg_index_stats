#ifndef PG_INDEX_STATS_H
#define PG_INDEX_STATS_H

#include "postgres.h"

#include "access/relation.h"

#define MODULE_NAME "pg_index_stats"

#define STAT_NDISTINCT		(1<<0)
#define STAT_MCV			(1<<1)
#define STAT_DEPENDENCIES	(1<<2)

extern Bitmapset *check_duplicated(List *statList, int32 stat_types);

#endif							/* PG_INDEX_STATS_H */
