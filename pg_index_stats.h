#ifndef PG_INDEX_STATS_H
#define PG_INDEX_STATS_H

#include "postgres.h"

#include "access/relation.h"

#define MODULE_NAME "pg_index_stats"

#define STAT_NDISTINCT_NAME		"ndistinct"
#define STAT_MCV_NAME			"mcv"
#define STAT_DEPENDENCIES_NAME	"dependencies"

#define STAT_NDISTINCT		(1<<0)
#define STAT_MCV			(1<<1)
#define STAT_DEPENDENCIES	(1<<2)

extern MemoryContext mem_ctx;

extern Bitmapset *check_duplicated(List *statList, int32 stat_types);

/* Query-based statistic generator routines */

extern void qds_init(void);

#endif							/* PG_INDEX_STATS_H */
