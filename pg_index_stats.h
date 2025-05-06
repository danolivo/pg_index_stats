#ifndef PG_INDEX_STATS_H
#define PG_INDEX_STATS_H

#include "postgres.h"

#define MODULE_NAME "pg_index_stats"

#define STAT_NDISTINCT_NAME		"ndistinct"
#define STAT_MCV_NAME			"mcv"
#define STAT_DEPENDENCIES_NAME	"dependencies"

#define STAT_NDISTINCT		(1<<0)
#define STAT_MCV			(1<<1)
#define STAT_DEPENDENCIES	(1<<2)

#include "access/relation.h"
#if PG_VERSION_NUM >= 180000
#include "commands/explain_state.h"
/*
 * Explain options. There are only one way exist is to consolidate all the
 * options inside a single structure.
 */
typedef struct StatMgrOptions
{
	bool show_stat;
	bool show_extstat_candidates;
} StatMgrOptions;

StatMgrOptions *StatMgrOptions_ensure(ExplainState *es);
#endif

extern MemoryContext pg_index_stats_mem_ctx;
extern int current_execution_level;

extern Bitmapset *check_duplicated(List *statList, int32 stat_types);

/* Query-based statistic generator routines */

extern void qds_init(void);

#endif							/* PG_INDEX_STATS_H */
