#ifndef PG_INDEX_STATS_H
#define PG_INDEX_STATS_H

#include "postgres.h"

#include "access/relation.h"
#include "commands/explain_state.h"

#define MODULE_NAME "pg_index_stats"

#define STAT_NDISTINCT_NAME		"ndistinct"
#define STAT_MCV_NAME			"mcv"
#define STAT_DEPENDENCIES_NAME	"dependencies"

#define STAT_NDISTINCT		(1<<0)
#define STAT_MCV			(1<<1)
#define STAT_DEPENDENCIES	(1<<2)

/*
 * Explain options. There are only one way exist is to consolidate all the
 * options inside a single structure.
 */
typedef struct StatMgrOptions
{
	bool show_stat;
	bool show_extstat_candidates;
} StatMgrOptions;

extern MemoryContext pg_index_stats_mem_ctx;
extern int current_execution_level;

extern Bitmapset *check_duplicated(List *statList, int32 stat_types);
StatMgrOptions *StatMgrOptions_ensure(ExplainState *es);

/* Query-based statistic generator routines */

extern void qds_init(void);

#endif							/* PG_INDEX_STATS_H */
