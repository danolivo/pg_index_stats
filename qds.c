/* -----------------------------------------------------------------------------
 *
 *
 *
 * -----------------------------------------------------------------------------
 */

#include "postgres.h"

#include "executor/executor.h"
#include "utils/guc.h"

#include "pg_index_stats.h"

static int estimation_error_threshold = 2;

static ExecutorEnd_hook_type prev_ExecutorEnd_hook = NULL;

static void
qds_ExecutorEnd(QueryDesc *queryDesc)
{
	if (prev_ExecutorEnd_hook)
		(*prev_ExecutorEnd_hook) (queryDesc);
	else
		standard_ExecutorEnd(queryDesc);
}

void
qds_init(void)
{
	DefineCustomIntVariable(MODULE_NAME".estimation_error_threshold",
		"Planner estimation error deviation, above which extended statistics is built",
		NULL,
		&estimation_error_threshold,
		2,
		-1,
		INT_MAX,
		PGC_SUSET,
		0,
		NULL,
		NULL,
		NULL);

		prev_ExecutorEnd_hook = ExecutorEnd_hook;
		ExecutorEnd_hook = qds_ExecutorEnd;
}
