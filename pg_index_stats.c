/*-------------------------------------------------------------------------
 *
 * pg_index_stats.c
 *		Generate extended statistics on a definition of non-system index.

 * Copyright (c) 2023-2025 Andrei Lepikhov
 *
 * This software may be modified and distributed under the terms
 * of the MIT license. See the LICENSE file for details.
 *
 * IDENTIFICATION
 *	  contrib/pg_sindex_stats/pg_index_stats.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/nbtree.h"
#include "access/xact.h"
#include "catalog/dependency.h"
#include "catalog/index.h"
#include "catalog/namespace.h"
#include "catalog/objectaccess.h"
#include "catalog/pg_extension.h"
#include "catalog/pg_statistic_ext.h"
#include "commands/defrem.h"
#if PG_VERSION_NUM >= 180000
#include "commands/explain.h"
#include "commands/explain_state.h"
#endif
#include "commands/extension.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "tcop/utility.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/varlena.h"

#include "pg_index_stats.h"
#include "duplicated_slots.h"

#if PG_VERSION_NUM >= 180000
PG_MODULE_MAGIC_EXT(
					.name = MODULE_NAME,
					.version = "0.3.0"
);
#else
PG_MODULE_MAGIC;
#endif

PG_FUNCTION_INFO_V1(pg_index_stats_build);

#define DEFAULT_STATTYPES STAT_MCV_NAME", "STAT_NDISTINCT_NAME

static char *stattypes = DEFAULT_STATTYPES;
static int extstat_columns_limit = 5; /* Don't allow to be too expensive */
static bool combine_stats = true;


/* Stuff for the explain extension */
#if PG_VERSION_NUM >= 180000
#include "catalog/pg_statistic.h"
#include "commands/explain_format.h"
//#include "optimizer/planner.h"
#include "utils/selfuncs.h"
#include "utils/syscache.h"

static int	es_extension_id = -1;
static explain_per_plan_hook_type prev_explain_per_plan_hook = NULL;
static get_relation_stats_hook_type prev_get_relation_stats_hook = NULL;
static get_index_stats_hook_type prev_get_index_stats_hook = NULL;
static ExplainOneQuery_hook_type prev_ExplainOneQuery_hook = NULL;

static void table_stat_handler(ExplainState *es, DefElem *opt, ParseState *pstate);
static void table_stat_per_plan_hook(PlannedStmt *plannedstmt, IntoClause *into,
									 ExplainState *es, const char *queryString,
									 ParamListInfo params,
									 QueryEnvironment *queryEnv);


 typedef struct table_stat_options
 {
	 bool showstat;
 } table_stat_options;


typedef struct RelStatEntryKey
{
	Oid			relid;
	AttrNumber	attnum;
} RelStatEntryKey;

typedef struct RelStatEntry
{
	RelStatEntryKey	key;

	int				freq;

	bool			mcv;
	int				mcv_nvalues;

	bool			mcelems;
	int				mcelems_nvalues;

	bool			hist;
	int				hist_nvalues;

	bool			range_hist;
	int				range_hist_nvalues;

	bool			dec_hist;
	int				dec_hist_nvalues;

	bool			corr;

	double			stadistinct;
	double			stanullfrac;
	double			stawidth;
} RelStatEntry;

static HTAB *sc_htab = NULL;

/*
 * We need to avoid mixing statistics gathered on different levels of explain -
 * remember, inside an EXPLAIN ANALYZE a stored routine may be executed which
 * at its turn, may execute an EXPLAIN ANALYZE.
 *
 * It also allows us to clean statistics at proper moment and disable actions in
 * the relation_stats_hook at any level more than one.
 */
static int explain_level = 0;

static bool sc_enable = false;

static bool
index_stats_hook(PlannerInfo *root, Oid indexOid, AttrNumber indexattnum,
				 VariableStatData *vardata)
{
	/* TODO: at first we need to identify the use case */

	if (prev_get_index_stats_hook)
		return (*prev_get_index_stats_hook) (root, indexOid, indexattnum, vardata);

	return false;
}

/*
 * Register the fact that statistics was requested. Save that fact until the
 * end of explain process and print it.
 * We don't afraid overhead because it should work only with explains.
 */
static bool
relation_stats_hook(PlannerInfo *root, RangeTblEntry *rte, AttrNumber attnum,
					VariableStatData *vardata)
{
	HeapTuple		statsTuple;
	Form_pg_statistic stats;
	AttStatsSlot	sslot;
	RelStatEntry   *entry;
	RelStatEntryKey	key;
	bool			found;
	int				i;

	if (!sc_enable || rte->rtekind != RTE_RELATION)
		return false;

	Assert(OidIsValid(rte->relid));

	if (explain_level != 1)
		/*
		 * We may design multi-level stat gathering and showing in each explain
		 * but for this purpose we need to invent a storage for each level stat.
		 */
		return false;

	statsTuple = SearchSysCache3(STATRELATTINH, ObjectIdGetDatum(rte->relid),
								 Int16GetDatum(attnum), BoolGetDatum(rte->inh));

	if (!HeapTupleIsValid(statsTuple))
	{
		return false;
	}

	if (sc_htab == NULL)
	{
		const HASHCTL info = {
								.hcxt = TopMemoryContext,
								.keysize = sizeof(RelStatEntryKey),
								.entrysize = sizeof(RelStatEntry)
							};
		sc_htab = hash_create(MODULE_NAME" stats hash", 64, &info,
								HASH_ELEM | HASH_BLOBS);
	}

	/*
	 * Now, we have a stat. need to probe it and detect which type of statistic
	 * exists there.
	 */

	memset(&key, 0, sizeof(RelStatEntryKey));

	for (i = 1; i < root->simple_rel_array_size; i++)
	{
		if (root->simple_rte_array[i] != rte)
			continue;

		break;
	}
	Assert(i < root->simple_rel_array_size);

	/* Use the index instead of oid to see which statistics was used */
	key.relid = i;

	key.attnum = attnum;
	entry = hash_search(sc_htab, &key, HASH_ENTER, &found);
	if (!found)
	{
		entry->freq = 0;
		entry->dec_hist = false;
		entry->hist = false;
		entry->range_hist = false;
		entry->mcelems = false;
		entry->mcv = false;
		entry->corr = false;
	}

	entry->freq++;

	/*
	 * Check what kind of statistic exists on this column and how big it is.
	 * We need only numbers to avoid unnecessary overhead.
	 */
	if (get_attstatsslot(&sslot, statsTuple,
						 STATISTIC_KIND_MCV, InvalidOid, ATTSTATSSLOT_NUMBERS))
	{
		entry->mcv = true;
		entry->mcv_nvalues = sslot.nnumbers;
		free_attstatsslot(&sslot);
	}
	if (get_attstatsslot(&sslot, statsTuple,
		STATISTIC_KIND_MCELEM, InvalidOid, ATTSTATSSLOT_NUMBERS))
	{
		entry->mcelems = true;
		entry->mcelems_nvalues = sslot.nnumbers;
		free_attstatsslot(&sslot);
	}
	if (get_attstatsslot(&sslot, statsTuple,
		STATISTIC_KIND_HISTOGRAM, InvalidOid, ATTSTATSSLOT_VALUES)) /* Doesn't have numbers? */
	{
		entry->hist = true;
		entry->hist_nvalues = sslot.nvalues;
		free_attstatsslot(&sslot);
	}
	if (get_attstatsslot(&sslot, statsTuple,
		STATISTIC_KIND_RANGE_LENGTH_HISTOGRAM, InvalidOid, ATTSTATSSLOT_NUMBERS))
	{
		entry->range_hist = true;
		entry->range_hist_nvalues = sslot.nnumbers;
		free_attstatsslot(&sslot);
	}
	if (get_attstatsslot(&sslot, statsTuple,
		STATISTIC_KIND_DECHIST, InvalidOid, ATTSTATSSLOT_NUMBERS))
	{
		entry->dec_hist = true;
		entry->dec_hist_nvalues = sslot.nnumbers;
		free_attstatsslot(&sslot);
	}
	if (get_attstatsslot(&sslot, statsTuple,
		STATISTIC_KIND_CORRELATION, InvalidOid, ATTSTATSSLOT_NUMBERS))
	{
		entry->corr = true;
		free_attstatsslot(&sslot);
	}

	stats = ((Form_pg_statistic) GETSTRUCT(statsTuple));
	entry->stadistinct = stats->stadistinct;
	entry->stanullfrac = stats->stanullfrac;
	entry->stawidth = stats->stawidth;
	ReleaseSysCache(statsTuple);

	/*
	 * If someone else uses this hook let them do the job and reuse their
	 * decision
	 */
	if (prev_get_relation_stats_hook)
		return (*prev_get_relation_stats_hook) (root, rte, attnum, vardata);

	/* Or just return false - we don't provide any stats here */
	return false;
}

static void
sc_ExplainOneQuery_hook(Query *query, int cursorOptions, IntoClause *into,
						struct ExplainState *es, const char *queryString,
						ParamListInfo params, QueryEnvironment *queryEnv)
{
	Assert(es_extension_id >= 0);
	explain_level++;

	/*
	 * Do nothing if EXPLAIN doesn't include our options.
	 * It may happen that next EXPLAIN down the recursion contains that option.
	 * In this case we show statistics only for the first explain, enabled it.
	 * Does it seem strange?
	 */
	if (explain_level == 1)
	{
		table_stat_options *options;

		Assert(sc_enable == false);
		options = GetExplainExtensionState(es, es_extension_id);
		if (options != NULL && options->showstat)
			sc_enable = true;
	}

	PG_TRY();
	{
		if (prev_ExplainOneQuery_hook)
			(*prev_ExplainOneQuery_hook) (query, cursorOptions, into, es,
										  queryString, params, queryEnv);
		else
			standard_ExplainOneQuery(query, cursorOptions, into, es,
									 queryString, params, queryEnv);
	}
	PG_FINALLY();
	{
		explain_level--;

		if (explain_level == 0)
		{
			/* Cleanup after the end of top-level explain */
			if (sc_htab != NULL)
			{
				hash_destroy(sc_htab);
				sc_htab = NULL;

				/* Does hash table is filled without a command? */
				Assert(sc_enable == true);
			}

			sc_enable = false;
		}
	}
	PG_END_TRY();
}

#endif


/* Local Memory Context to avoid multiple free commands */
MemoryContext mem_ctx = NULL;

void _PG_init(void);

static object_access_hook_type next_object_access_hook = NULL;
static ProcessUtility_hook_type next_ProcessUtility_hook = NULL;

static List *index_candidates = NIL;

static bool pg_index_stats_build_int(Relation rel);

static bool
_check_stattypes_string(const char *str)
{
	List	   *elemlist = NIL;
	ListCell   *lc;
	char	   *tmp_str;

	if (strlen(str) == 0)
	{
		GUC_check_errdetail("must not be empty");
		return false;
	}

	tmp_str = pstrdup(str);

	if (!SplitDirectoriesString(tmp_str, ',', &elemlist) || elemlist == NIL)
	{
		pfree(tmp_str);
		GUC_check_errdetail("variable list format error");
		return false;
	}

	pfree(tmp_str);

	foreach(lc, elemlist)
	{
		char   *stattype = (char *) lfirst(lc);

		if (strcmp(stattype, STAT_NDISTINCT_NAME) == 0)
			continue;
		else if (strcmp(stattype, STAT_MCV_NAME) == 0)
			continue;
		else if (strcmp(stattype, STAT_DEPENDENCIES_NAME) == 0)
			continue;
		else if (strcmp(stattype, "all") == 0)
			continue;

		GUC_check_errdetail("parameter %s is incorrect", stattype);
		return false;
	}

	return true;
}

/*
 * Return set of statistic types that a user wants to be generated in auto mode.
 *
 * Later in the code we should check duplicated statistics. So, here is not a
 * final decision on which types of extended statistics we will see after the
 * index creation.
 *
 * It is expensive a little bit to use in each hook call. So, be careful or
 * invent a cache.
 */
static int32
get_statistic_types()
{
	List	   *elemlist;
	ListCell   *lc;
	int			statistic_types = 0;
	char	   *tmp_stattypes;
	MemoryContext oldctx;

	oldctx = MemoryContextSwitchTo(mem_ctx);
	tmp_stattypes = pstrdup(stattypes);
	if (!SplitDirectoriesString(tmp_stattypes, ',', &elemlist))
	{
		/* syntax error in list */
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("invalid list syntax in parameter \"%s\"",
						"statistic_types")));
	}

	if (elemlist == NIL)
	{
		MemoryContextSwitchTo(oldctx);
		return 0;
	}

	foreach(lc, elemlist)
	{
		char   *stattype = (char *) lfirst(lc);

		if (strcmp(stattype, STAT_NDISTINCT_NAME) == 0)
			statistic_types |= STAT_NDISTINCT;
		else if (strcmp(stattype, STAT_MCV_NAME) == 0)
			statistic_types |= STAT_MCV;
		else if (strcmp(stattype, STAT_DEPENDENCIES_NAME) == 0)
			statistic_types |= STAT_DEPENDENCIES;
		else if (strcmp(stattype, "all") == 0)
			statistic_types = STAT_NDISTINCT | STAT_MCV | STAT_DEPENDENCIES;
		else
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("Invalid parameter \"%s\" in GUC \"%s\"",
							stattype, "statistic_types")));
	}

	MemoryContextSwitchTo(oldctx);
	MemoryContextReset(mem_ctx);
	return statistic_types;
}

static bool
_create_statistics(CreateStatsStmt *stmt, Oid indexId)
{
	ObjectAddress	obj;
	ObjectAddress	refobj;
	Oid				extoid;

	obj = CreateStatistics(stmt);
	if (!OidIsValid(obj.classId))
		/* Statistics aren't generated by a reason - return. */
		return false;

	extoid = get_extension_oid(MODULE_NAME, true);

	/* Add dependency on the extension and the index */
	if (OidIsValid(extoid))
	{
		ObjectAddressSet(refobj, ExtensionRelationId, extoid);
		recordDependencyOn(&obj, &refobj, DEPENDENCY_AUTO);
	}

	/*
	 * If the extension wasn't created in the database, statistics will
	 * depend on the index relation only.
	 */
	ObjectAddressSet(refobj, RelationRelationId, indexId);
	recordDependencyOn(&obj, &refobj, DEPENDENCY_AUTO);

	/* Let next command to see newly created statistics */
	CommandCounterIncrement();

	return true;
}

/*
 * generateClonedExtStatsStmt
 */
Datum
pg_index_stats_build(PG_FUNCTION_ARGS)
{
	text	   *relname = PG_GETARG_TEXT_PP(0);
	char	   *stats_list = text_to_cstring(PG_GETARG_TEXT_PP(1));
	char *tmp_stats_list;
	RangeVar   *relvar;
	Relation	rel;
	bool		result;

	if (extstat_columns_limit <= 0)
	{
		elog(WARNING, "Nothing to create because columns limit is too small");
		PG_RETURN_BOOL(false);
	}

	tmp_stats_list = pstrdup(GetConfigOption(MODULE_NAME".stattypes", false, true));

	/* Check correctness of the stats string */
	(void) get_statistic_types();

	SetConfigOption(MODULE_NAME".stattypes", stats_list, PGC_SUSET, PGC_S_SESSION);

	/* Get descriptor of incoming index relation */
	relvar = makeRangeVarFromNameList(textToQualifiedNameList(relname));
	rel = relation_openrv(relvar, AccessShareLock);

	if (rel->rd_rel->relkind != RELKIND_INDEX &&
		rel->rd_rel->relkind != RELKIND_PARTITIONED_INDEX)
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("\"%s\" is not an index",
						RelationGetRelationName(rel))));

	result = pg_index_stats_build_int(rel);
	relation_close(rel, AccessShareLock);

	/* XXX: In case of an ERROR it will be restored at the end of the function? */
	SetConfigOption(MODULE_NAME".stattypes", tmp_stats_list, PGC_SUSET, PGC_S_SESSION);
	pfree(tmp_stats_list);
	PG_RETURN_BOOL(result);
}

/*
 * Use index and its description for creating definition of extended statistics
 * expression.
 */
static bool
pg_index_stats_build_int(Relation rel)
{
	Relation		hrel = NULL;
	TupleDesc		tupdesc = NULL;
	Oid				indexId;
	Oid				heapId;
	IndexInfo	   *indexInfo = NULL;
	Oid				save_userid;
	int				save_sec_context;
	int				save_nestlevel;
	bool			result = false;
	int32			stat_types = 0;

	if (extstat_columns_limit <= 0 || (stat_types = get_statistic_types()) == 0)
		return false;

	/*
	 * Switch to the table owner's userid, so that any index functions are
	 * run as that user.  Also lock down security-restricted operations
	 * and arrange to make GUC variable changes local to this command.
	 */
	GetUserIdAndSecContext(&save_userid, &save_sec_context);
	SetUserIdAndSecContext(rel->rd_rel->relowner,
						   save_sec_context | SECURITY_RESTRICTED_OPERATION);
	save_nestlevel = NewGUCNestLevel();

	indexId = RelationGetRelid(rel);
	indexInfo = BuildIndexInfo(rel);

	/*
	 * Forbid any other indexes except btree just to be sure we have specific
	 * operator for each variable in the statistics.
	 * TODO:
	 * 1) Is not enough for expressions?
	 * 2) Should we ease it for manual mode?
	 */
	if (indexInfo->ii_Am != BTREE_AM_OID || indexInfo->ii_NumIndexKeyAttrs < 2)
		goto cleanup;

	/*
	 * Here is we form a statement to build statistics.
	 */
	{
		CreateStatsStmt	   *stmt = makeNode(CreateStatsStmt);
		ListCell		   *indexpr_item = list_head(indexInfo->ii_Expressions);
		RangeVar		   *from;
		int					i;
		Bitmapset		   *atts_used = NULL;
		List			   *exprlst = NIL;

		heapId = IndexGetRelation(indexId, false);
		hrel = relation_open(heapId, AccessShareLock);

		/*
		 * TODO: Create statistics could be applied to plain table, foreign
		 * table or materialized VIEW.
		 */
		if (hrel->rd_rel->relkind != RELKIND_RELATION)
			/*
			 * Just for sure. TODO: may be better. At least for TOAST relations
			 */
			goto cleanup;

		/* Next step is to build statistics expression list */

		tupdesc = RelationGetDescr(hrel);
		from = makeRangeVar(get_namespace_name(RelationGetNamespace(hrel)),
								pstrdup(RelationGetRelationName(hrel)), -1);

		for (i = 0; i < indexInfo->ii_NumIndexKeyAttrs; i++)
		{
			AttrNumber	attnum = indexInfo->ii_IndexAttrNumbers[i];
			StatsElem  *selem;

			Assert(extstat_columns_limit > 1);

			if (list_length(exprlst) >= extstat_columns_limit)
			{
				/*
				 * To reduce risks of blind usage use only limited number of
				 * index columns.
				 */
				break;
			}

			if (attnum != 0)
			{
				if (bms_is_member(attnum, atts_used))
					/* Can't build extended statistics with column duplicates */
					continue;

				selem = makeNode(StatsElem);
				selem->name = pstrdup(TupleDescAttr(tupdesc, attnum - 1)->attname.data);
				selem->expr = NULL;
				atts_used = bms_add_member(atts_used, attnum);
			}
			else
			{
				Node	   *indexkey;

				selem = makeNode(StatsElem);
				indexkey = (Node *) lfirst(indexpr_item);
				Assert(indexkey != NULL);
				indexpr_item = lnext(indexInfo->ii_Expressions, indexpr_item);
				selem->name = NULL;
				selem->expr = indexkey;
			}

			exprlst = lappend(exprlst, selem);
		}

		if (list_length(exprlst) < 2)
			/* Extended statistics can be made only for two or more expressions */
			goto cleanup;

		/*
		 * Now we have statistics definition. That's a good place to check other
		 * statistics on the same relation and correct our definition to reduce
		 * duplicated data as much as possible.
		 */
		if (combine_stats)
			stat_types = reduce_duplicated_stat(exprlst, atts_used, hrel, stat_types);
		if (stat_types == 0)
			/* Reduced to nothing */
			goto cleanup;

		elog(DEBUG2, "Final Auto-generated statistics definition: %d", stat_types);

		/* Still only one relation allowed in the core */
		stmt->relations = list_make1(from);
		stmt->stxcomment = MODULE_NAME" - multivariate statistics";
		stmt->transformed = false;	/* true when transformStatsStmt is finished */
		stmt->if_not_exists = true;
		stmt->defnames = NULL;		/* qualified name (list of String) */
		stmt->exprs = exprlst;

		if (stat_types & STAT_NDISTINCT)
			stmt->stat_types = lappend(stmt->stat_types, makeString(STAT_NDISTINCT_NAME));
		if (stat_types & STAT_DEPENDENCIES)
			stmt->stat_types = lappend(stmt->stat_types, makeString(STAT_DEPENDENCIES_NAME));
		if (stat_types & STAT_MCV)
			stmt->stat_types = lappend(stmt->stat_types, makeString(STAT_MCV_NAME));
		Assert(stmt->stat_types != NIL);

		if (!_create_statistics(stmt, indexId))
			goto cleanup;

		/*
		 * Don't free here allocated structures because we do it in transaction
		 * memory context. May we need to clean it locally in the case of
		 * building statistics over huge database?
		 */
	}

	result = true;

cleanup:
	if (hrel)
		relation_close(hrel, AccessShareLock);
	if (indexInfo)
		pfree(indexInfo);

	AtEOXact_GUC(false, save_nestlevel);
	SetUserIdAndSecContext(save_userid, save_sec_context);

	return result;
}

/*
 * Just save candidate OID to the list.
 * We can't make a lot of actions here, because we don't see all the changes
 * needed. So, just save it.
 */
static void
extstat_remember_index_hook(ObjectAccessType access, Oid classId,
							Oid objectId, int subId, void *arg)
{
	MemoryContext memctx;

	if (next_object_access_hook)
		(*next_object_access_hook) (access, classId, objectId, subId, arg);

	/*
	 * Disable the extension machinery in some cases.
	 * Changing that place remember, that we have UI to manually generate
	 * extended statistics over existed schema. Sometimes, you need to change
	 * that place as well.
	 */
	if (extstat_columns_limit <= 0 ||
		!IsNormalProcessingMode() ||
		access != OAT_POST_CREATE || classId != RelationRelationId)
		return;

	memctx = MemoryContextSwitchTo(TopMemoryContext);
	index_candidates = lappend_oid(index_candidates, objectId);
	MemoryContextSwitchTo(memctx);
}

static void
after_utility_extstat_creation(PlannedStmt *pstmt, const char *queryString,
							   bool readOnlyTree,
							   ProcessUtilityContext context,
							   ParamListInfo params, QueryEnvironment *queryEnv,
							   DestReceiver *dest, QueryCompletion *qc)
{
	ListCell   *lc;

	if (next_ProcessUtility_hook)
		(*next_ProcessUtility_hook) (pstmt, queryString, readOnlyTree,
									 context, params, queryEnv,
									 dest, qc);
	else
		standard_ProcessUtility(pstmt, queryString, readOnlyTree,
								context, params, queryEnv,
								dest, qc);

	/* Now, we can create extended statistics */

	/* Quick exit on ROLLBACK or nothing to do */
	if (index_candidates == NIL || !IsTransactionState() ||
		IsA(pstmt->utilityStmt, ReindexStmt))
	{
		/*
		 * HACK: We ignore ReindexStmt because don't understand exactly how to
		 * avoid some issues caused by this command. Should be resolved.
		 */
		list_free(index_candidates);
		index_candidates = NIL;
		return;
	}

	Assert(extstat_columns_limit > 0);

	PG_TRY();
	{
		foreach (lc, index_candidates)
		{
			Oid			reloid = lfirst_oid(lc);
			Relation	rel;

			rel = try_relation_open(reloid, AccessShareLock);
			if (rel == NULL)
			{
				index_candidates = foreach_delete_current(index_candidates, lc);
				continue;
			}

			if (!(rel->rd_rel->relkind == RELKIND_INDEX ||
				rel->rd_rel->relkind == RELKIND_PARTITIONED_INDEX))
			{
				index_candidates = foreach_delete_current(index_candidates, lc);
				relation_close(rel, AccessShareLock);
				continue;
			}

			pg_index_stats_build_int(rel);
			index_candidates = foreach_delete_current(index_candidates, lc);
			relation_close(rel, AccessShareLock);
	}
	}
	PG_FINALLY();
	{
		/*
		 * To avoid memory leaks and repeated errors clean list of candidates
		 * in the case of any errors.
		 */
		list_free(index_candidates);
		index_candidates = NIL;
	}
	PG_END_TRY();
}

static bool
check_hook_stattypes(char **newval, void **extra, GucSource source)
{
	if (!_check_stattypes_string(*newval))
		return false;

	return true;
}


void
_PG_init(void)
{
	DefineCustomStringVariable(MODULE_NAME".stattypes",
							   "Types of statistics to be automatically created",
							   NULL,
							   &stattypes,
							   DEFAULT_STATTYPES,
							   PGC_SUSET,
							   0,
							   check_hook_stattypes, NULL, NULL);

	DefineCustomIntVariable(MODULE_NAME".columns_limit",
							"Sets the maximum number of columns involved in extended statistics",
							NULL,
							&extstat_columns_limit,
							5,
							0,
							INT_MAX,
							PGC_SUSET,
							0,
							NULL,
							NULL,
							NULL);

	DefineCustomBoolVariable(MODULE_NAME".compactify",
								"Reduce redundancy of extended statistics",
								"Before creation of new statistic remove all already existed stat types from the definition",
								&combine_stats,
								true,
								PGC_USERSET,
								0,
								NULL,
								NULL,
								NULL
	   );

	next_object_access_hook = object_access_hook;
	object_access_hook = extstat_remember_index_hook;

	next_ProcessUtility_hook = ProcessUtility_hook;
	ProcessUtility_hook = after_utility_extstat_creation;

	mem_ctx = AllocSetContextCreate(TopMemoryContext,
											 MODULE_NAME" - local memory context",
											 ALLOCSET_DEFAULT_SIZES);

#if PG_VERSION_NUM < 150000
	EmitWarningsOnPlaceholders(MODULE_NAME);
#else
	MarkGUCPrefixReserved(MODULE_NAME);
#endif


#if PG_VERSION_NUM >= 180000
	RegisterExtensionExplainOption("stat", table_stat_handler);
	es_extension_id = GetExplainExtensionId(MODULE_NAME);

	prev_explain_per_plan_hook = explain_per_plan_hook;
	explain_per_plan_hook = table_stat_per_plan_hook;

	prev_get_relation_stats_hook = get_relation_stats_hook;
	get_relation_stats_hook = relation_stats_hook;
	prev_ExplainOneQuery_hook = ExplainOneQuery_hook;
	ExplainOneQuery_hook = sc_ExplainOneQuery_hook;
	prev_get_index_stats_hook = get_index_stats_hook;
	get_index_stats_hook = index_stats_hook;
#endif
}


/* *****************************************************************************
 *
 * EXPLAIN extension
 *
 **************************************************************************** */

 #if PG_VERSION_NUM >= 180000

 static table_stat_options *
 table_stat_ensure_options(ExplainState *es)
 {
	 table_stat_options *options;

	 options = GetExplainExtensionState(es, es_extension_id);

	 if (options == NULL)
	 {
		 options = palloc0(sizeof(table_stat_options));
		 SetExplainExtensionState(es, es_extension_id, options);
	 }

	 return options;
 }

 static void
 table_stat_handler(ExplainState *es, DefElem *opt, ParseState *pstate)
 {
	 table_stat_options *options = table_stat_ensure_options(es);

	 options->showstat = defGetBoolean(opt);
 }

 #include "parser/parsetree.h"
 static void
 relation_stats_show(ExplainState *es)
 {
	HASH_SEQ_STATUS	status;
	RelStatEntry   *entry;

	if (sc_htab == NULL || hash_get_num_entries(sc_htab) == 0)
	{
		appendStringInfo(es->str, "No statistics used during the query planning\n");
		return;
	}

	hash_seq_init(&status, sc_htab);
	while ((entry = (RelStatEntry *) hash_seq_search(&status)) != NULL)
	{
		RangeTblEntry  *rte;
		char		   *attname;

		rte = rt_fetch(entry->key.relid, es->rtable);
		attname = get_attname(rte->relid, entry->key.attnum, false);

		if (es->format != EXPLAIN_FORMAT_TEXT)
		{
			ExplainPropertyText("table", get_rel_name(rte->relid), es);
			if (rte->alias && rte->alias->aliasname)
				ExplainPropertyText("alias", rte->alias->aliasname, es);

			ExplainPropertyText("attname", attname, es);
			ExplainPropertyInteger("times", NULL, entry->freq, es);
			ExplainOpenGroup("Stats", "stats", true, es);
			if (entry->mcv)
				ExplainPropertyInteger("MCV values", NULL, entry->mcv_nvalues, es);
			if (entry->hist)
				ExplainPropertyInteger("Histogram values", NULL,
									   entry->hist_nvalues, es);
			if (entry->dec_hist)
				ExplainPropertyInteger("Dist histogram values", NULL,
									   entry->dec_hist_nvalues, es);
			if (entry->mcelems)
				ExplainPropertyInteger("MC Elements values", NULL,
									   entry->mcelems_nvalues, es);
			if (entry->range_hist)
				ExplainPropertyInteger("Range histogram values", NULL,
									   entry->range_hist_nvalues, es);
			if (entry->corr)
				ExplainPropertyBool("Correlation", true, es);

			ExplainPropertyFloat("ndistinct", NULL, entry->stadistinct, 4, es);
			ExplainPropertyFloat("nullfrac", NULL, entry->stanullfrac, 4, es);
			ExplainPropertyFloat("width", NULL, entry->stawidth, 0, es);
			ExplainCloseGroup("Stats", "stats", true, es);
		}
		else
		{
			ExplainIndentText(es);
			if (rte->alias && rte->alias->aliasname)
				appendStringInfo(es->str, "%s (%s).%s: %d times, stats: {",
								 get_rel_name(rte->relid),
								 rte->alias->aliasname,  attname, entry->freq);
			else
				appendStringInfo(es->str, "%s.%s: %d times, stats: {",
								 get_rel_name(rte->relid), attname,
								 entry->freq);
			if (entry->mcv)
				appendStringInfo(es->str, " MCV: %d values,", entry->mcv_nvalues);
			if (entry->hist)
				appendStringInfo(es->str, " Histogram: %d values,",
								 entry->hist_nvalues);
			if (entry->dec_hist)
				appendStringInfo(es->str, " Dist histogram: %d values,",
								 entry->dec_hist_nvalues);
			if (entry->mcelems)
				appendStringInfo(es->str, " MC Elements: %d values,",
								 entry->mcelems_nvalues);
			if (entry->range_hist)
				appendStringInfo(es->str, " Range histogram: %d values,",
								 entry->range_hist_nvalues);
			if (entry->corr)
				appendStringInfo(es->str, " Correlation,");

			appendStringInfo(es->str, " ndistinct: %.4lf, nullfrac: %.4lf, width: %.0lf",
							 entry->stadistinct, entry->stanullfrac,
							 entry->stawidth);
			appendStringInfo(es->str, " }\n");
		}

	}
 }

 static void
 table_stat_per_plan_hook(PlannedStmt *plannedstmt,
						  IntoClause *into,
						  ExplainState *es,
						  const char *queryString,
						  ParamListInfo params,
						  QueryEnvironment *queryEnv)
 {
	table_stat_options *options;

	if (prev_explain_per_plan_hook)
		(*prev_explain_per_plan_hook) (plannedstmt, into, es, queryString,
									   params, queryEnv);

	options = GetExplainExtensionState(es, es_extension_id);
	if (options == NULL || !options->showstat)
		return;

	ExplainOpenGroup("Statistics", "Statistics", true, es);

	if (es->format == EXPLAIN_FORMAT_TEXT)
	{
		ExplainIndentText(es);
		appendStringInfo(es->str, "Statistics:\n");
		es->indent++;
	}
	relation_stats_show(es);

	if (es->format == EXPLAIN_FORMAT_TEXT)
	{
		es->indent--;
	}
	ExplainCloseGroup("Statistics", "Statistics", true, es);
 }

 #endif
