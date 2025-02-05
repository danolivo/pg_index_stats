/*-------------------------------------------------------------------------
 *
 * pg_index_stats.c
 *		Generate extended statistics on a definition of each newly created
 *		non-system index.

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

#include "pg_index_stats.h"

#include "access/nbtree.h"
#include "access/xact.h"
#include "catalog/dependency.h"
#include "catalog/index.h"
#include "catalog/namespace.h"
#include "catalog/objectaccess.h"
#include "catalog/pg_extension.h"
#include "catalog/pg_statistic_ext.h"
#include "commands/defrem.h"
#include "commands/extension.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "tcop/utility.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/varlena.h"

PG_MODULE_MAGIC;

PG_FUNCTION_INFO_V1(pg_index_stats_build);

static char *stattypes = "mcv, distinct";
static int32 statistic_types = -1;
static int extstat_columns_limit = 5; /* Don't allow to be too expensive */

void _PG_init(void);

static object_access_hook_type next_object_access_hook = NULL;
static ProcessUtility_hook_type next_ProcessUtility_hook = NULL;

static List *index_candidates = NIL;

static bool pg_index_stats_build_int(Relation rel);

/*
 * Return set of statistic types that a user wants to be generated in auto mode.
 *
 * Later in the code we should check duplicated statistics. So, here is not a
 * final decision on which types of extended statistics we will see after the
 * index creation.
 */
static int32
get_statistic_types()
{
	List	   *elemlist;
	ListCell   *lc;

	if (statistic_types != -1)
		return statistic_types;

	if (!SplitDirectoriesString(stattypes, ',', &elemlist))
	{
		/* syntax error in list */
		ereport(FATAL,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("invalid list syntax in parameter \"%s\"",
						"statistic_types")));
	}

	statistic_types = 0;
	if (elemlist == NIL)
		return statistic_types;

	foreach(lc, elemlist)
	{
		char   *stattype = (char *) lfirst(lc);

		if (strcmp(stattype, "distinct") == 0)
			statistic_types |= STAT_NDISTINCT;
		else if (strcmp(stattype, "mcv") == 0)
			statistic_types |= STAT_MCV;
		else if (strcmp(stattype, "deps") == 0)
			statistic_types |= STAT_DEPENDENCIES;
		else if (strcmp(stattype, "all") == 0)
			statistic_types = STAT_NDISTINCT | STAT_MCV | STAT_DEPENDENCIES;
		else
			ereport(FATAL,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("Invalid parameter \"%s\" in GUC \"%s\"",
						stattype, "statistic_types")));
	}

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
 * Decide on types of extended statistics that should stay in the definition.
 */
static int
_remove_duplicates(const List *exprs, const IndexInfo *indexInfo, int32 stat_types)
{
	/*
	 * Most simple decision: we have to left it in the definition if no exact
	 * duplicate exists in the stat list.
	 */
	if (stat_types & STAT_MCV)
	{
		/* TODO */
	}

	if (stat_types & STAT_NDISTINCT)
	{
		/* TODO */
	}

	if (stat_types & STAT_DEPENDENCIES)
	{
		/* TODO */
	}

	return stat_types;
}

/*
 * generateClonedExtStatsStmt
 */
Datum
pg_index_stats_build(PG_FUNCTION_ARGS)
{
	text	   *relname = PG_GETARG_TEXT_PP(0);
	char	   *cmode = text_to_cstring(PG_GETARG_TEXT_PP(1));
	const char *tmpmode = GetConfigOption(MODULE_NAME".stattypes", false, true);
	RangeVar   *relvar;
	Relation	rel;
	bool		result;

	if (extstat_columns_limit <= 0)
	{
		elog(WARNING, "Nothing to create because columns limit is too small");
		PG_RETURN_BOOL(false);
	}

	SetConfigOption(MODULE_NAME".stattypes", cmode, PGC_SUSET, PGC_S_SESSION);

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

	SetConfigOption(MODULE_NAME".stattypes", tmpmode, PGC_SUSET, PGC_S_SESSION);
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
		stat_types = _remove_duplicates(exprlst, indexInfo, stat_types);

		/* Still only one relation allowed in the core */
		stmt->relations = list_make1(from);
		stmt->stxcomment = MODULE_NAME" - multivariate statistics";
		stmt->transformed = false;	/* true when transformStatsStmt is finished */
		stmt->if_not_exists = true;
		stmt->defnames = NULL;		/* qualified name (list of String) */
		stmt->exprs = exprlst;

		if (stat_types & STAT_NDISTINCT)
			stmt->stat_types = lappend(stmt->stat_types, makeString("ndistinct"));
		if (stat_types & STAT_DEPENDENCIES)
			stmt->stat_types = lappend(stmt->stat_types, makeString("dependencies"));
		if (stat_types & STAT_MCV)
			stmt->stat_types = lappend(stmt->stat_types, makeString("mcv"));
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
	if (extstat_columns_limit <= 0 || get_statistic_types() == 0 ||
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
	if (!IsTransactionState() || index_candidates == NIL)
		return;

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

static void
assign_hook_stattypes(const char *newval, void *extra)
{
	statistic_types = -1;
}

void
_PG_init(void)
{
	DefineCustomStringVariable(MODULE_NAME".stattypes",
							   "Types of statistics to be automatically created",
							   NULL,
							   &stattypes,
							   "mcv, distinct",
							   PGC_SUSET,
							   0,
							   NULL, assign_hook_stattypes, NULL);

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

	next_object_access_hook = object_access_hook;
	object_access_hook = extstat_remember_index_hook;

	next_ProcessUtility_hook = ProcessUtility_hook;
	ProcessUtility_hook = after_utility_extstat_creation;

#if PG_VERSION_NUM < 150000
	EmitWarningsOnPlaceholders(MODULE_NAME);
#else
	MarkGUCPrefixReserved(MODULE_NAME);
#endif
}
