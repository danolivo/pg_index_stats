#include "postgres.h"

#include "access/nbtree.h"
#include "access/relation.h"
#include "access/xact.h"
#include "catalog/dependency.h"
#include "catalog/index.h"
#include "catalog/namespace.h"
#include "catalog/objectaccess.h"
#include "catalog/pg_extension.h"
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

PG_FUNCTION_INFO_V1(build_extended_statistic);

#define EXTENSION_NAME "pg_index_stats"

typedef enum
{
	MODE_DISABLED,
	MODE_ALL,
	MODE_UNIVARIATE,
	MODE_MULTIVARIATE,
}	pg_index_stats_mode;

/* GUC variables */
static const struct config_enum_entry format_options[] = {
	{"disabled", MODE_DISABLED, false},
	{"all", MODE_ALL, false},
	{"univariate", MODE_UNIVARIATE, false},
	{"multivariate", MODE_MULTIVARIATE, false},
	{NULL, 0, false}
};
static int extstat_autogen_mode = MODE_MULTIVARIATE;

void _PG_init(void);

static object_access_hook_type next_object_access_hook = NULL;
static ProcessUtility_hook_type next_ProcessUtility_hook = NULL;

static List *index_candidates = NIL;

static bool build_extended_statistic_int(Relation rel);

static bool _create_statistics(CreateStatsStmt *stmt, Oid indexId)
{
	ObjectAddress	obj;
	ObjectAddress	refobj;
	Oid				extoid;

	obj = CreateStatistics(stmt);
	if (!OidIsValid(obj.classId))
		return false;

	extoid = get_extension_oid(EXTENSION_NAME, true);

	/* Add dependency on the extension and the index */
	if (OidIsValid(extoid))
	{
		/*
		 * If the extension wasn't created in the database, statostics will
		 * depend on the index relation only.
		 */
		ObjectAddressSet(refobj, ExtensionRelationId, extoid);
		recordDependencyOn(&obj, &refobj, DEPENDENCY_AUTO);
	}

	ObjectAddressSet(refobj, RelationRelationId, indexId);
	recordDependencyOn(&obj, &refobj, DEPENDENCY_AUTO);

	return true;
}

/*
 * generateClonedExtStatsStmt
 */
Datum
build_extended_statistic(PG_FUNCTION_ARGS)
{
	text	   *relname = PG_GETARG_TEXT_PP(0);
	char	   *cmode = text_to_cstring(PG_GETARG_TEXT_PP(1));
	const char *tmpmode = GetConfigOption(EXTENSION_NAME".mode", false, true);
	RangeVar   *relvar;
	Relation	rel;
	bool		result;

	SetConfigOption(EXTENSION_NAME".mode", cmode, PGC_SUSET, PGC_S_SESSION);

	/* Get descriptor of incoming index relation */
	relvar = makeRangeVarFromNameList(textToQualifiedNameList(relname));
	rel = relation_openrv(relvar, AccessShareLock);

	if (rel->rd_rel->relkind != RELKIND_INDEX &&
		rel->rd_rel->relkind != RELKIND_PARTITIONED_INDEX)
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("\"%s\" is not an index",
						RelationGetRelationName(rel))));

	result = build_extended_statistic_int(rel);
	relation_close(rel, AccessShareLock);

	SetConfigOption(EXTENSION_NAME".mode", tmpmode, PGC_SUSET, PGC_S_SESSION);
	PG_RETURN_BOOL(result);
}

/*
 * Use index and its description for creating definition of extended statistics
 * expression.
 */
static bool
build_extended_statistic_int(Relation rel)
{
	TupleDesc		tupdesc = NULL;
	Oid				indexId;
	Oid				heapId;
	IndexInfo	   *indexInfo = NULL;
	Oid				save_userid;
	int				save_sec_context;
	int				save_nestlevel;
	bool			result = false;

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
	{
		AtEOXact_GUC(false, save_nestlevel);
		SetUserIdAndSecContext(save_userid, save_sec_context);
		pfree(indexInfo);
		return false;
	}

	/*
	 * Here is we form a statement to build statistics.
	 */
	{
		CreateStatsStmt	   *stmt = makeNode(CreateStatsStmt);
		ListCell		   *indexpr_item = list_head(indexInfo->ii_Expressions);
		RangeVar		   *from;
		int					i;
		Relation			hrel;
		Bitmapset		   *atts_used = NULL;

		heapId = IndexGetRelation(indexId, false);
		hrel = relation_open(heapId, AccessShareLock);

		/*
		 * TODO: Create statistics could be applied to plain table, foreign
		 * table or materialized VIEW.
		 */
		if (hrel->rd_rel->relkind != RELKIND_RELATION)
		{
			/*
			 * Just for sure. TODO: may be better. At least for TOAST relations
			 */
			relation_close(hrel, AccessShareLock);
			goto cleanup;
		}

		tupdesc = CreateTupleDescCopy(RelationGetDescr(hrel));
		from = makeRangeVar(get_namespace_name(RelationGetNamespace(hrel)),
								pstrdup(RelationGetRelationName(hrel)), -1),
		relation_close(hrel, AccessShareLock);

		stmt->defnames = NULL;		/* qualified name (list of String) */
		stmt->exprs = NIL;
		stmt->stat_types = list_make3(makeString("ndistinct"),
									  makeString("dependencies"),
									  makeString("mcv"));

		for (i = 0; i < indexInfo->ii_NumIndexKeyAttrs; i++)
		{
			AttrNumber	atnum = indexInfo->ii_IndexAttrNumbers[i];
			StatsElem  *selem = makeNode(StatsElem);

			if (atnum != 0)
			{
				if (bms_is_member(atnum, atts_used))
					/* Can't build extended statistics with column duplicates */
					continue;

				selem->name = pstrdup(tupdesc->attrs[atnum - 1].attname.data);
				selem->expr = NULL;
				atts_used = bms_add_member(atts_used, atnum);
			}
			else
			{
				Node	   *indexkey;

				indexkey = (Node *) lfirst(indexpr_item);
				Assert(indexkey != NULL);
				indexpr_item = lnext(indexInfo->ii_Expressions, indexpr_item);
				selem->name = NULL;
				selem->expr = indexkey;
			}

			stmt->exprs = lappend(stmt->exprs, selem);
		}

		if (list_length(stmt->exprs) < 2)
			/* Extended statistics can be made only for two or more expressions */
			goto cleanup;

		/* Still only one relation allowed in the core */
		stmt->relations = list_make1(from);
		stmt->stxcomment = EXTENSION_NAME" - multivariate statistics";
		stmt->transformed = false;	/* true when transformStatsStmt is finished */
		stmt->if_not_exists = true;

		if (extstat_autogen_mode == MODE_ALL ||
			extstat_autogen_mode == MODE_MULTIVARIATE)
		{
			if (!_create_statistics(stmt, indexId))
				goto cleanup;
		}

		/* Univariate statistics */
		if (extstat_autogen_mode == MODE_ALL ||
			extstat_autogen_mode == MODE_UNIVARIATE)
		{
			RowExpr	   *rowexpr = makeNode(RowExpr);
			StatsElem  *selem = makeNode(StatsElem);

			/* Need to see results of previously inserted statistics */
			CommandCounterIncrement();

			indexpr_item = list_head(indexInfo->ii_Expressions);
			rowexpr->args = NIL;
			bms_free(atts_used);
			atts_used = NULL;

			for (i = 0; i < indexInfo->ii_NumIndexKeyAttrs; i++)
			{
				AttrNumber	atnum = indexInfo->ii_IndexAttrNumbers[i];

				if (atnum != 0)
				{
					Var		   *varnode;

					if (bms_is_member(atnum, atts_used))
						/* Can't build extended statistics with column duplicates */
						continue;

					varnode = makeVar(1, /* I see this trick in the code around.
										  * But it would be better get to know
										  * what does this magic number means
										  * exactly.
										  */
									  atnum,
									  tupdesc->attrs[atnum - 1].atttypid,
									  tupdesc->attrs[atnum - 1].atttypmod,
									  tupdesc->attrs[atnum - 1].attcollation,
									  0);
					varnode->location = -1;
					rowexpr->args = lappend(rowexpr->args, varnode);
					atts_used = bms_add_member(atts_used, atnum);
				}
				else
				{
					Node	   *indexkey;

					indexkey = (Node *) lfirst(indexpr_item);
					Assert(indexkey != NULL);
					indexpr_item = lnext(indexInfo->ii_Expressions, indexpr_item);
					rowexpr->args = lappend(rowexpr->args, indexkey);
				}
			}

			rowexpr->row_typeid = RECORDOID;
			rowexpr->row_format = COERCE_IMPLICIT_CAST;
			rowexpr->colnames = NIL;
			rowexpr->location = -1;

			selem->name = NULL;
			selem->expr = (Node *) rowexpr;
			stmt->stxcomment = EXTENSION_NAME" - univariate statistics";
			stmt->stat_types = NIL; /* not needed for a single expression */

			list_free(stmt->exprs);
			stmt->exprs = list_make1(selem);
			if (!_create_statistics(stmt, indexId))
				goto cleanup;
		}

		list_free_deep(stmt->relations);
	}

	result = true;

cleanup:

	if (indexInfo)
		pfree(indexInfo);
	if (tupdesc)
		FreeTupleDesc(tupdesc);

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

	if (extstat_autogen_mode == MODE_DISABLED || !IsNormalProcessingMode() ||
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

			build_extended_statistic_int(rel);
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

void
_PG_init(void)
{
	DefineCustomEnumVariable(EXTENSION_NAME".mode",
							 "Mode of extended statistics creation on new index.",
							 NULL,
							 &extstat_autogen_mode,
							 MODE_MULTIVARIATE,
							 format_options,
							 PGC_SUSET,
							 0,
							 NULL,
							 NULL,
							 NULL
	);

	next_object_access_hook = object_access_hook;
	object_access_hook = extstat_remember_index_hook;

	next_ProcessUtility_hook = ProcessUtility_hook;
	ProcessUtility_hook = after_utility_extstat_creation;

	MarkGUCPrefixReserved(EXTENSION_NAME);
}
