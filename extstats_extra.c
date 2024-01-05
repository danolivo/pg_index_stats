/*-------------------------------------------------------------------------
 *
 * extstats_extra.c
 *		Extra code to operate with extended statistics. Should correspond to any
 *		changes in extended_stats.c

 * Copyright (c) 2023 Andrei Lepikhov
 *
 * This software may be modified and distributed under the terms
 * of the MIT license. See the LICENSE file for details.
 *
 * IDENTIFICATION
 *	  contrib/pg_sindex_stats/extstats_extra.c
 *
 *-------------------------------------------------------------------------
 */

#include "pg_index_stats.h"

#include "access/genam.h"
#include "access/table.h"
#include "catalog/pg_statistic_ext.h"
#include "utils/rel.h"

typedef struct StatExtEntry
{
	Oid			statOid;		/* OID of pg_statistic_ext entry */
	char	   *schema;			/* statistics object's schema */
	char	   *name;			/* statistics object's name */
	Bitmapset  *columns;		/* attribute numbers covered by the object */
	List	   *types;			/* 'char' list of enabled statistics kinds */
	int			stattarget;		/* statistics target (-1 for default) */
	List	   *exprs;			/* expressions */
} StatExtEntry;

#include "access/htup_details.h"
#include "catalog/pg_type.h"
#include "commands/comment.h"
#include "nodes/nodeFuncs.h"
#include "optimizer/optimizer.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"

/*
 * Extract auto-generated statistics
 */
static List *
fetch_statentries_for_relation(Relation pg_statext, Oid relid)
{
	SysScanDesc scan;
	ScanKeyData skey;
	HeapTuple	htup;
	List	   *result = NIL;

	/*
	 * Prepare to scan pg_statistic_ext for entries having stxrelid = this
	 * rel.
	 */
	ScanKeyInit(&skey,
				Anum_pg_statistic_ext_stxrelid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(relid));

	scan = systable_beginscan(pg_statext, StatisticExtRelidIndexId, true,
							  NULL, 1, &skey);

	while (HeapTupleIsValid(htup = systable_getnext(scan)))
	{
		StatExtEntry *entry;
		Datum		datum;
		bool		isnull;
		int			i;
		ArrayType  *arr;
		char	   *enabled;
		Form_pg_statistic_ext staForm;
		List	   *exprs = NIL;
//		char	   *stxcomment;

		entry = palloc0(sizeof(StatExtEntry));
		staForm = (Form_pg_statistic_ext) GETSTRUCT(htup);
		entry->statOid = staForm->oid;

		/* Skip custom statistics has made by an user manually */
//		stxcomment = GetComment(entry->statOid, StatisticExtRelationId, 0);
//		if (strstr(stxcomment, EXTENSION_NAME" - ") == NULL)
//			continue;

		entry->schema = get_namespace_name(staForm->stxnamespace);
		entry->name = pstrdup(NameStr(staForm->stxname));
		entry->stattarget = staForm->stxstattarget;
		for (i = 0; i < staForm->stxkeys.dim1; i++)
		{
			entry->columns = bms_add_member(entry->columns,
											staForm->stxkeys.values[i]);
		}

		/* decode the stxkind char array into a list of chars */
		datum = SysCacheGetAttrNotNull(STATEXTOID, htup,
									   Anum_pg_statistic_ext_stxkind);
		arr = DatumGetArrayTypeP(datum);
		if (ARR_NDIM(arr) != 1 ||
			ARR_HASNULL(arr) ||
			ARR_ELEMTYPE(arr) != CHAROID)
			elog(ERROR, "stxkind is not a 1-D char array");
		enabled = (char *) ARR_DATA_PTR(arr);
		for (i = 0; i < ARR_DIMS(arr)[0]; i++)
		{
			Assert((enabled[i] == STATS_EXT_NDISTINCT) ||
				   (enabled[i] == STATS_EXT_DEPENDENCIES) ||
				   (enabled[i] == STATS_EXT_MCV) ||
				   (enabled[i] == STATS_EXT_EXPRESSIONS));
			entry->types = lappend_int(entry->types, (int) enabled[i]);
		}

		/* decode expression (if any) */
		datum = SysCacheGetAttr(STATEXTOID, htup,
								Anum_pg_statistic_ext_stxexprs, &isnull);

		if (!isnull)
		{
			char	   *exprsString;

			exprsString = TextDatumGetCString(datum);
			exprs = (List *) stringToNode(exprsString);

			pfree(exprsString);

			/*
			 * Run the expressions through eval_const_expressions. This is not
			 * just an optimization, but is necessary, because the planner
			 * will be comparing them to similarly-processed qual clauses, and
			 * may fail to detect valid matches without this.  We must not use
			 * canonicalize_qual, however, since these aren't qual
			 * expressions.
			 */
			exprs = (List *) eval_const_expressions(NULL, (Node *) exprs);

			/* May as well fix opfuncids too */
			fix_opfuncids((Node *) exprs);
		}

		entry->exprs = exprs;

		result = lappend(result, entry);
	}

	systable_endscan(scan);

	return result;
}

bool
lookup_relation_statistics(Oid heapOid, Bitmapset *columns, List *exprs)
{
	Relation	pg_stext;
	List	   *statslist;
	ListCell   *lc;
	List	   *tmp_exprs;
	bool		exprs_equal;

	pg_stext = table_open(StatisticExtRelationId, AccessShareLock);
	statslist = fetch_statentries_for_relation(pg_stext, heapOid);

	/* Lookup for duplicated statistics */
	foreach(lc, statslist)
	{
		StatExtEntry *entry = (StatExtEntry *) lfirst(lc);

		if (bms_equal(columns, entry->columns))
		{
			ListCell   *lc1;

			/* Columns are the same. Compare expressions */

			tmp_exprs = list_copy(entry->exprs);

			foreach (lc1, exprs)
			{

				StatsElem  *elem =  (StatsElem *) lfirst(lc1);
				Node	   *expr1 = elem->expr;
				ListCell   *lc2;
				bool		found = false;

				if (tmp_exprs == NIL)
				{
					exprs_equal = false;
					break;
				}

				foreach (lc2, tmp_exprs)
				{
					Node *expr2 = (Node *) lfirst(lc2);

					if (!equal(expr1, expr2))
						continue;

					tmp_exprs = foreach_delete_current(tmp_exprs, lc2);
					found = true;
					break;
				}

				if (!found)
				{
					exprs_equal = false;
					break;
				}

				exprs_equal = true;
			}
		}
	}

	table_close(pg_stext, AccessShareLock);
	return exprs_equal;
}
