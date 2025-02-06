/*-------------------------------------------------------------------------
 *
 * duplicated_slots.c
 *		Analyse request on new extended statistic and already existing in
 * 		the pg_statistic_ext. Exclude from the definition duplicated data
 * 		(as much as possible).

 * Copyright (c) 2023-2025 Andrei Lepikhov
 *
 * This software may be modified and distributed under the terms
 * of the MIT license. See the LICENSE file for details.
 *
 * IDENTIFICATION
 *	  contrib/pg_sindex_stats/duplicated_slots.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/genam.h"
#include "access/htup_details.h"
#include "access/table.h"
#include "catalog/pg_statistic_ext.h"
#include "catalog/pg_statistic_ext_data.h"
#include "nodes/nodeFuncs.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/rel.h"
#include "utils/syscache.h"

#include "duplicated_slots.h"
#include "pg_index_stats.h"

typedef struct StatExtEntry
{
	char	   *name;
	Bitmapset  *columns;
	int			types;
	List	   *exprs;
} StatExtEntry;

/*
 * It is based on the code of the static fetch_statentries_for_relation, the
 * extended_stats.c module
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

		entry = palloc0(sizeof(StatExtEntry));
		staForm = (Form_pg_statistic_ext) GETSTRUCT(htup);
		entry->name = pstrdup(NameStr(staForm->stxname));
		for (i = 0; i < staForm->stxkeys.dim1; i++)
		{
			entry->columns = bms_add_member(entry->columns,
											staForm->stxkeys.values[i]);
		}

		/* decode the stxkind char array into a list of chars */
		datum = SysCacheGetAttr(STATEXTOID, htup,
								Anum_pg_statistic_ext_stxkind, &isnull);
		Assert(!isnull);
		arr = DatumGetArrayTypeP(datum);
		enabled = (char *) ARR_DATA_PTR(arr);
		for (i = 0; i < ARR_DIMS(arr)[0]; i++)
		{
			if (enabled[i] == STATS_EXT_NDISTINCT)
				entry->types |= STAT_NDISTINCT;
			else if (enabled[i] == STATS_EXT_DEPENDENCIES)
				entry->types |= STAT_DEPENDENCIES;
			else if (enabled[i] == STATS_EXT_MCV)
				entry->types |= STAT_MCV;
			else if (enabled[i] == STATS_EXT_EXPRESSIONS)
			{
				/* Not implemented yet */
			}
			else
				/* XXX: in case of extensibility it would not correct */
				elog(PANIC, "Unknown extstat type");
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

			/* May as well fix opfuncids too */
			fix_opfuncids((Node *) exprs);
		}

		entry->exprs = exprs;

		result = lappend(result, entry);
	}

	systable_endscan(scan);

	return result;
}

static bool
has_same_mcv(const List *statslist, const List *exprs,
			 const Bitmapset *atts_used)
{
	ListCell   *lc;
	List	   *tmp_exprs = list_copy(exprs);

	foreach(lc, tmp_exprs)
	{
		StatsElem  *selem = (StatsElem *) lfirst(lc);

		if (selem->expr == NULL)
			tmp_exprs = foreach_delete_current(tmp_exprs, lc);
	}

	/*
	 * Look for the first index with the same definition containing MCV stat.
	 */
	foreach(lc, statslist)
	{
		StatExtEntry *stat = (StatExtEntry *) lfirst(lc);

		/*  */
		if (bms_equal(stat->columns, atts_used))
		{
			ListCell   *lc1;

			/* exprs doesn't contain duplicates already */
			foreach(lc1, tmp_exprs)
			{
				StatsElem	   *selem = (StatsElem *) lfirst(lc1);
				ListCell	   *lc2;

				foreach(lc2, stat->exprs)
				{
					Node *expr2 = (Node *) lfirst(lc2);

					if (equal(selem->expr, expr2))
					{
						break;
					}
				}

				if (lc2 == NULL)
				{
					/* Unsuccessful comparison, break */
					break;
				}
			}

			if (lc1 != NULL)
				/* Unsuccessful comparison, go to the next stat slot */
				continue;

			/* Duplicated stat found. Just check the internals */
			if (stat->types & STAT_MCV)
				return true;
		}
	}

	return false;
}

/*
 * Decide on types of extended statistics that should stay in the definition.
 */
int
reduce_duplicated_stat(const List *exprs, Bitmapset *atts_used,
					   Relation hrel, int32 stat_types)
{
	Relation	pg_stext;
	List	   *statslist;
	MemoryContext oldctx;

	oldctx = MemoryContextSwitchTo(mem_ctx);

	Assert(stat_types > 0);

	pg_stext = table_open(StatisticExtRelationId, RowExclusiveLock);
	statslist = fetch_statentries_for_relation(pg_stext, RelationGetRelid(hrel));
	if (statslist == NIL)
	{
		table_close(pg_stext, RowExclusiveLock);
		MemoryContextSwitchTo(oldctx);
		return stat_types;
	}

	/*
	 * Most simple decision: we have to left it in the definition if no exact
	 * duplicate exists in the stat list.
	 */
	if (stat_types & STAT_MCV && has_same_mcv(statslist, exprs, atts_used))
	{
		stat_types &= ~STAT_MCV;
	}

	if (stat_types & STAT_NDISTINCT)
	{
		/* TODO */
	}

	if (stat_types & STAT_DEPENDENCIES)
	{
		/* TODO */
	}

	table_close(pg_stext, RowExclusiveLock);
	MemoryContextSwitchTo(oldctx);
	return stat_types;
}
