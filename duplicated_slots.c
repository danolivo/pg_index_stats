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

typedef struct
{
	StatExtEntry *entry;

	Bitmapset *common_attrs;
	Bitmapset *existed_attrs;
	Bitmapset *new_attrs;

	List	   *common_exprs;
	List	   *existed_exprs;
	List	   *new_exprs;
} StatListCmp;

#define DUPDEF_STAT(cmps) ( \
	cmps->existed_exprs == NIL && cmps->new_exprs == NIL && \
	bms_num_members(cmps->existed_attrs) == 0 && \
	bms_num_members(cmps->new_attrs) == 0  && \
	bms_num_members(cmps->common_attrs) + list_length(cmps->common_exprs) > 0 \
)

#define COVERINGDEF_STAT(cmps) ( \
	cmps->new_exprs == NIL && bms_num_members(cmps->new_attrs) == 0 \
)

static List *
_probe_statistics(const List *statslist, const List *exprs,
				  const Bitmapset *attrs_used)
{
	ListCell   *lc;
	List	   *tmp_exprs = list_copy(exprs);
	List	   *result = NIL;
	List	   *tmp_exprs2;

	/*
	 * Remove Vars from the list. We need only exprs there.
	 */
	foreach(lc, tmp_exprs)
	{
		StatsElem  *selem = (StatsElem *) lfirst(lc);

		if (selem->expr == NULL)
			tmp_exprs = foreach_delete_current(tmp_exprs, lc);
	}

	Assert(list_length(exprs) ==
			list_length(tmp_exprs) + bms_num_members(attrs_used));

	tmp_exprs2 = tmp_exprs;
	foreach(lc, statslist)
	{
		StatExtEntry   *stat = (StatExtEntry *) lfirst(lc);
		StatListCmp	   *cmps = palloc0(sizeof(StatListCmp));
		ListCell	   *lc1;

		cmps->entry = stat;
		cmps->common_attrs = bms_intersect(stat->columns, attrs_used);
		cmps->existed_attrs = bms_del_members(bms_copy(stat->columns), attrs_used);
		cmps->new_attrs = bms_del_members(bms_copy(attrs_used), stat->columns);

		tmp_exprs = list_copy(tmp_exprs2);
		foreach(lc1, stat->exprs)
		{
			Node		   *expr2 = (Node *) lfirst(lc1);
			ListCell	   *lc2;

			foreach(lc2, tmp_exprs)
			{
				StatsElem	   *selem = (StatsElem *) lfirst(lc2);

				if (equal(selem->expr, expr2))
				{
					/* Match is found. Add it into the list and go out */
					cmps->common_exprs = lappend(cmps->common_exprs, expr2);
					tmp_exprs = foreach_delete_current(tmp_exprs, lc2);
					break;
				}
			}

			if (lc2 == NULL)
			{
				/*
				 * Unsuccessful comparison: existing stat contains expression
				 * which is not in the incoming expression list
				 */
				cmps->existed_exprs = lappend(cmps->existed_exprs, expr2);
				break;
			}
		}

		/* Save remaining expressions from the incoming definition as 'new' */
		foreach(lc1, tmp_exprs)
		{
			StatsElem	   *selem = (StatsElem *) lfirst(lc1);

			cmps->new_exprs = lappend(cmps->new_exprs, selem->expr);
			tmp_exprs = foreach_delete_current(tmp_exprs, lc1);
		}

		Assert(tmp_exprs == NIL);
		Assert(list_length(cmps->new_exprs) + list_length(cmps->common_exprs) ==
							list_length(exprs) - bms_num_members(attrs_used));
		Assert(list_length(cmps->existed_exprs) +
					list_length(cmps->common_exprs) == list_length(stat->exprs));

		result = lappend(result, cmps);
	}

	return result;
}

/*
 * Decide on types of extended statistics that should stay in the definition.
 */
int
reduce_duplicated_stat(const List *exprs, Bitmapset *atts_used,
					   Relation hrel, int32 stat_types)
{
	Relation		pg_stext;
	List		   *statslist;
	MemoryContext	oldctx;
	List		   *cmpsList;
	ListCell	   *lc;

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

	cmpsList = _probe_statistics(statslist, exprs, atts_used);
	foreach(lc, cmpsList)
	{
		StatListCmp	   *cmps = (StatListCmp *) lfirst(lc);

		if (DUPDEF_STAT(cmps))
		{
			/*
			 * The most simple decision: if we already have a statistics with
			 * the same definition, we don't need new statistics unless existed
			 * one doesn't contain statistic of specific type.
			 */
			if (cmps->entry->types & STAT_MCV)
				stat_types &= ~STAT_MCV;
			if (cmps->entry->types & STAT_NDISTINCT)
				stat_types &= ~STAT_NDISTINCT;
			if (cmps->entry->types & STAT_DEPENDENCIES)
				stat_types &= ~STAT_DEPENDENCIES;
		}

		if (COVERINGDEF_STAT(cmps))
		{
			if (cmps->entry->types & STAT_NDISTINCT)
				stat_types &= ~STAT_NDISTINCT;
			if (cmps->entry->types & STAT_DEPENDENCIES)
				stat_types &= ~STAT_DEPENDENCIES;
		}

		/* TODO: change old statistic if a new one covers this old one */

		/* XXX: What if we have intersecting statistics ? */
	}

	table_close(pg_stext, RowExclusiveLock);
	MemoryContextSwitchTo(oldctx);
	return stat_types;
}
