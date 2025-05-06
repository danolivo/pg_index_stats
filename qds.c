/* -----------------------------------------------------------------------------
 *
 *
 *
 * -----------------------------------------------------------------------------
 */

#include "postgres.h"

#include "commands/defrem.h"
#include "commands/explain.h"
#include "executor/executor.h"
#include "nodes/nodeFuncs.h"
#include "optimizer/optimizer.h"
#include "optimizer/planner.h"
#include "parser/parsetree.h"
#include "statistics/extended_stats_internal.h"
#include "utils/fmgroids.h"
#include "utils/guc.h"
#include "utils/lsyscache.h"

#include "pg_index_stats.h"


static bool enable_qds = true;
static double estimation_error_threshold = 2.0;

static create_upper_paths_hook_type prev_create_upper_paths_hook = NULL;
static ExecutorFinish_hook_type prev_ExecutorFinish = NULL;
static ExecutorRun_hook_type prev_ExecutorRun = NULL;
static ExecutorEnd_hook_type prev_ExecutorEnd_hook = NULL;

#if PG_VERSION_NUM >= 180000
#include "commands/explain_format.h"

static int	es_extension_id = -1;

static explain_validate_options_hook_type prev_explain_validate_options_hook = NULL;
static explain_per_node_hook_type prev_explain_per_node_hook = NULL;
#endif

#if PG_VERSION_NUM < 150000
typedef double Cardinality;
#endif

/* *****************************************************************************
 *
 * Copy of extended_stat.c static routines
 *
 *
 ******************************************************************************/

#include "miscadmin.h"
#include "nodes/pathnodes.h"
#include "utils/acl.h"

static bool
statext_is_compatible_clause_internal(PlannerInfo *root, Node *clause,
									  Index relid, Bitmapset **attnums,
									  List **exprs)
{
	/* Look inside any binary-compatible relabeling (as in examine_variable) */
	if (IsA(clause, RelabelType))
		clause = (Node *) ((RelabelType *) clause)->arg;

	/* plain Var references (boolean Vars or recursive checks) */
	if (IsA(clause, Var))
	{
		Var		   *var = (Var *) clause;

		/* Ensure var is from the correct relation */
		if (var->varno != relid)
			return false;

		/* we also better ensure the Var is from the current level */
		if (var->varlevelsup > 0)
			return false;

		/*
		 * Also reject system attributes and whole-row Vars (we don't allow
		 * stats on those).
		 */
		if (!AttrNumberIsForUserDefinedAttr(var->varattno))
			return false;

		/* OK, record the attnum for later permissions checks. */
		*attnums = bms_add_member(*attnums, var->varattno);

		return true;
	}

	/* (Var/Expr op Const) or (Const op Var/Expr) */
	if (is_opclause(clause))
	{
		RangeTblEntry *rte = root->simple_rte_array[relid];
		OpExpr	   *expr = (OpExpr *) clause;
		Node	   *clause_expr;

		/* Only expressions with two arguments are considered compatible. */
		if (list_length(expr->args) != 2)
			return false;

		/* Check if the expression has the right shape */
		if (!examine_opclause_args(expr->args, &clause_expr, NULL, NULL))
			return false;

		/*
		 * If it's not one of the supported operators ("=", "<", ">", etc.),
		 * just ignore the clause, as it's not compatible with MCV lists.
		 *
		 * This uses the function for estimating selectivity, not the operator
		 * directly (a bit awkward, but well ...).
		 */
		switch (get_oprrest(expr->opno))
		{
			case F_EQSEL:
			case F_NEQSEL:
			case F_SCALARLTSEL:
			case F_SCALARLESEL:
			case F_SCALARGTSEL:
			case F_SCALARGESEL:
				/* supported, will continue with inspection of the Var/Expr */
				break;

			default:
				/* other estimators are considered unknown/unsupported */
				return false;
		}

		/*
		 * If there are any securityQuals on the RTE from security barrier
		 * views or RLS policies, then the user may not have access to all the
		 * table's data, and we must check that the operator is leakproof.
		 *
		 * If the operator is leaky, then we must ignore this clause for the
		 * purposes of estimating with MCV lists, otherwise the operator might
		 * reveal values from the MCV list that the user doesn't have
		 * permission to see.
		 */
		if (rte->securityQuals != NIL &&
			!get_func_leakproof(get_opcode(expr->opno)))
			return false;

		/* Check (Var op Const) or (Const op Var) clauses by recursing. */
		if (IsA(clause_expr, Var))
			return statext_is_compatible_clause_internal(root, clause_expr,
														 relid, attnums, exprs);

		/* Otherwise we have (Expr op Const) or (Const op Expr). */
		*exprs = lappend(*exprs, clause_expr);
		return true;
	}

	/* Var/Expr IN Array */
	if (IsA(clause, ScalarArrayOpExpr))
	{
		RangeTblEntry *rte = root->simple_rte_array[relid];
		ScalarArrayOpExpr *expr = (ScalarArrayOpExpr *) clause;
		Node	   *clause_expr;
		bool		expronleft;

		/* Only expressions with two arguments are considered compatible. */
		if (list_length(expr->args) != 2)
			return false;

		/* Check if the expression has the right shape (one Var, one Const) */
		if (!examine_opclause_args(expr->args, &clause_expr, NULL, &expronleft))
			return false;

		/* We only support Var on left, Const on right */
		if (!expronleft)
			return false;

		/*
		 * If it's not one of the supported operators ("=", "<", ">", etc.),
		 * just ignore the clause, as it's not compatible with MCV lists.
		 *
		 * This uses the function for estimating selectivity, not the operator
		 * directly (a bit awkward, but well ...).
		 */
		switch (get_oprrest(expr->opno))
		{
			case F_EQSEL:
			case F_NEQSEL:
			case F_SCALARLTSEL:
			case F_SCALARLESEL:
			case F_SCALARGTSEL:
			case F_SCALARGESEL:
				/* supported, will continue with inspection of the Var/Expr */
				break;

			default:
				/* other estimators are considered unknown/unsupported */
				return false;
		}

		/*
		 * If there are any securityQuals on the RTE from security barrier
		 * views or RLS policies, then the user may not have access to all the
		 * table's data, and we must check that the operator is leakproof.
		 *
		 * If the operator is leaky, then we must ignore this clause for the
		 * purposes of estimating with MCV lists, otherwise the operator might
		 * reveal values from the MCV list that the user doesn't have
		 * permission to see.
		 */
		if (rte->securityQuals != NIL &&
			!get_func_leakproof(get_opcode(expr->opno)))
			return false;

		/* Check Var IN Array clauses by recursing. */
		if (IsA(clause_expr, Var))
			return statext_is_compatible_clause_internal(root, clause_expr,
														 relid, attnums, exprs);

		/* Otherwise we have Expr IN Array. */
		*exprs = lappend(*exprs, clause_expr);
		return true;
	}

	/* AND/OR/NOT clause */
	if (is_andclause(clause) ||
		is_orclause(clause) ||
		is_notclause(clause))
	{
		/*
		 * AND/OR/NOT-clauses are supported if all sub-clauses are supported
		 *
		 * Perhaps we could improve this by handling mixed cases, when some of
		 * the clauses are supported and some are not. Selectivity for the
		 * supported subclauses would be computed using extended statistics,
		 * and the remaining clauses would be estimated using the traditional
		 * algorithm (product of selectivities).
		 *
		 * It however seems overly complex, and in a way we already do that
		 * because if we reject the whole clause as unsupported here, it will
		 * be eventually passed to clauselist_selectivity() which does exactly
		 * this (split into supported/unsupported clauses etc).
		 */
		BoolExpr   *expr = (BoolExpr *) clause;
		ListCell   *lc;

		foreach(lc, expr->args)
		{
			/*
			 * If we find an incompatible clause in the arguments, treat the
			 * whole clause as incompatible.
			 */
			if (!statext_is_compatible_clause_internal(root,
													   (Node *) lfirst(lc),
													   relid, attnums, exprs))
				return false;
		}

		return true;
	}

	/* Var/Expr IS NULL */
	if (IsA(clause, NullTest))
	{
		NullTest   *nt = (NullTest *) clause;

		/* Check Var IS NULL clauses by recursing. */
		if (IsA(nt->arg, Var))
			return statext_is_compatible_clause_internal(root, (Node *) (nt->arg),
														 relid, attnums, exprs);

		/* Otherwise we have Expr IS NULL. */
		*exprs = lappend(*exprs, nt->arg);
		return true;
	}

	/*
	 * Treat any other expressions as bare expressions to be matched against
	 * expressions in statistics objects.
	 */
	*exprs = lappend(*exprs, clause);
	return true;
}

/*
 * statext_is_compatible_clause
 *		Determines if the clause is compatible with MCV lists.
 *
 * See statext_is_compatible_clause_internal, above, for the basic rules.
 * This layer deals with RestrictInfo superstructure and applies permissions
 * checks to verify that it's okay to examine all mentioned Vars.
 *
 * Arguments:
 * clause: clause to be inspected (in RestrictInfo form)
 * relid: rel that all Vars in clause must belong to
 * *attnums: input/output parameter collecting attribute numbers of all
 *		mentioned Vars.  Note that we do not offset the attribute numbers,
 *		so we can't cope with system columns.
 * *exprs: input/output parameter collecting primitive subclauses within
 *		the clause tree
 *
 * Returns false if there is something we definitively can't handle.
 * On true return, we can proceed to match the *exprs against statistics.
 */
static bool
statext_is_compatible_clause(PlannerInfo *root, Node *clause, Index relid,
							 Bitmapset **attnums, List **exprs)
{
	RangeTblEntry *rte = root->simple_rte_array[relid];
	RelOptInfo *rel = root->simple_rel_array[relid];
	RestrictInfo *rinfo;
	int			clause_relid;
	Oid			userid;

	/*
	 * Special-case handling for bare BoolExpr AND clauses, because the
	 * restrictinfo machinery doesn't build RestrictInfos on top of AND
	 * clauses.
	 */
	if (is_andclause(clause))
	{
		BoolExpr   *expr = (BoolExpr *) clause;
		ListCell   *lc;

		/*
		 * Check that each sub-clause is compatible.  We expect these to be
		 * RestrictInfos.
		 */
		foreach(lc, expr->args)
		{
			if (!statext_is_compatible_clause(root, (Node *) lfirst(lc),
											  relid, attnums, exprs))
				return false;
		}

		return true;
	}

	/* Otherwise it must be a RestrictInfo. */
	if (!IsA(clause, RestrictInfo))
		return false;
	rinfo = (RestrictInfo *) clause;

	/* Pseudoconstants are not really interesting here. */
	if (rinfo->pseudoconstant)
		return false;

	/* Clauses referencing other varnos are incompatible. */
	if (!bms_get_singleton_member(rinfo->clause_relids, &clause_relid) ||
		clause_relid != relid)
		return false;

	/* Check the clause and determine what attributes it references. */
	if (!statext_is_compatible_clause_internal(root, (Node *) rinfo->clause,
											   relid, attnums, exprs))
		return false;

	/*
	 * Check that the user has permission to read all required attributes.
	 */
	userid = OidIsValid(rel->userid) ? rel->userid : GetUserId();

	/* Table-level SELECT privilege is sufficient for all columns */
	if (pg_class_aclcheck(rte->relid, userid, ACL_SELECT) != ACLCHECK_OK)
	{
		Bitmapset  *clause_attnums = NULL;
		int			attnum = -1;

		/*
		 * We have to check per-column privileges.  *attnums has the attnums
		 * for individual Vars we saw, but there may also be Vars within
		 * subexpressions in *exprs.  We can use pull_varattnos() to extract
		 * those, but there's an impedance mismatch: attnums returned by
		 * pull_varattnos() are offset by FirstLowInvalidHeapAttributeNumber,
		 * while attnums within *attnums aren't.  Convert *attnums to the
		 * offset style so we can combine the results.
		 */
		while ((attnum = bms_next_member(*attnums, attnum)) >= 0)
		{
			clause_attnums =
				bms_add_member(clause_attnums,
							   attnum - FirstLowInvalidHeapAttributeNumber);
		}

		/* Now merge attnums from *exprs into clause_attnums */
		if (*exprs != NIL)
			pull_varattnos((Node *) *exprs, relid, &clause_attnums);

		attnum = -1;
		while ((attnum = bms_next_member(clause_attnums, attnum)) >= 0)
		{
			/* Undo the offset */
			AttrNumber	attno = attnum + FirstLowInvalidHeapAttributeNumber;

			if (attno == InvalidAttrNumber)
			{
				/* Whole-row reference, so must have access to all columns */
				if (pg_attribute_aclcheck_all(rte->relid, userid, ACL_SELECT,
											  ACLMASK_ALL) != ACLCHECK_OK)
					return false;
			}
			else
			{
				if (pg_attribute_aclcheck(rte->relid, attno, userid,
										  ACL_SELECT) != ACLCHECK_OK)
					return false;
			}
		}
	}

	/* If we reach here, the clause is OK */
	return true;
}

/* *****************************************************************************
 *
 * END OF COPIED FRAGMENT
 *
 ******************************************************************************/

/*
 * Extract planned rows and number of rows, processed at real execution. Also,
 * compute number of filtered tuples which is not passed further by the query
 * tree, but was filtered at the scan.
 *
 * It is quite complicated operation for now, so separate it in a function.
 *
 * Return false, if the operation was failed.
 */
static bool
planstate_calculate(PlanState *ps,
					Cardinality *plan_rows, Cardinality *real_rows,
					Cardinality *touched_tuples)
{
	double nloops;
	double filtered_tuples = 0.;

	if (!ps || ps->instrument == NULL)
		/*
		 * It means a break inside the feature logic. Don't crash the instance,
		 * just complain about that. Let people safely disable the extension.
		 */
		elog(ERROR, "instrumentation is needed to analyze this query");

	/*
	 * Finish the node before an analysis. And only after that we can touch any
	 * instrument fields.
	 */
	InstrEndLoop(ps->instrument);
	nloops = ps->instrument->nloops;

	if (nloops <= 0.0)
		/*
		 * Skip 'never executed' case or "0-Tuple situation" and the case of
		 * manual switching off of the timing instrumentation
		 */
		return false;

	*real_rows = 0.;
	*touched_tuples = 0.;

	/*
	 * Calculate number of rows predicted by the optimizer and really passed
	 * through the node. This simplistic code becomes a bit tricky in the case
	 * of parallel workers.
	 */
	if (ps->worker_instrument)
	{
		double	wnloops = 0.;
		double	wntuples = 0.;
		double	divisor = ps->worker_instrument->num_workers;
		double	leader_contribution;
		int		i;

		/* XXX: Copy-pasted from the get_parallel_divisor() */
		leader_contribution = 1.0 - (0.3 * divisor);
		if (leader_contribution > 0)
			divisor += leader_contribution;
		*plan_rows = ps->plan->plan_rows * divisor;

		for (i = 0; i < ps->worker_instrument->num_workers; i++)
		{
			double t = ps->worker_instrument->instrument[i].ntuples;
			double l = ps->worker_instrument->instrument[i].nloops;

			if (l <= 0.0)
			{
				/*
				 * Worker could start but not to process any tuples just because
				 * of laziness. Skip such a node.
				 */
				continue;
			}

			wntuples += t;
			wnloops += l;
			*real_rows += t / l;

			/* In leaf nodes we should get into account filtered tuples */
			if (ps->lefttree == NULL)
				filtered_tuples +=
							ps->worker_instrument->instrument[i].nfiltered1 +
							ps->worker_instrument->instrument[i].nfiltered2 +
							ps->instrument->ntuples2;


		}

		Assert(nloops >= wnloops);

		/* Calculate the part of job have made by the main process */
		if (nloops - wnloops > 0.0)
		{
			double ntuples = ps->instrument->ntuples;

			/* In leaf nodes we should get into account filtered tuples */
			if (ps->lefttree == NULL)
				filtered_tuples +=
									ps->instrument->nfiltered1 +
									ps->instrument->nfiltered2 +
									ps->instrument->ntuples2;

			Assert(ntuples >= wntuples);
			*real_rows += (ntuples - wntuples) / (nloops - wnloops);
		}
	}
	else
	{
		*plan_rows = ps->plan->plan_rows;
		*real_rows = ps->instrument->ntuples / nloops;

		/* In leaf nodes we should get into account filtered tuples */
		if (ps->lefttree == NULL)
			filtered_tuples += (ps->instrument->nfiltered1 +
								ps->instrument->nfiltered2 +
								ps->instrument->ntuples2) / nloops;
	}

	*plan_rows = clamp_row_est(*plan_rows);
	*real_rows = clamp_row_est(*real_rows);
	*touched_tuples = filtered_tuples;
	return true;
}

static HTAB *candidate_quals = NULL;

typedef struct CandidateQualEntryKey
{
	Oid		oid;
	Index	relid;
} CandidateQualEntryKey;

typedef struct
{
	CandidateQualEntryKey	key;

	Bitmapset			   *attnums;
	List				   *exprs_list;
} CandidateQualEntry;

static bool
gather_compatible_clauses(PlannerInfo *root)
{
	int					i;

	for (i = 1; i < root->simple_rel_array_size; i++)
	{
		CandidateQualEntry *entry;
		Bitmapset		   *attnums = NULL;
		List			   *exprs = NIL;
		bool				found;
		RelOptInfo		   *rel = root->simple_rel_array[i];
		RangeTblEntry	   *rte = root->simple_rte_array[i];
		ListCell		   *lc;

		if (rel == NULL || rel->baserestrictinfo == NIL)
			continue;

		if (!(rte->rtekind == RTE_RELATION && rte->relkind == RELKIND_RELATION))
			continue;

		/* Gather all compatible columns and expressions */
		foreach (lc, rel->baserestrictinfo)
		{
			RestrictInfo *rinfo = lfirst_node(RestrictInfo, lc);

			/* Let's discover conditions */
			if (!statext_is_compatible_clause(root, (Node *) rinfo, rel->relid,
											  &attnums, &exprs))
				continue;
		}

		if (bms_num_members(attnums) + list_length(exprs) > 1)
		{
			CandidateQualEntryKey	key;
			int						member;

			Assert(rel->relid > 0 &&
				   bms_get_singleton_member(rel->relids, &member) &&
				   member == rel->relid);

			memset(&key, 0, sizeof(CandidateQualEntryKey));
			key.oid = rte->relid;
			key.relid = member;
			entry = hash_search(candidate_quals, &key, HASH_ENTER, &found);
			if (!found)
			{
				entry->attnums = NULL;
				entry->exprs_list = NIL;
			}

			entry->attnums = bms_join(entry->attnums, attnums);
			entry->exprs_list = list_concat(entry->exprs_list, exprs);
		}
	}
	return true;
}

static void
upper_paths_hook(PlannerInfo *root, UpperRelationKind stage,
				 RelOptInfo *input_rel, RelOptInfo *output_rel, void *extra)
{
	MemoryContext oldctx;

	if (prev_create_upper_paths_hook)
		(*prev_create_upper_paths_hook) (root, stage,
										 input_rel, output_rel, &extra);

	if (estimation_error_threshold < 0.0)
		return;

	if (stage != UPPERREL_FINAL)
		return;

	if (!enable_qds)
		return;

	if (candidate_quals == NULL)
	{
		HASHCTL ctl;

		ctl.keysize = sizeof(CandidateQualEntryKey);
		ctl.entrysize = sizeof(CandidateQualEntry);
		ctl.hcxt = CacheMemoryContext;

		candidate_quals = hash_create("Candidate quals table", 64, &ctl,
									  HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
	}

	/* Make any allocations outside current unsafe memory context */
	oldctx = MemoryContextSwitchTo(CacheMemoryContext);

	gather_compatible_clauses(root);

	MemoryContextSwitchTo(oldctx);
}

/*
 * Having an executor node check if it have misestimations and, at the same
 * time attnums and expressions, compatible with extended statistics.
 *
 * Return non-null entry if we have something for new statistics definition.
 */
static CandidateQualEntry *
fetch_candidate_entry(PlanState *ps, List *rtable)
{
	Index					relid = 0;
	CandidateQualEntry	   *entry;
	CandidateQualEntryKey	key;
	RangeTblEntry		   *rte;
	bool					found;

	if (candidate_quals == NULL || hash_get_num_entries(candidate_quals) == 0)
		return NULL;

	/* Now, find a relid */
	switch (nodeTag(ps))
	{
		case T_SeqScanState:
		{
			Scan *scan = (Scan *) ps->plan;

			relid = scan->scanrelid;
		}
			break;
		case T_IndexScanState:
		case T_IndexOnlyScanState:
		case T_BitmapIndexScanState:
		case T_ForeignScanState:
		case T_CustomScanState:
		case T_BitmapHeapScanState:
		case T_HashState:
		case T_IncrementalSortState:
		case T_AggState:
		case T_MemoizeState:
		case T_GroupState:
			break;
		default:
			break;
	}

	if (relid <= 0)
		return NULL;

	/*
	 * We have a candidate according to the rule. Need to extract it from
	 * the hash table and add to the recommendation's list.
	 *
	 * XXX: we can't identify exact clause set because (reloid, relid) may
	 * be the same in different subplans. Just assume it is a rare case and
	 * nothing wrong may happen except non-precise statistics definition.
	 */
	rte = rt_fetch(relid, rtable);
	memset(&key, 0, sizeof(CandidateQualEntryKey));
	key.relid = relid;
	key.oid = rte->relid;
	entry = hash_search(candidate_quals, &key, HASH_FIND, &found);

	if (!found)
		return NULL;

	Assert(entry != NULL &&
		   (!bms_is_empty(entry->attnums) || entry->exprs_list != NIL));
	return entry;
}

static bool
probe_candidate_node(PlanState *ps)
{
	Cardinality	plan_rows;
	Cardinality	real_rows;
	Cardinality filtered;
	bool		ret;

	if (ps == NULL)
		return false;

	ret = planstate_calculate(ps, &plan_rows, &real_rows, &filtered);
	if (!ret || real_rows < 2. ||
		Max(plan_rows / real_rows, real_rows / plan_rows) < estimation_error_threshold)
		return false;

	return true;
}

#if PG_VERSION_NUM >= 180000

/* *****************************************************************************
 *
 * COPY From explain.c
 *
 * ****************************************************************************/
#include "utils/ruleutils.h"
static void
show_expression(Node *node, const char *qlabel,
				PlanState *planstate, List *ancestors,
				bool useprefix, ExplainState *es)
{
	List	   *context;
	char	   *exprstr;

	/* Set up deparsing context */
	context = set_deparse_context_plan(es->deparse_cxt,
									   planstate->plan,
									   ancestors);

	/* Deparse the expression */
	exprstr = deparse_expression(node, context, useprefix, false);

	/* And add to es->str */
	ExplainPropertyText(qlabel, exprstr, es);
}

/* *****************************************************************************
 *
 * End of copied froagment.
 *
 * ****************************************************************************/

#include "nodes/makefuncs.h"
#include "utils/lsyscache.h"
static void
qds_per_node_hook(PlanState *planstate, List *ancestors,
				  const char *relationship, const char *plan_name,
				  struct ExplainState *es)
{
	StatMgrOptions *options;

	if (prev_explain_per_node_hook)
		(*prev_explain_per_node_hook) (planstate, ancestors, relationship,
									   plan_name, es);

	options = GetExplainExtensionState(es, es_extension_id);
	if (options == NULL)
		return;

	if (options->show_extstat_candidates && probe_candidate_node(planstate))
	{
		CandidateQualEntry	   *entry;
		List				   *candidates = NIL;
		int						i = -1;

		/* Show candidate clauses for extended statistics definition */
		entry = fetch_candidate_entry(planstate, es->rtable);
		if (entry == NULL)
			return;

		while ((i = bms_next_member(entry->attnums, i)) > 0)
		{
			Var	   *var;
			Oid		typid;
			int32	typmod;
			Oid		collid;

			get_atttypetypmodcoll(entry->key.oid, i, &typid, &typmod, &collid);
			var= makeVar(entry->key.relid, i, typid, typmod, collid, 0);
			candidates = lappend(candidates, var);
		}

		candidates = list_concat(candidates, entry->exprs_list);
		show_expression((Node *) candidates, "Candidate extstat quals",
						planstate, ancestors, false, es);
	}
}

static void
extstat_candidates_handler(ExplainState *es, DefElem *opt, ParseState *pstate)
{
	StatMgrOptions *options = StatMgrOptions_ensure(es);

	options->show_extstat_candidates = defGetBoolean(opt);
}

static void
qds_explain_validate_options_hook(struct ExplainState *es, List *options,
								  ParseState *pstate)
{
	StatMgrOptions *opts;

	if (prev_explain_validate_options_hook)
		(*prev_explain_validate_options_hook) (es, options, pstate);

	opts = GetExplainExtensionState(es, es_extension_id);

	if (opts && opts->show_extstat_candidates && !es->analyze)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("EXPLAIN option %s requires ANALYZE", "EXTSTAT_CANDIDATES")));
}
#endif

/*
 * Code to track current execution level to cleanup resource on the upper level.
 */

int current_execution_level = 0;

/*
 * ExecutorRun hook: all we need do is track nesting depth
 */
#if (PG_VERSION_NUM >= 180000)
static void
qds_ExecutorRun(QueryDesc *queryDesc, ScanDirection direction, uint64 count)
{
	current_execution_level++;
	PG_TRY();
	{
		if (prev_ExecutorRun)
			prev_ExecutorRun(queryDesc, direction, count);
		else
			standard_ExecutorRun(queryDesc, direction, count);
	}
	PG_FINALLY();
	{
		current_execution_level--;
	}
	PG_END_TRY();
}
#else
static void
qds_ExecutorRun(QueryDesc *queryDesc, ScanDirection direction, uint64 count,
				bool execute_once)
{
	current_execution_level++;
	PG_TRY();
	{
		if (prev_ExecutorRun)
			prev_ExecutorRun(queryDesc, direction, count, execute_once);
		else
			standard_ExecutorRun(queryDesc, direction, count, execute_once);
	}
	PG_FINALLY();
	{
		current_execution_level--;
	}
	PG_END_TRY();
}
#endif
 /*
  * ExecutorFinish hook: all we need do is track nesting depth
  */
 static void
 qds_ExecutorFinish(QueryDesc *queryDesc)
 {
	 current_execution_level++;
	 PG_TRY();
	 {
		 if (prev_ExecutorFinish)
			 prev_ExecutorFinish(queryDesc);
		 else
			 standard_ExecutorFinish(queryDesc);
	 }
	 PG_FINALLY();
	 {
		 current_execution_level--;
	 }
	 PG_END_TRY();
 }

/*
 * This is the point where we can find out which clauses can be candidates to
 * extended statistics definition.
 *
 * XXX: Should we try to build extended statistic here or put it out and let
 * a background worker to do this job? Both approaches have their pros, cons and
 * open issues.
 */
static void
qds_ExecutorEnd(QueryDesc *queryDesc)
{
	PlanState  *ps = queryDesc->planstate;

	if (queryDesc->instrument_options & INSTRUMENT_ROWS)
	{
		if (probe_candidate_node(ps))
		{
			/*
			 * TODO: print to elog. But remember, we may be not in the top level
			 * query.
			 */
			(void) fetch_candidate_entry(ps, queryDesc->plannedstmt->rtable);
		}
	}

	/* At the end, remove all the data */
	if (current_execution_level == 0)
	{
		hash_destroy(candidate_quals);
		candidate_quals = NULL;
	}

	if (prev_ExecutorEnd_hook)
		(*prev_ExecutorEnd_hook) (queryDesc);
	else
		standard_ExecutorEnd(queryDesc);
}

void qds_init(void)
{
	DefineCustomBoolVariable(MODULE_NAME".qds",
							"Enable/Disable extended statistics recommendation analysis",
							NULL,
							&enable_qds,
							true,
							PGC_USERSET,
							0,
							NULL,
							NULL,
							NULL);

	DefineCustomRealVariable(MODULE_NAME ".estimation_error_threshold",
							"Planner estimation error deviation, above which "
							"extended statistics is built",
							NULL,
							&estimation_error_threshold,
							2.0,
							-1.0,
							INT_MAX,
							PGC_USERSET,
							0,
							NULL,
							NULL,
							NULL);

	prev_create_upper_paths_hook = create_upper_paths_hook;
	create_upper_paths_hook = upper_paths_hook;

	prev_ExecutorRun = ExecutorRun_hook;
	ExecutorRun_hook = qds_ExecutorRun;
	prev_ExecutorFinish = ExecutorFinish_hook;
	ExecutorFinish_hook = qds_ExecutorFinish;
	prev_ExecutorEnd_hook = ExecutorEnd_hook;
	ExecutorEnd_hook = qds_ExecutorEnd;

#if PG_VERSION_NUM >= 180000
	prev_explain_per_node_hook = explain_per_node_hook;
	explain_per_node_hook = qds_per_node_hook;
	prev_explain_validate_options_hook = explain_validate_options_hook;
	explain_validate_options_hook = qds_explain_validate_options_hook;

	RegisterExtensionExplainOption("extstat_candidates",
									extstat_candidates_handler);
	es_extension_id = GetExplainExtensionId(MODULE_NAME);
#endif
}
