
/* -----------------------------------------------------------------------------
 *
 * Shared HTAB to store clause signature
 *
 * -----------------------------------------------------------------------------
 */

#include "postgres.h"

#include "lib/dshash.h"
#include "miscadmin.h"
#include "storage/dsm_registry.h"
#include "storage/lwlock.h"
#include "utils/dsa.h"
#include "utils/memutils.h"

#include "pg_index_stats.h"

/*
 * Local handlers for shared memory structures
 */

typedef struct SharedState
{
	int					tranche_id;

	dsa_handle			dsah;
	dshash_table_handle	dshh;

	/* Just for DEBUG */
	Oid					dbOid;
} SharedState;

/*
 * HTAB containing signatures of extended statistic definitions.
 *
 * Store signatures of the definitions to avoid duplicated definitions.
 * Each backend, planning query, collects candidate quals and may lookup this
 * table with shared read lock. If it finds a duplicate it may just skip this
 * candidate on early stage, avoiding unnecessary memory allocations and
 * analysis during the ExecutorEnd stage.
 */
typedef struct HashTableEntry
{
  Oid oid; /* The key */
  uint64 qual_hashes[8];
} HashTableEntry;

static dsa_area *dsa = NULL;
static dshash_parameters dsh_params =
{
	sizeof(HashTableEntry),
	sizeof(HashTableEntry),
	dshash_memcmp,
	dshash_memhash,
	dshash_memcpy,
	-1
};
static SharedState *state = NULL;
static dshash_table *sign_htab = NULL;

static void
pgm_init_state(void *ptr)
{
	SharedState *state = (SharedState *) ptr;

	state->tranche_id = LWLockNewTrancheId();
	state->dbOid = MyDatabaseId;
	Assert(OidIsValid(state->dbOid));

	dsa = dsa_create(state->tranche_id);
	dsa_pin(dsa);
	dsa_pin_mapping(dsa);
	dsh_params.tranche_id = state->tranche_id;
	sign_htab = dshash_create(dsa, &dsh_params, NULL);

	/* Store handles in shared memory for other backends to use. */
	state->dsah = dsa_get_handle(dsa);
	state->dshh = dshash_get_hash_table_handle(sign_htab);
}

/*
 * On call, create or attach to a named shared memory segment, individual for
 * each database, that employes this module.
 */
static bool
qds_init_shmem(void)
{
	bool			found;
	char		   *segment_name;
	MemoryContext	memctx;

	if (state != NULL)
		return true;

	Assert(OidIsValid(MyDatabaseId));

	memctx = MemoryContextSwitchTo(TopMemoryContext);
	segment_name = psprintf(MODULE_NAME"-%u", MyDatabaseId);
	state = GetNamedDSMSegment(segment_name, sizeof(SharedState),
							   pgm_init_state, &found);

	if (found)
	{
		/* Attach to proper database */
		Assert(state->dbOid == MyDatabaseId);

		dsa = dsa_attach(state->dsah);
		dsa_pin_mapping(dsa);
		sign_htab = dshash_attach(dsa, &dsh_params, state->dshh, NULL);
	}
	LWLockRegisterTranche(state->tranche_id, segment_name);

	MemoryContextSwitchTo(memctx);
	Assert(dsa != NULL && sign_htab != NULL);
	return found;
}

bool
lookup_extstat_definition(Bitmapset *attnums, List *exprs)
{
	qds_init_shmem();

	return false;
}
