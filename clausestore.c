
/* -----------------------------------------------------------------------------
 *
 * Shared HTAB to store clause signature
 *
 * -----------------------------------------------------------------------------
 */

#include "postgres.h"

#include "common/hashfn.h"
#include "lib/dshash.h"
#include "miscadmin.h"
#include "statistics/statistics.h"
#include "storage/dsm_registry.h"
#include "storage/lwlock.h"
#include "utils/builtins.h"
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
  uint32 qual_hashes[STATS_MAX_DIMENSIONS];
} HashTableEntry;

static dsa_area *dsa = NULL;

/*
 * A compare function.
 *
 * Check: Is one of entries fully covered by another one?
 */
static int
qds_cmp(const void *a, const void *b, size_t size, void *arg)
{
	HashTableEntry *v1 = (HashTableEntry *) a;
	HashTableEntry *v2 = (HashTableEntry *) b;
	int				i = 0;

	Assert(sizeof(HashTableEntry) == size);

	for (i = 0; i < STATS_MAX_DIMENSIONS; i++)
	{
		if (v1->qual_hashes[i] != v2->qual_hashes[i])
			return 1;
	}

	return 0;
}
/*
 * A hash function that forwards to tag_hash.
 */
static dshash_hash
qds_hash(const void *v, size_t size, void *arg)
{
	HashTableEntry *entry = (HashTableEntry *) v;

	Assert(sizeof(HashTableEntry) == size);

	return oid_hash(&entry->oid, sizeof(Oid));
}
static dshash_parameters dsh_params =
{
	sizeof(HashTableEntry),
	sizeof(HashTableEntry),
	qds_cmp,
	qds_hash,
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
lookup_extstat_definition(Oid reloid, Bitmapset *attnums, List *exprs)
{
	HashTableEntry	key = {0};
	HashTableEntry *entry;
	int				counter = 0;
	int				i = -1;
	ListCell	   *lc;

	qds_init_shmem();

//	memset(&key, 0, sizeof(HashTableEntry));
	key.oid = reloid;

	/*
	 * Prepare signatures of the extstat definition.
	 * We doesn't care about accidental match of some hashes - after application
	 * of one hash it will be saved inside the database and we would apply
	 * accidentally skipped definition again.
	 */
	while ((i = bms_next_member(attnums, i)) >= 0)
	{
		key.qual_hashes[counter++] = (uint32) i;
	}
	foreach(lc, exprs)
	{
		Node	   *n = (Node *) lfirst(lc);
		const char *str = nodeToString(n);
		int			len = strlen(str);
		uint32		hash = hash_bytes((const unsigned char *) str, len);

		Assert(counter < STATS_MAX_DIMENSIONS);

		key.qual_hashes[counter++] = (hash == 0) ? 1 : hash;
	}
	/* Sort hashes for quick comparison */
	qsort(key.qual_hashes, counter, sizeof(uint32), oid_cmp);

	/*
	 * Check for the same definition. To simplify the code, don't bother if
	 * current definition covers (or covered by) something existing in the
	 * storage. Anyway, they would be combined further by the compression
	 * algorithm.
	 */
	entry = (HashTableEntry *) dshash_find(sign_htab, &key, false);
	if (entry == NULL)
		return false;

	dshash_release_lock(sign_htab, entry);

	return true;
}
