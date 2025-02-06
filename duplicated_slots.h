#ifndef _DUPLICATED_SLOTS_H_
#define _DUPLICATED_SLOTS_H_

#include "postgres.h"

#include "nodes/pg_list.h"
#include "utils/relcache.h"

extern int reduce_duplicated_stat(const List *exprs, Bitmapset *atts_used,
								  Relation hrel, int32 stat_types);

#endif /* _DUPLICATED_SLOTS_H_ */
