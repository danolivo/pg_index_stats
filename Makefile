# contrib/pg_index_stats/Makefile

MODULE_big = pg_index_stats
OBJS = \
	$(WIN32RES) \
	pg_index_stats.o duplicated_slots.o
PGFILEDESC = "pg_index_stats - create extended statistics"

REGRESS = basic module duplicates sc_explain
EXTENSION = pg_index_stats
DATA = pg_index_stats--0.2.sql

ifdef USE_PGXS
PG_CONFIG ?= pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/pg_index_stats
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
