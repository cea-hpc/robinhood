AM_CFLAGS= -Wall $(DB_CFLAGS) $(PURPOSE_CFLAGS)

noinst_LTLIBRARIES=liblistmgr.la

if USE_MYSQL_DB
DB_WRAPPER_SRC=mysql_wrapper.c
endif

if USE_SQLITE_DB
DB_WRAPPER_SRC=sqlite_wrapper.c
endif

if HSM_LITE
DB_PURPOSE_SRC=listmgr_recov.c
endif

liblistmgr_la_SOURCES=	listmgr_init.c listmgr_common.c listmgr_common.h \
			listmgr_get.c listmgr_insert.c \
		    	listmgr_stripe.c listmgr_stripe.h \
			listmgr_update.c listmgr_filters.c listmgr_remove.c listmgr_iterators.c \
			listmgr_reports.c listmgr_config.c listmgr_internal.h database.h \
			listmgr_vars.c $(DB_WRAPPER_SRC) $(DB_PURPOSE_SRC)

indent:
	$(top_srcdir)/scripts/indent.sh
