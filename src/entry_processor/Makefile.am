AM_CFLAGS= $(CC_OPT) $(DB_CFLAGS) $(PURPOSE_CFLAGS)

noinst_LTLIBRARIES=libentryproc.la

libentryproc_la_SOURCES=entry_proc_impl.c entry_proc_tools.c entry_proc_tools.h pipeline_common.c

indent:
	$(top_srcdir)/scripts/indent.sh
