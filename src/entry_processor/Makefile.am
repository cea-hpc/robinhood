AM_CFLAGS= $(CC_OPT) $(DB_CFLAGS) $(PURPOSE_CFLAGS)

noinst_LTLIBRARIES=libentryproc.la

libentryproc_la_SOURCES=entry_proc_impl.c entry_proc_tools.c entry_proc_tools.h \
			std_pipeline.c diff_pipeline.c entry_proc_hash.c

indent:
	$(top_srcdir)/scripts/indent.sh
