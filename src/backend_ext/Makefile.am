AM_CFLAGS= $(CC_OPT) $(DB_CFLAGS) $(PURPOSE_CFLAGS)

noinst_LTLIBRARIES=libbackend_ext.la

libbackend_ext_la_SOURCES=backend_loader.c backend_basic.c
libbackend_ext_la_LIBADD=-lz


indent:
	$(top_srcdir)/scripts/indent.sh
