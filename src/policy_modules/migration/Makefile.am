AM_CFLAGS= $(CC_OPT) $(DB_CFLAGS) $(PURPOSE_CFLAGS)

noinst_LTLIBRARIES=libmigration.la

libmigration_la_SOURCES=migr_config.c migr_main.c \
			migr_arch.h migr_arch.c

indent:
	$(top_srcdir)/scripts/indent.sh
