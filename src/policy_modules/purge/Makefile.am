# a lot of unused variables in this module... (remove -Werror)
AM_CFLAGS= -Wall $(DB_CFLAGS) $(PURPOSE_CFLAGS)

noinst_LTLIBRARIES=libresmonitor.la

libresmonitor_la_SOURCES=resmon_config.c resmon_triggers.c \
			resmon_purge.h resmon_purge.c

indent:
	$(top_srcdir)/scripts/indent.sh
