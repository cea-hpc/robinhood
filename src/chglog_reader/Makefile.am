AM_CFLAGS= $(CC_OPT) $(DB_CFLAGS) $(PURPOSE_CFLAGS)

noinst_LTLIBRARIES=libchglog_rd.la

libchglog_rd_la_SOURCES= chglog_reader_config.c chglog_reader.c \
                         chglog_postproc.c


indent:
	$(top_srcdir)/scripts/indent.sh
