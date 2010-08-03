AM_CFLAGS= -Wall $(DB_CFLAGS) $(PURPOSE_CFLAGS)

noinst_LTLIBRARIES=libcommontools.la

if LUSTRE
FS_SRC=lustre_tools.c
endif
if SHERPA
PURPOSE_SRC=sherpa_tools.c
endif

libcommontools_la_SOURCES= RW_Lock.c SemN.c uidgidcache.c RobinhoodMisc.c global_config.c queue.c StringAlloc.c $(FS_SRC) $(PURPOSE_SRC)

indent:
	$(top_srcdir)/scripts/indent.sh
