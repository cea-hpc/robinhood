AM_CFLAGS= $(CC_OPT) $(DB_CFLAGS) $(PURPOSE_CFLAGS)

noinst_LTLIBRARIES=

noinst_LTLIBRARIES+=libmodule_common.la
libmodule_common_la_SOURCES=common_actions.c common_actions.h

if LUSTRE_HSM
noinst_LTLIBRARIES+=libmodule_lhsm.la
libmodule_lhsm_la_SOURCES=lhsm.c lhsm.h
endif
if HSM_LITE
noinst_LTLIBRARIES+=libmodule_backup.la
libmodule_backup_la_SOURCES=backup.c
libmodule_shook_la_CFLAGS=-D_HSM_LITE
endif
if SHOOK
noinst_LTLIBRARIES+=libmodule_shook.la
libmodule_shook_la_SOURCES=shook.c backup.c
libmodule_shook_la_CFLAGS=-DHAVE_SHOOK
endif
