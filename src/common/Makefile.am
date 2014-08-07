AM_CFLAGS= $(CC_OPT) $(DB_CFLAGS) $(PURPOSE_CFLAGS)

noinst_LTLIBRARIES=libcommontools.la

if LUSTRE
FS_SRC=lustre_tools.c
endif
if MNTENTCOMPAT
COMPAT_SRC=mntent_compat.c mntent_compat.h
endif
if SHOOK
PURPOSE_SRC=shook_wrap.c
endif

libcommontools_la_SOURCES= RW_Lock.c uidgidcache.c rbh_misc.c \
                           global_config.c update_params.c queue.c \
                           rbh_logs.c \
                           $(FS_SRC) $(PURPOSE_SRC) $(COMPAT_SRC)

indent:
	$(top_srcdir)/scripts/indent.sh
